"""Telemetry side of design 1: turn decoded BatchMetricsRec values back into Prometheus metrics.

Rebuilds MAX's own BatchMetrics / CompletedBatchStats from a values tuple (record_spec order,
as returned by metrics_log.decode_records) and calls MAX's ORIGINAL publish functions, so the
emitted measurements are the ones the worker would have emitted. Shared by the telemetry
process tailer (max_patch.py) and parity_test.py.
"""

from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from record_spec import COMPLETED_NAMES, NAMES, VIDEO_NAMES, VISION_NAMES  # noqa: E402


TIMINGS: dict[str, list[int]] = {"rebuild": [], "publish": [], "decode": [], "read": []}


def write_timings(path: str) -> None:
    """Dump the telemetry-side per-record costs next to the log (the telemetry process has no
    usable logger here), then clear them."""
    def line(name: str) -> str:
        d = sorted(TIMINGS[name])
        if not d:
            return f"{name}: -"
        return (f"{name}: n={len(d)} med {d[len(d) // 2] / 1e3:.2f} mean {sum(d) / len(d) / 1e3:.2f} "
                f"p90 {d[int(len(d) * 0.9)] / 1e3:.2f} max {d[-1] / 1e3:.2f} us")

    with open(path, "a") as fh:
        fh.write(" | ".join(line(k) for k in ("read", "decode", "rebuild", "publish")) + "\n")
    for v in TIMINGS.values():
        v.clear()


def make_publisher():
    from max.pipelines.modeling.types.pipeline_variants.text_generation import (
        BatchType,
        CompletedBatchStats,
    )
    from max.pipelines.lib.vision_encoder_cache import (
        VideoEncoderMetrics,
        VisionEncoderMetrics,
    )
    from max.serve.scheduler.utils import BatchMetrics, publish_completed_batch_metrics
    from max.serve.telemetry.metrics import METRICS

    types = (BatchType.CE, BatchType.TG)
    n = len(NAMES)

    def to_metrics(values) -> BatchMetrics:
        kw = dict(zip(NAMES, values[:n], strict=True))
        kw["batch_type"] = types[kw["batch_type"]]
        c = kw["completed"]
        if c is not None:
            ck = dict(zip(COMPLETED_NAMES, c, strict=True))
            ck["batch_type"] = types[ck["batch_type"]]
            kw["completed"] = CompletedBatchStats(**ck)
        vision, video = values[n + 1], values[n + 2]
        if vision is not None:
            kw["vision_metrics"] = VisionEncoderMetrics(**dict(zip(VISION_NAMES, vision, strict=True)))
        if video is not None:
            kw["video_metrics"] = VideoEncoderMetrics(**dict(zip(VIDEO_NAMES, video, strict=True)))
        return BatchMetrics(**kw)

    timing = os.environ.get("MAX_SERVE_TELEMETRY_TIMING") == "1"

    def publish(values) -> bool:
        """Publish one decoded record; returns False when the worker already published it."""
        if values[n]:                      # published_in_worker
            return False
        if timing:
            t0 = time.perf_counter_ns()
            m = to_metrics(values)
            t1 = time.perf_counter_ns()
            with METRICS.transaction():
                m.publish_metrics(defer_execution_metrics=m.overlap_active)
                if m.completed is not None:
                    publish_completed_batch_metrics(m.completed, m.terminated_reqs)
            TIMINGS["rebuild"].append(t1 - t0)
            TIMINGS["publish"].append(time.perf_counter_ns() - t1)
            return True
        m = to_metrics(values)
        with METRICS.transaction():
            m.publish_metrics(defer_execution_metrics=m.overlap_active)
            if m.completed is not None:
                publish_completed_batch_metrics(m.completed, m.terminated_reqs)
        return True

    return to_metrics, publish


def tail_and_publish(path: str, module_dir: str, poll_s: float = 0.02) -> None:
    """Follow a growing MaxBatchMetrics log and publish every complete record (thread body)."""
    import logging

    import numpy as np

    sys.path.insert(0, module_dir)
    import metrics_log
    from max.serve.telemetry.metrics import METRICS, SyncClient

    logger = logging.getLogger("max.serve")
    METRICS.configure(SyncClient())        # this process records directly into OTel
    _, publish = make_publisher()
    timing = os.environ.get("MAX_SERVE_TELEMETRY_TIMING") == "1"
    timing_path = path + ".telemetry.txt"
    last_dump = time.monotonic()

    while not os.path.exists(path):
        time.sleep(poll_s)
    logger.info("PROTOTYPE telemetry tailing %s", path)
    published = 0
    mmap_mode = os.environ.get("MAX_SERVE_RECORD_METRICS_MMAP") == "1"
    len_fd = None
    if mmap_mode:  # the writer publishes the committed length in <path>.len
        while not os.path.exists(path + ".len"):
            time.sleep(poll_s)
        len_fd = os.open(path + ".len", os.O_RDONLY)
    with open(path, "rb") as f:
        fd = f.fileno()
        offset = 0          # bytes of the file consumed into `pending`/decoded
        pending = b""
        header_done = False
        while True:
            _t0 = time.perf_counter_ns()
            if mmap_mode:
                # pread only: the file is pre-sized with zeros past the committed length, and
                # a buffered reader would keep (and later serve) those zeros.
                raw = os.pread(len_fd, 8, 0)
                committed = int.from_bytes(raw, "little") if len(raw) == 8 else 0
                chunk = os.pread(fd, committed - offset, offset) if committed > offset else b""
            else:
                chunk = f.read()
            if timing and chunk:
                TIMINGS["read"].append(time.perf_counter_ns() - _t0)
            if not chunk:
                time.sleep(poll_s)
                continue
            offset += len(chunk)
            data = pending + chunk
            arr = np.frombuffer(data, dtype=np.uint8)
            addr, size, pos = arr.ctypes.data, len(data), 0
            if not header_done:
                start = metrics_log.records_start(addr, size)
                if start < 0:
                    pending = data
                    continue
                pos, header_done = start, True
            _t1 = time.perf_counter_ns()
            records, pos = metrics_log.decode_records(addr, size, pos)
            if timing:
                TIMINGS["decode"].append(time.perf_counter_ns() - _t1)
            pending = data[pos:]
            for values in records:
                try:
                    if publish(values):
                        published += 1
                except Exception:
                    logger.exception("PROTOTYPE telemetry: failed to publish a record")
            if timing and time.monotonic() - last_dump > 10:
                last_dump = time.monotonic()
                write_timings(timing_path)
            if records and published % 500 < len(records):
                logger.info("PROTOTYPE telemetry published %d records", published)
