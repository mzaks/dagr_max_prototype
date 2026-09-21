"""Telemetry side of the measurement log: follow every producer's stream and commit.

One file per producing process (`meas-<pid>.dagr`, written by DagrMetricClient). New files are
picked up as they appear. Each file keeps its own name / attribute tables, since ids are per
producer. Committing is what MAX's telemetry process already does with measurements taken off
the queue — only the transport changed.
"""

from __future__ import annotations

import glob
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

TIMINGS: dict[str, list[int]] = {"read": [], "decode": [], "commit": []}
COMMITTED: dict[str, list] = {}    # instrument -> [count, first_ts, last_ts], vs /metrics


class _Producer:
    """One measurement file being followed."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.fd = os.open(path, os.O_RDONLY)
        self.len_fd = os.open(path + ".len", os.O_RDONLY)
        self.offset = 0
        self.pending = b""
        self.pos = None                 # byte offset of the first record, once the header lands
        self.names: dict[int, str] = {}
        self.attrs: dict[int, dict] = {}

    def close(self) -> None:
        os.close(self.fd)
        os.close(self.len_fd)


def tail_measurements(directory: str, module_dir: str, poll_s: float = 0.02) -> None:
    """Thread body: follow every meas-*.dagr in `directory` and commit what they carry."""
    import logging

    import numpy as np

    sys.path.insert(0, module_dir)
    import metrics_log
    from max.serve.telemetry.metrics import MaxMeasurement

    logger = logging.getLogger("max.serve")
    timing = os.environ.get("MAX_SERVE_TELEMETRY_TIMING") == "1"
    timing_path = os.path.join(directory, "measurements.telemetry.txt")
    last_dump = time.monotonic()
    last_counts = time.monotonic()
    os.makedirs(directory, exist_ok=True)   # the producers create it lazily; do not race them
    producers: dict[str, _Producer] = {}
    committed = 0
    logger.info("PROTOTYPE telemetry tailing measurements in %s", directory)

    while True:
        for path in sorted(glob.glob(os.path.join(directory, "meas-*.dagr"))):
            if path not in producers and os.path.exists(path + ".len"):
                producers[path] = _Producer(path)

        did_work = False
        for p in list(producers.values()):
            t0 = time.perf_counter_ns()
            raw = os.pread(p.len_fd, 8, 0)
            committed_len = int.from_bytes(raw, "little") if len(raw) == 8 else 0
            if committed_len <= p.offset:
                continue
            chunk = os.pread(p.fd, committed_len - p.offset, p.offset)
            if not chunk:
                continue
            p.offset += len(chunk)
            data = p.pending + chunk
            if timing:
                TIMINGS["read"].append(time.perf_counter_ns() - t0)
            arr = np.frombuffer(data, dtype=np.uint8)
            addr, size = arr.ctypes.data, len(data)
            if p.pos is None:
                start = metrics_log.measurements_start(addr, size)
                if start < 0:
                    p.pending = data
                    continue
                p.pos = start
            t1 = time.perf_counter_ns()
            rows, names, attrs, pos = metrics_log.decode_measurements(addr, size, p.pos)
            if timing:
                TIMINGS["decode"].append(time.perf_counter_ns() - t1)
            # `pos` is relative to the buffer just decoded; its leftover bytes start the next
            # buffer, so from here on a decode begins at offset 0.
            p.pending = data[pos:]
            p.pos = 0
            for nid, name in names:
                p.names[nid] = name
            for aid, kv in attrs:
                p.attrs[aid] = {kv[i]: kv[i + 1] for i in range(0, len(kv), 2)}
            if not rows:
                continue
            did_work = True
            t2 = time.perf_counter_ns()
            for ts_ns, name_id, attrs_id, value in rows:
                try:
                    name = p.names[name_id]
                    MaxMeasurement(name, value, p.attrs.get(attrs_id) or None, ts_ns).commit()
                    committed += 1
                    if timing:
                        c = COMMITTED.get(name)
                        if c is None:
                            COMMITTED[name] = [1, ts_ns, ts_ns]
                        else:
                            c[0] += 1
                            c[2] = ts_ns
                except Exception:
                    logger.exception("PROTOTYPE telemetry: failed to commit a measurement")
            if timing:
                TIMINGS["commit"].append((time.perf_counter_ns() - t2) // max(len(rows), 1))

        if timing and time.monotonic() - last_counts > 1:
            last_counts = time.monotonic()
            try:
                _dump_counts(timing_path + ".counts")
            except Exception:
                logger.exception("PROTOTYPE telemetry: counts dump failed")
        if timing and time.monotonic() - last_dump > 10:
            last_dump = time.monotonic()
            try:
                _dump(timing_path, committed)
            except Exception:       # never let instrumentation kill the tailer
                logger.exception("PROTOTYPE telemetry: timing dump failed")
        if not did_work:
            time.sleep(poll_s)


def _dump_counts(path: str) -> None:
    with open(path, "w") as fh:
        for k in sorted(COMMITTED):
            n, first, last = COMMITTED[k]
            print(f"{k} {n} {first} {last}", file=fh)


def _dump(path: str, committed: int) -> None:
    def line(name: str) -> str:
        d = sorted(TIMINGS[name])
        if not d:
            return f"{name}: -"
        return (f"{name}: n={len(d)} med {d[len(d) // 2] / 1e3:.2f} mean {sum(d) / len(d) / 1e3:.2f} "
                f"p90 {d[int(len(d) * 0.9)] / 1e3:.2f} us")

    with open(path, "a") as fh:
        print(f"committed={committed} | " + " | ".join(line(k) for k in TIMINGS), file=fh)
    _dump_counts(path + ".counts")
    for v in TIMINGS.values():
        v.clear()
