"""Exact parity for design 1, in-process (no server).

For thousands of randomized scheduler steps covering every branch of BatchMetrics.create
and publish_metrics (DP > 1, host/disk/dKV tiers, spec decode, CE admissions, overlap,
completed batches, kv_cache None), compare:

  A  original:  BatchMetrics.create(...) -> publish_metrics + publish_completed_batch_metrics
  B  design 1:  BatchMetrics.compute_values(...) -> MetricsLog (file) -> decode_records
                -> record_publish (rebuild BatchMetrics, same publish functions)

Both paths record measurements with a capturing client; the (instrument, value, attributes)
sequences must be identical, and the rebuilt BatchMetrics must equal the original object.
Run after `python3 max_patch.py`:  .venv/bin/python -I parity_test.py [steps]
"""

import copy
import os
import random
import sys
import tempfile
from types import SimpleNamespace

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import max._core_mojo  # noqa: F401,E402
from max.pipelines.modeling.types.pipeline_variants.text_generation import (  # noqa: E402
    BatchType,
    CompletedBatchStats,
)
from max.pipelines.lib.vision_encoder_cache import (  # noqa: E402
    VideoEncoderMetrics,
    VisionEncoderMetrics,
)
from max.serve.scheduler.utils import BatchMetrics, publish_completed_batch_metrics  # noqa: E402
from max.serve.telemetry.metrics import METRICS, MetricClient  # noqa: E402

import metrics_log  # noqa: E402
from record_publish import make_publisher  # noqa: E402


class Capture(MetricClient):
    def __init__(self):
        self.seen = []

    def send_measurement(self, m):
        self.seen.append((m.instrument_name, m.value, dict(m.attributes or {})))

    def cross_process_factory(self, settings):
        raise NotImplementedError


def scenario(rng: random.Random):
    dp = rng.choice([1, 1, 2, 4])
    bt = rng.choice([BatchType.CE, BatchType.TG])
    batch_size = rng.randint(1, 512)
    sch = SimpleNamespace(
        data_parallel_degree=dp,
        max_batch_size=512,
        target_tokens_per_batch_ce=8192,
        max_batch_total_tokens=rng.choice([None, 262144]),
    )
    ctxs = []
    if bt == BatchType.CE:
        for _ in range(rng.randint(0, 6)):
            prompt = rng.randint(0, 4000)
            cached = rng.choice([None, rng.randint(0, prompt)])
            ctxs.append(SimpleNamespace(
                _cache_metrics_emitted=rng.random() < 0.2,
                cached_prefix_length=cached,
                cached_prefix_external_length=0 if cached is None else rng.randint(0, cached),
                tokens=SimpleNamespace(prompt_length=prompt),
            ))
    inputs = SimpleNamespace(
        batch_type=bt,
        batch_size=batch_size,
        input_tokens=rng.randint(1, 8192),
        context_tokens=rng.randint(0, 262144),
        per_replica_input_tokens=[rng.randint(0, 4096) for _ in range(rng.randint(0, dp))],
        per_replica_context_tokens=[rng.randint(0, 65536) for _ in range(dp)],
        flat_batch=ctxs,
    )
    kv = None
    if rng.random() < 0.9:
        host = rng.choice([0, 4096])
        disk = rng.choice([0, 65536])
        dkv = rng.choice([0, 0, 2])
        per = [SimpleNamespace(total=2048, used=rng.randint(0, 2048)) for _ in range(dp)]
        hper = [SimpleNamespace(total=host, used=rng.randint(0, host)) for _ in range(dp)]
        dper = [SimpleNamespace(total=disk, used=rng.randint(0, disk)) for _ in range(dp)]
        agg = SimpleNamespace(
            device_blocks_served=rng.randint(0, 100), h2d_blocks_copied=rng.randint(0, 50),
            d2h_blocks_copied=rng.randint(0, 50), cross_replica_blocks_copied=rng.randint(0, 9),
            cross_replica_bytes_copied=rng.randint(0, 1 << 30), disk_blocks_written=rng.randint(0, 9),
            disk_blocks_read=rng.randint(0, 9), inflight_disk_ops=rng.randint(0, 3),
            nixl_read_latency_avg_ms=rng.choice([0.0, rng.random() * 5]),
            nixl_write_latency_avg_ms=rng.choice([0.0, rng.random() * 5]),
            rpc_acquire_latency_avg_ms=rng.choice([0.0, rng.random()]),
            rpc_read_latency_avg_ms=rng.choice([0.0, rng.random()]),
            nixl_read_gib_per_s=rng.random() * 3, nixl_write_gib_per_s=rng.random() * 3,
            dkv_connected_clients=rng.randint(0, dkv), dkv_total_clients=dkv,
            dkv_reconnect_attempts=rng.randint(0, 5), nixl_read_blocks=rng.randint(0, 20),
        )
        kv = SimpleNamespace(
            block_count=lambda i, per=per: per[i],
            host_block_count=lambda i, hper=hper: hper[i],
            disk_block_count=lambda i, dper=dper: dper[i],
            get_metrics_aggregated=lambda agg=agg: agg,
            reset_metrics=lambda: None,
        )
    spec = None
    if rng.random() < 0.3:
        k = rng.randint(1, 4)
        spec = SimpleNamespace(
            output_tokens=rng.randint(batch_size, batch_size * (k + 1)),
            draft_tokens_generated=rng.randint(0, 2000), draft_tokens_accepted=rng.randint(0, 2000),
            avg_acceptance_length=rng.random() * k, num_speculative_tokens=k,
            acceptance_rate_per_position=[rng.random() for _ in range(k)],
        )
    completed = None
    if rng.random() < 0.7:
        completed = CompletedBatchStats(
            batch_type=rng.choice([BatchType.CE, BatchType.TG]),
            batch_size=rng.randint(1, 512), num_input_tokens=rng.randint(1, 8192),
            num_context_tokens=rng.randint(0, 262144),
            execution_time_s=rng.choice([0.0, rng.random() * 0.5]),
            num_output_tokens=rng.choice([None, rng.randint(1, 2048)]),
            draft_tokens_generated=rng.randint(0, 99), draft_tokens_accepted=rng.randint(0, 99),
            avg_acceptance_length=rng.random() * 3, max_acceptance_length=rng.randint(0, 4),
            acceptance_rate_per_position=rng.choice([[], [rng.random(), rng.random()]]),
        )
    vision = None
    if rng.random() < 0.35:   # a VLM step: images referenced, some encoded, some cached
        total = rng.choice([0, 1, rng.randint(2, 16)])
        cached = rng.randint(0, total)
        vision = VisionEncoderMetrics(
            num_images_total=total, num_images_encoded=total - cached, num_images_cached=cached,
            num_patches_encoded=rng.randint(0, 4096), num_tokens_encoded=rng.randint(0, 2048),
        )
    video = None
    if rng.random() < 0.25:
        clips = rng.choice([0, 1, rng.randint(2, 8)])
        cached = rng.randint(0, clips)
        vision_frames = [rng.randint(1, 64) for _ in range(clips - cached)]
        video = VideoEncoderMetrics(
            num_clips_total=clips, num_clips_encoded=clips - cached, num_clips_cached=cached,
            frame_counts=vision_frames, num_tokens_encoded=rng.randint(0, 8192),
            encoding_time_ms=rng.choice([0.0, rng.random() * 200]),
        )
    return dict(
        sch_config=sch, inputs=inputs, kv_cache=kv,
        batch_vision_metrics=vision, batch_video_metrics=video,
        batch_creation_time_s=rng.random() * 0.003,
        batch_execution_time_s=rng.random() * 0.5 + 1e-6,
        num_pending_reqs=rng.randint(0, 200), num_terminated_reqs=rng.randint(0, 8),
        total_preemption_count=rng.randint(0, 9),
        batch_spec_decode_metrics=spec, overlap_active=rng.random() < 0.5,
        completed_batch_stats=completed,
    )


def main() -> None:
    steps = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
    rng = random.Random(2026)
    scenarios = [scenario(rng) for _ in range(steps)]

    # A: original path
    cap_a = Capture()
    METRICS.configure(cap_a)
    originals = []
    for sc in scenarios:
        args = copy.deepcopy(sc)
        m = BatchMetrics.create(**args)
        originals.append(m)
        with METRICS.transaction():
            m.publish_metrics(defer_execution_metrics=args["overlap_active"])
            if args["completed_batch_stats"] is not None:
                publish_completed_batch_metrics(args["completed_batch_stats"], args["num_terminated_reqs"])

    # B: design 1 — values -> Dagr record file -> decode -> rebuild -> same publish functions
    path = os.path.join(tempfile.mkdtemp(prefix="dagr_parity_"), "metrics.dagr")
    log = metrics_log.MetricsLog(path, "parity", "none", 65536)
    for i, sc in enumerate(scenarios):
        args = copy.deepcopy(sc)
        values = BatchMetrics.compute_values(**args)
        log.append_values_at(
            values + (False, args["batch_vision_metrics"], args["batch_video_metrics"]),
            1_789_000_000_000_000_000 + i,
        )
    assert log.close() == steps
    data = open(path, "rb").read()
    arr = np.frombuffer(data, dtype=np.uint8)
    start = metrics_log.records_start(arr.ctypes.data, len(data))
    records, end = metrics_log.decode_records(arr.ctypes.data, len(data), start)
    assert end == len(data) and len(records) == steps, (end, len(data), len(records))

    cap_b = Capture()
    METRICS.configure(cap_b)
    to_metrics, publish = make_publisher()
    for i, values in enumerate(records):
        rebuilt = to_metrics(values)
        assert rebuilt == originals[i], f"step {i}: rebuilt BatchMetrics differs\n{rebuilt}\n{originals[i]}"
        assert publish(values)

    assert len(cap_a.seen) == len(cap_b.seen), (len(cap_a.seen), len(cap_b.seen))
    for j, (a, b) in enumerate(zip(cap_a.seen, cap_b.seen, strict=True)):
        assert a == b, f"measurement {j}: {a} != {b}"
    names = sorted({n for n, _, _ in cap_a.seen})
    vis = sum(1 for sc in scenarios if sc["batch_vision_metrics"] is not None
              and sc["batch_vision_metrics"].num_images_total > 0)
    vid = sum(1 for sc in scenarios if sc["batch_video_metrics"] is not None
              and sc["batch_video_metrics"].num_clips_total > 0)
    print(f"  steps carrying published vision metrics: {vis}, video metrics: {vid}")
    print(f"parity: {steps} steps, {len(cap_a.seen)} measurements identical across {len(names)} instruments; "
          f"rebuilt BatchMetrics == original for every step; log {len(data)} bytes "
          f"({len(data) / steps:.1f} B/step)")


if __name__ == "__main__":
    main()
