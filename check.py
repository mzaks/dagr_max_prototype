"""Correctness: every way of handing a step to Mojo must log byte-identical records.

  append_at(metrics, ts)               Mojo reads the BatchMetrics object
  <writer>(metrics); commit_at(ts)     Python fills the SharedBuffer record, Mojo reads it
                                       for writer in struct / numpy / dagr_py

Then Dagr's independent reflective reader checks every field against the source metrics.
Run: cd ~/dev/dagr_max_prototype && .venv/bin/python -I check.py
"""

import os
import random
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import max._core_mojo  # noqa: F401,E402  (MAX's Mojo runtime first)
from slot_writers import TG, WRITERS, make_metrics, new_slot  # noqa: E402

import batch_log  # noqa: E402
from dagr_py import dagr_sink as ds  # noqa: E402
from dagr_schema import MAX_BATCH_LOG  # noqa: E402

N = 3000
rng = random.Random(7)
metrics = [make_metrics(rng, i) for i in range(N)]
ts0 = 1_789_000_000_000_000_000
tmp = tempfile.mkdtemp(prefix="dagr_max_check_")


def log_path(name):
    return os.path.join(tmp, f"{name}.dagr")


# reference: Mojo reads the object
log = batch_log.BatchLog(log_path("append"), "max-serve", "Qwen/Qwen3-0.6B", 512, 65536)
for i, m in enumerate(metrics):
    log.append_at(m, ts0 + i)
assert log.close()[0] == N
reference = open(log_path("append"), "rb").read()

for name, make in WRITERS.items():
    slot = new_slot()
    write = make(slot)
    log = batch_log.BatchLog(log_path(name), "max-serve", "Qwen/Qwen3-0.6B", 512, 65536)
    log.attach(slot.ctypes.data)
    for i, m in enumerate(metrics):
        write(m)
        log.commit_at(ts0 + i)
    assert log.close()[0] == N
    data = open(log_path(name), "rb").read()
    assert data == reference, f"{name}: log differs from append path"
    print(f"{name:8s} + commit: byte-identical to append ({len(data)} bytes, {N} records)")

recs = [v.fields for _, _, v in ds.iter_sink_records(MAX_BATCH_LOG, reference)]
assert len(recs) == N
for i, (f, m) in enumerate(zip(recs, metrics, strict=True)):
    assert f["ts_ns"] == ts0 + i
    assert f["batch_type"] == (1 if m.batch_type is TG else 0)
    assert f["batch_size"] == m.batch_size and f["terminated_reqs"] == m.terminated_reqs
    assert f["pending_reqs"] == m.num_pending_reqs and f["input_tokens"] == m.num_input_tokens
    assert f["context_tokens"] == m.num_context_tokens
    assert f["creation_time_s"] == m.batch_creation_time_s
    assert f["execution_time_s"] == m.batch_execution_time_s
    assert f["prompt_throughput"] == m.prompt_throughput
    assert f["generation_throughput"] == m.generation_throughput
    assert f["preemptions_total"] == m.total_preemption_count and f["used_kv_pct"] == m.used_kv_pct
    assert f["total_kv_blocks"] == m.total_kv_blocks
    assert f["cache_hit_tokens"] == (m.cache_hit_tokens or None)
    assert f["cache_miss_tokens"] == (m.cache_miss_tokens or None)
    assert f["draft_tokens_generated"] == (m.draft_tokens_generated or None)
    assert f["draft_tokens_accepted"] == (m.draft_tokens_accepted or None)
    assert f["overlap_active"] == m.overlap_active
    if m.completed is None:
        assert f["completed"] is None
    else:
        c = f["completed"].fields
        assert c["batch_type"] == 1 and c["batch_size"] == m.completed.batch_size
        assert c["input_tokens"] == m.completed.num_input_tokens
        assert c["context_tokens"] == m.completed.num_context_tokens
        assert c["execution_time_s"] == m.completed.execution_time_s
print(f"reflective Python reader: all {N} records match the source metrics field for field")
