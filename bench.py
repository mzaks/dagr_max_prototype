"""Per-step cost of handing one BatchMetrics to the Dagr log. One variant per process.

Usage: .venv/bin/python -I bench.py <variant> [n]

  append          Mojo reads the BatchMetrics object (__dict__ lookups)       -> append
  struct_commit   Python struct.pack_into fills the SharedBuffer record        -> commit
  numpy_commit    Python numpy per-field views fill the record                 -> commit
  dagr_py_commit  Dagr reflective dagr_sb_writer.write_region fills the record -> commit
  struct_only     struct.pack_into fill only (no Mojo call)
  numpy_only      numpy fill only (no Mojo call)
  commit_only     commit() of an already-filled record (Mojo read + encode + buffer)
  jsonl_subset    msgspec JSONL of the same 20 fields + time.time_ns(), buffered file

All logs use a 64 KB BufferedFileDestination. Timed per step with perf_counter_ns.
"""

import os
import random
import statistics
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import max._core_mojo  # noqa: F401,E402
import msgspec  # noqa: E402
from slot_writers import WRITERS, make_metrics, new_slot  # noqa: E402

import batch_log  # noqa: E402

variant = sys.argv[1]
n = int(sys.argv[2]) if len(sys.argv) > 2 else 20000
rng = random.Random(11)
metrics = [make_metrics(rng, i) for i in range(1000)]
path = os.path.join(tempfile.mkdtemp(prefix="dagr_max_bench_"), "log.bin")


def run(step, finish):
    per = []
    for i in range(n):
        m = metrics[i % 1000]
        t0 = time.perf_counter_ns()
        step(m)
        per.append(time.perf_counter_ns() - t0)
    finish()
    per.sort()
    size = os.path.getsize(path) if os.path.exists(path) else 0
    print(
        f"variant={variant} n={n} med_us={statistics.median(per) / 1e3:.3f} "
        f"mean_us={statistics.fmean(per) / 1e3:.3f} p99_us={per[int(n * 0.99)] / 1e3:.3f} "
        f"bytes_per_record={size / n:.1f}"
    )


def new_log():
    return batch_log.BatchLog(path, "max-serve", "Qwen/Qwen3-0.6B", 512, 65536)


if variant == "append":
    log = new_log()
    run(log.append, log.close)

elif variant in ("struct_commit", "numpy_commit", "dagr_py_commit"):
    slot = new_slot()
    write = WRITERS[variant.removesuffix("_commit")](slot)
    log = new_log()
    log.attach(slot.ctypes.data)
    commit = log.commit

    def step(m):
        write(m)
        commit()

    run(step, log.close)

elif variant in ("struct_only", "numpy_only"):
    slot = new_slot()
    run(WRITERS[variant.removesuffix("_only")](slot), lambda: None)

elif variant == "commit_only":
    slot = new_slot()
    WRITERS["struct"](slot)(metrics[1])     # a TG step with a completed batch
    log = new_log()
    log.attach(slot.ctypes.data)
    commit = log.commit
    run(lambda m: commit(), log.close)

elif variant == "jsonl_subset":
    enc = msgspec.json.Encoder()
    f = open(path, "wb")

    def step(m):
        c = m.completed
        f.write(enc.encode({
            "ts_ns": time.time_ns(),
            "batch_type": m.batch_type.value,
            "batch_size": m.batch_size,
            "terminated_reqs": m.terminated_reqs,
            "pending_reqs": m.num_pending_reqs,
            "input_tokens": m.num_input_tokens,
            "context_tokens": m.num_context_tokens,
            "creation_time_s": m.batch_creation_time_s,
            "execution_time_s": m.batch_execution_time_s,
            "prompt_throughput": m.prompt_throughput,
            "generation_throughput": m.generation_throughput,
            "preemptions_total": m.total_preemption_count,
            "used_kv_pct": m.used_kv_pct,
            "total_kv_blocks": m.total_kv_blocks,
            "cache_hit_tokens": m.cache_hit_tokens or None,
            "cache_miss_tokens": m.cache_miss_tokens or None,
            "draft_tokens_generated": m.draft_tokens_generated or None,
            "draft_tokens_accepted": m.draft_tokens_accepted or None,
            "overlap_active": m.overlap_active,
            "completed": None if c is None else {
                "batch_type": c.batch_type.value, "batch_size": c.batch_size,
                "input_tokens": c.num_input_tokens, "context_tokens": c.num_context_tokens,
                "execution_time_s": c.execution_time_s,
            },
        }))
        f.write(b"\n")

    run(step, f.close)

else:
    raise SystemExit(f"unknown variant {variant}")
