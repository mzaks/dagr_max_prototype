"""Fill one BatchStepSlot SharedBuffer record (136 bytes) from a MAX BatchMetrics in Python.

Layout comes from `dagr_layout.compute_layout(BATCH_STEP_SLOT)` (printed by check.py).
Presence rules match batch_log.mojo `_from_metrics` so both paths log identical records:
  always present: terminated/pending/input/context tokens, execution time, throughputs,
                  preemptions, used_kv_pct, total_kv_blocks, overlap_active
  present iff != 0: cache_hit_tokens, cache_miss_tokens, draft_tokens_generated/accepted
  completed: present iff not None (its input/context tokens always present)
The ts_ns slot is left 0: batch_log stamps the time in commit().
"""

import dataclasses
import os
import random
import struct
import sys

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "gen", "python"))

from max.pipelines.modeling.types.pipeline_variants.text_generation import (  # noqa: E402
    BatchType,
    CompletedBatchStats,
)
from max.serve.scheduler.utils import BatchMetrics  # noqa: E402

SLOT_SIZE = 136
TG = BatchType.TG


def make_metrics(rng: random.Random, i: int) -> BatchMetrics:
    kwargs = {}
    for f in dataclasses.fields(BatchMetrics):
        if f.default is not dataclasses.MISSING or f.default_factory is not dataclasses.MISSING:
            continue
        kwargs[f.name] = [] if f.name == "acceptance_rate_per_position" else 0
    kwargs.update(
        batch_type=BatchType.TG if i % 7 else BatchType.CE,
        batch_size=rng.randint(1, 512),
        max_batch_size=512,
        terminated_reqs=rng.randint(0, 5),
        num_pending_reqs=rng.randint(0, 100),
        num_input_tokens=rng.randint(1, 8192),
        max_batch_input_tokens=8192,
        num_context_tokens=rng.randint(0, 200_000),
        batch_creation_time_s=rng.random() * 0.003,
        batch_execution_time_s=rng.random() * 0.4,
        prompt_throughput=rng.random() * 50_000,
        generation_throughput=rng.random() * 5_000,
        total_preemption_count=rng.randint(0, 3),
        used_kv_pct=rng.random() * 100,
        total_kv_blocks=18_000,
        cache_hit_tokens=rng.choice([0, rng.randint(1, 4000)]),
        cache_miss_tokens=rng.choice([0, rng.randint(1, 4000)]),
        draft_tokens_generated=rng.choice([0, rng.randint(1, 64)]),
        draft_tokens_accepted=rng.choice([0, rng.randint(1, 64)]),
        avg_acceptance_length=0.0,
        max_acceptance_length=0,
        overlap_active=bool(i % 3),
    )
    m = BatchMetrics(**kwargs)
    if i % 3:
        m.completed = CompletedBatchStats(
            batch_type=BatchType.TG,
            batch_size=rng.randint(1, 512),
            num_input_tokens=rng.randint(1, 512),
            num_context_tokens=rng.randint(0, 200_000),
            execution_time_s=rng.random() * 0.4,
        )
    return m


def new_slot() -> np.ndarray:
    """A zeroed, stable, writable 136-byte record; pass `slot.ctypes.data` to BatchLog.attach."""
    return np.zeros(SLOT_SIZE, dtype=np.uint8)


# ── 1) one precompiled struct.pack_into over the whole record ────────────────────
#  0..63   ts u64, context u64, creation f64, execution f64, prompt f64, gen f64, preempt u64, used_kv f64
#  64..95  completed: context u64, execution f64, batch_size u32, input u32, bits u8 (+7 pad)
#          bits: input present (0), context present (1), batch_type (2)
#  96..131 batch_size, terminated, pending, input, total_kv, hit, miss, draft_gen, draft_acc (u32 each)
#  132     presence: terminated..preempt (bits 0..7)
#  133     presence: used_kv(0) total_kv(1) hit(2) miss(3) draft_gen(4) draft_acc(5) overlap(6) completed(7)
#  134     batch_type (bit 0), overlap_active (bit 1)
_RECORD = struct.Struct("<QQddddQd" "QdIIB7x" "IIIIIIIII" "BBBx")
assert _RECORD.size == SLOT_SIZE


def make_struct_writer(slot: np.ndarray):
    pack_into = _RECORD.pack_into

    def write(m) -> None:
        c = m.completed
        hit = m.cache_hit_tokens
        miss = m.cache_miss_tokens
        dg = m.draft_tokens_generated
        da = m.draft_tokens_accepted
        ov = m.overlap_active
        b133 = 0x43 | (hit != 0) << 2 | (miss != 0) << 3 | (dg != 0) << 4 | (da != 0) << 5
        if c is None:
            cc = ce = cbs = ci = cbits = 0
        else:
            b133 |= 0x80
            cc = c.num_context_tokens
            ce = c.execution_time_s
            cbs = c.batch_size
            ci = c.num_input_tokens
            cbits = 0x03 | (c.batch_type is TG) << 2
        pack_into(
            slot, 0,
            0, m.num_context_tokens, m.batch_creation_time_s, m.batch_execution_time_s,
            m.prompt_throughput, m.generation_throughput, m.total_preemption_count, m.used_kv_pct,
            cc, ce, cbs, ci, cbits,
            m.batch_size, m.terminated_reqs, m.num_pending_reqs, m.num_input_tokens,
            m.total_kv_blocks, hit, miss, dg, da,
            0xFF, b133, (m.batch_type is TG) | (bool(ov) << 1),
        )

    return write


# ── 2) numpy per-field views onto the same record ───────────────────────────────
def make_numpy_writer(slot: np.ndarray):
    u64 = slot[0:64].view(np.uint64)          # indices 1 (context), 6 (preempt)
    f64 = slot[0:64].view(np.float64)         # indices 2..5 (creation..gen), 7 (used_kv)
    c_u64 = slot[64:72].view(np.uint64)
    c_f64 = slot[72:80].view(np.float64)
    c_u32 = slot[80:88].view(np.uint32)
    u32 = slot[96:132].view(np.uint32)
    bits = slot[132:136]
    zero32 = bytes(32)

    def write(m) -> None:
        u64[1] = m.num_context_tokens
        f64[2] = m.batch_creation_time_s
        f64[3] = m.batch_execution_time_s
        f64[4] = m.prompt_throughput
        f64[5] = m.generation_throughput
        u64[6] = m.total_preemption_count
        f64[7] = m.used_kv_pct
        hit = m.cache_hit_tokens
        miss = m.cache_miss_tokens
        dg = m.draft_tokens_generated
        da = m.draft_tokens_accepted
        u32[0] = m.batch_size
        u32[1] = m.terminated_reqs
        u32[2] = m.num_pending_reqs
        u32[3] = m.num_input_tokens
        u32[4] = m.total_kv_blocks
        u32[5] = hit
        u32[6] = miss
        u32[7] = dg
        u32[8] = da
        b133 = 0x43 | (hit != 0) << 2 | (miss != 0) << 3 | (dg != 0) << 4 | (da != 0) << 5
        c = m.completed
        if c is None:
            slot[64:96] = np.frombuffer(zero32, dtype=np.uint8)
        else:
            b133 |= 0x80
            c_u64[0] = c.num_context_tokens
            c_f64[0] = c.execution_time_s
            c_u32[0] = c.batch_size
            c_u32[1] = c.num_input_tokens
            slot[88] = 0x03 | (c.batch_type is TG) << 2
        bits[0] = 0xFF
        bits[1] = b133
        bits[2] = (m.batch_type is TG) | (bool(m.overlap_active) << 1)

    return write


# ── 3) Dagr's reflective SharedBuffer writer (dagr_sb_writer.write_region) ──────
def make_dagr_py_writer(slot: np.ndarray):
    from dagr_sb_writer import write_region
    from dagr_schema import BATCH_STEP_SLOT

    view = memoryview(slot).cast("B")

    def write(m) -> None:
        c = m.completed
        values = {
            "ts_ns": 0,
            "batch_type": 1 if m.batch_type is TG else 0,
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
            "overlap_active": bool(m.overlap_active),
            "completed": None if c is None else {
                "batch_type": 1 if c.batch_type is TG else 0,
                "batch_size": c.batch_size,
                "input_tokens": c.num_input_tokens,
                "context_tokens": c.num_context_tokens,
                "execution_time_s": c.execution_time_s,
            },
        }
        view[:] = write_region(BATCH_STEP_SLOT, values)

    return write


WRITERS = {
    "struct": make_struct_writer,
    "numpy": make_numpy_writer,
    "dagr_py": make_dagr_py_writer,
}
