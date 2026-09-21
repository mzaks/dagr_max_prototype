"""Microbench the expensive pieces of BatchMetrics.compute_values with MAX's real classes.
Usage: .venv/bin/python -I tests/bench_pieces.py <piece> [batch]   (one piece per process)"""
import sys, time, gc
import importlib
importlib.import_module("max._core_mojo")
from max.pipelines.context.context import TextContext
from max.pipelines.modeling.types.pipeline_variants.text_generation import TextGenerationInputs
from max.nn.kv_cache.metrics import KVCacheMetrics
from max.pipelines.kv_cache.kv_connector import BlockCount

piece = sys.argv[1]
n = int(sys.argv[2]) if len(sys.argv) > 2 else 512

ctxs = []
for i in range(n):
    c = TextContext.new_padding_context(max_length=512, model_name="m")
    c._is_padding_ctx = False
    ctxs.append(c)
inputs = TextGenerationInputs(batches=[ctxs])
a, b = KVCacheMetrics(), KVCacheMetrics()

def batch_size_property():
    return inputs.batch_size
def batch_size_nested_gen():
    return sum(1 for batch in inputs.batches for c in batch if not c._is_padding_ctx)
def batch_size_len():
    return sum(map(len, inputs.batches))
def kvm_add():
    return a + b
def kvm_new():
    return KVCacheMetrics()
def block_count():
    return BlockCount(free=3, total=2048)

fn = globals()[piece]
reps = 20000
gc.disable()
for _ in range(2000):
    fn()
ts = []
pc = time.perf_counter_ns
for _ in range(7):
    t0 = pc()
    for _ in range(reps):
        fn()
    ts.append((pc() - t0) / reps)
ts.sort()
print(f"{piece:<24} n={n:<4} best {ts[0]/1e3:7.3f} us  median {ts[3]/1e3:7.3f} us")
