"""Equivalence of the PATCH_FAST_VALUES changes against the pristine MAX sources.

TextGenerationInputs: original (*.orig source) vs patched class over random batches with DP,
padding contexts, and CE/TG mixes — input_tokens, context_tokens, per-replica sums,
batch_type and batch_size must match. BlockManager.metrics: patched fast path must equal
`_metrics + NullConnector().metrics` field for field.

Run after `PATCH_FAST_VALUES=1 python3 max_patch.py`:  .venv/bin/python -I tests/fast_values_equiv.py
"""

import dataclasses
import importlib
import os
import random
import sys
import types
from types import SimpleNamespace

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
importlib.import_module("max._core_mojo")

from max.nn.kv_cache.metrics import KVCacheMetrics  # noqa: E402
from max.pipelines.kv_cache.connectors.null_connector import NullConnector  # noqa: E402
from max.pipelines.kv_cache.paged_kv_cache import block_manager  # noqa: E402
from max.pipelines.modeling.types.pipeline_variants import text_generation as patched  # noqa: E402


def load_original() -> types.ModuleType:
    mod = types.ModuleType("text_generation_orig")
    mod.__dict__["__name__"] = "text_generation_orig"
    sys.modules["text_generation_orig"] = mod
    src = open(os.path.join(HERE, "text_generation.py.orig")).read()
    exec(compile(src, "text_generation.py.orig", "exec"), mod.__dict__)
    return mod


def ctx(rng: random.Random, padding: bool):
    return SimpleNamespace(
        _is_padding_ctx=padding,
        tokens=SimpleNamespace(
            active_length=rng.randint(0, 4096),
            processed_length=rng.randint(0, 65536),
            generated_length=rng.choice([0, 1, 1, 1, rng.randint(2, 500)]),
        ),
    )


def main() -> None:
    assert "PROTOTYPE" in open(patched.__file__).read(), "run with PATCH_FAST_VALUES=1 first"
    orig = load_original()
    rng = random.Random(7)
    fields = ("input_tokens", "context_tokens", "per_replica_input_tokens",
              "per_replica_context_tokens", "batch_type", "batch_size")
    for i in range(20000):
        dp = rng.choice([1, 1, 2, 4])
        batches = []
        for _ in range(dp):
            batch = [ctx(rng, rng.random() < 0.1) for _ in range(rng.choice([0, 1, 3, 32, 512]))]
            if rng.random() < 0.3:  # all TG
                for c in batch:
                    c.tokens.generated_length = max(1, c.tokens.generated_length)
            batches.append(batch)
        a = orig.TextGenerationInputs(batches=[list(b) for b in batches])
        b = patched.TextGenerationInputs(batches=[list(b) for b in batches])
        for f in fields:
            va, vb = getattr(a, f), getattr(b, f)
            assert va == vb and type(va) is type(vb) or (f == "batch_type" and va.value == vb.value), \
                (i, f, va, vb)
    print("TextGenerationInputs: 20000 random batches, all fields equal")

    names = [f.name for f in dataclasses.fields(KVCacheMetrics)]
    bm = object.__new__(block_manager.BlockManager)
    bm.connector = object.__new__(NullConnector)
    for i in range(20000):
        m = KVCacheMetrics(**{
            n: (rng.random() * 1e3 if isinstance(getattr(KVCacheMetrics(), n), float) else rng.randint(0, 1 << 40))
            for n in names
        })
        bm._metrics = m
        fast = bm.metrics
        slow = m + NullConnector.metrics.fget(bm.connector)
        for n in names:
            assert getattr(fast, n) == getattr(slow, n) and type(getattr(fast, n)) is type(getattr(slow, n)), (i, n)
        props = [p for p, v in vars(KVCacheMetrics).items() if isinstance(v, property)]
        for p in props:
            assert getattr(fast, p) == getattr(slow, p), (i, p)
    print(f"BlockManager.metrics: 20000 random KVCacheMetrics, {len(names)} fields and {len(props)} properties equal")


if __name__ == "__main__":
    main()
