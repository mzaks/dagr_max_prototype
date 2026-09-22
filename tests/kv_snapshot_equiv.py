"""metrics_snapshot equivalence, end to end through BatchMetrics.compute_values.

For random scheduler steps (parity_test.scenario) and random KV state on a real BlockManager
(DP 1/2/4, free blocks per pool, random KVCacheMetrics), compute_values is run twice from the
same state:
  A  kv_cache = proxy exposing only block_count / host_byte_count / get_metrics_aggregated /
     disk_byte_count / reset_metrics (original per-call path)
  B  kv_cache = the PagedKVCacheManager itself (metrics_snapshot path)
The value tuples must be identical (values and types), and so must the post-reset state.
Connectors: NullConnector (fast path) and a tiered fake overriding counts, metrics and reset
(generic path).

Run after `PATCH_FAST_VALUES=1 PATCH_KV_SNAPSHOT=1 python3 max_patch.py`:
    .venv/bin/python -I tests/kv_snapshot_equiv.py [steps]
"""

import copy
import dataclasses
import importlib
import os
import random
import sys
from types import SimpleNamespace

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
importlib.import_module("max._core_mojo")

from max.nn.kv_cache.metrics import KVCacheMetrics  # noqa: E402
from max.pipelines.kv_cache.connectors.null_connector import NullConnector  # noqa: E402
from max.pipelines.kv_cache.kv_connector import ByteCount  # noqa: E402
from max.pipelines.kv_cache.paged_kv_cache.block_manager import BlockManager  # noqa: E402
from max.pipelines.kv_cache.paged_kv_cache.cache_manager import PagedKVCacheManager  # noqa: E402
from max.serve.scheduler.utils import BatchMetrics  # noqa: E402

from parity_test import scenario  # noqa: E402

FIELDS = [f.name for f in dataclasses.fields(KVCacheMetrics)]


def random_metrics(rng: random.Random) -> KVCacheMetrics:
    zero = KVCacheMetrics()
    vals = {}
    for n in FIELDS:
        if isinstance(getattr(zero, n), float):
            vals[n] = rng.choice([0.0, 0.0, rng.random() * 50, -0.0])
        else:
            vals[n] = rng.choice([0, 0, rng.randint(0, 1 << 40), rng.randint(0, 9)])
    return KVCacheMetrics(**vals)


class TieredFake(NullConnector):
    def __init__(self) -> None:
        self.host = ByteCount(free=0, total=0)
        self.disk = ByteCount(free=0, total=0)
        self.m = KVCacheMetrics()
        self.resets = 0

    @property
    def host_byte_count(self) -> ByteCount:
        return self.host

    @property
    def disk_byte_count(self) -> ByteCount:
        return self.disk

    @property
    def metrics(self) -> KVCacheMetrics:
        return self.m

    def reset_metrics(self) -> None:
        self.resets += 1
        self.m = KVCacheMetrics()


def make_manager(dp: int, connector) -> PagedKVCacheManager:
    bm = BlockManager(total_num_blocks=256, block_size=16, connector=connector,
                      enable_prefix_caching=False, num_replicas=dp)
    mgr = object.__new__(PagedKVCacheManager)
    mgr._block_manager = bm
    mgr._connector = connector
    mgr._replica = [SimpleNamespace(block_manager=bm, connector=connector) for _ in range(dp)]
    return mgr


def set_state(mgr: PagedKVCacheManager, rng_state, dp: int, tiered: bool) -> None:
    rng = random.Random(rng_state)
    bm = mgr._block_manager
    for pool in bm.device_block_pools:
        pool.free_block_queue.num_free_blocks = rng.randint(0, bm.total_num_blocks)
    bm._metrics = random_metrics(rng)
    if tiered:
        c = mgr._connector
        ht, dt = rng.choice([0, 4096 << 17]), rng.choice([0, 65536 << 17])
        c.host = ByteCount(free=rng.randint(0, ht), total=ht)
        c.disk = ByteCount(free=rng.randint(0, dt), total=dt)
        c.m = random_metrics(rng)


def proxy(mgr: PagedKVCacheManager) -> SimpleNamespace:
    return SimpleNamespace(
        block_count=mgr.block_count, host_byte_count=mgr.host_byte_count,
        disk_byte_count=mgr.disk_byte_count,
        get_metrics_aggregated=mgr.get_metrics_aggregated, reset_metrics=mgr.reset_metrics,
    )


def post_state(mgr: PagedKVCacheManager):
    c = mgr._connector
    return (dataclasses.astuple(mgr._block_manager._metrics),
            getattr(c, "resets", None), dataclasses.astuple(getattr(c, "m", KVCacheMetrics())))


def main() -> None:
    steps = int(sys.argv[1]) if len(sys.argv) > 1 else 20000
    rng = random.Random(11)
    managers = {(dp, tiered): make_manager(dp, TieredFake() if tiered else NullConnector())
                for dp in (1, 2, 4) for tiered in (False, True)}
    fast_path = {k: None for k in managers}
    for i in range(steps):
        sc = scenario(rng)
        dp = sc["sch_config"].data_parallel_degree
        tiered = rng.random() < 0.5
        mgr = managers[(dp, tiered)]
        state = rng.getrandbits(64)

        set_state(mgr, state, dp, tiered)
        a = BatchMetrics.compute_values(**{**copy.deepcopy(sc), "kv_cache": proxy(mgr)})
        post_a = post_state(mgr)

        set_state(mgr, state, dp, tiered)
        if tiered:
            mgr._connector.resets -= 1  # proxy path already counted one reset
        b = BatchMetrics.compute_values(**{**copy.deepcopy(sc), "kv_cache": mgr})
        post_b = post_state(mgr)
        if tiered:
            mgr._connector.resets = post_a[1]

        assert len(a) == len(b)
        for j, (x, y) in enumerate(zip(a, b, strict=True)):
            assert x == y and type(x) is type(y), (i, j, x, y)
        assert post_a == post_b, (i, post_a, post_b)
        fast_path[(dp, tiered)] = mgr.__dict__.get("_snapshot_flags")
    print(f"metrics_snapshot: {steps} steps, value tuples and post-reset state identical")
    for k, v in sorted(fast_path.items()):
        print(f"  dp={k[0]} tiered_fake={k[1]}: flags (default_counts, empty_metrics, default_manager) = {v}")


if __name__ == "__main__":
    main()
