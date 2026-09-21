"""Exact speedups for BatchMetrics.compute_values (PATCH_FAST_VALUES=1 in max_patch.py).

Every change keeps the published values identical:
  - TextGenerationInputs.__post_init__: one pass per replica instead of five passes over a
    rebuilt flat_batch; also counts DP padding contexts, so batch_size is
    sum(map(len, batches)) - padding instead of rebuilding flat_batch and rescanning it.
  - BlockManager.metrics: with a connector whose metrics are always empty (null / default),
    return the manager's own KVCacheMetrics instead of allocating an empty one and adding
    27 fields (x + 0 == x for every field).
  - compute_values: DP == 1 reads one BlockCount per tier without list comprehensions and
    generator sums.
"""

import os
import shutil

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.join(HERE, ".venv", "lib", "python3.12", "site-packages", "max")
INPUTS = os.path.join(PKG, "pipelines", "modeling", "types", "pipeline_variants", "text_generation.py")
BLOCK_MANAGER = os.path.join(PKG, "pipelines", "kv_cache", "paged_kv_cache", "block_manager.py")


def _pristine(path: str, backup: str) -> str:
    if not os.path.exists(backup):
        shutil.copyfile(path, backup)
    return open(backup).read()


def _replace_once(src: str, old: str, new: str) -> str:
    assert src.count(old) == 1, old
    return src.replace(old, new, 1)


INPUTS_POST_INIT_OLD = """    def __post_init__(self) -> None:
        self.input_tokens = sum(
            ctx.tokens.active_length for ctx in self.flat_batch
        )
        self.context_tokens = sum(
            ctx.tokens.processed_length for ctx in self.flat_batch
        )
        self.per_replica_input_tokens = [
            sum(
                ctx.tokens.active_length
                for ctx in batch
                if not getattr(ctx, "_is_padding_ctx", False)
            )
            for batch in self.batches
        ]
        self.per_replica_context_tokens = [
            sum(
                ctx.tokens.processed_length
                for ctx in batch
                if not getattr(ctx, "_is_padding_ctx", False)
            )
            for batch in self.batches
        ]
        self.batch_type = BatchType.TG
        for context in self.flat_batch:
            if context.tokens.generated_length == 0:
                self.batch_type = BatchType.CE
                break
"""

INPUTS_POST_INIT_NEW = """    def __post_init__(self) -> None:
        # PROTOTYPE: one pass per replica; also counts DP padding contexts for batch_size.
        input_tokens = 0
        context_tokens = 0
        per_replica_input_tokens = []
        per_replica_context_tokens = []
        num_padding = 0
        is_ce = False
        for batch in self.batches:
            replica_input = 0
            replica_context = 0
            for ctx in batch:
                tokens = ctx.tokens
                active = tokens.active_length
                processed = tokens.processed_length
                input_tokens += active
                context_tokens += processed
                if getattr(ctx, "_is_padding_ctx", False):
                    num_padding += 1
                else:
                    replica_input += active
                    replica_context += processed
                if not is_ce and tokens.generated_length == 0:
                    is_ce = True
            per_replica_input_tokens.append(replica_input)
            per_replica_context_tokens.append(replica_context)
        self.input_tokens = input_tokens
        self.context_tokens = context_tokens
        self.per_replica_input_tokens = per_replica_input_tokens
        self.per_replica_context_tokens = per_replica_context_tokens
        self._num_padding_ctx = num_padding
        self.batch_type = BatchType.CE if is_ce else BatchType.TG
"""

BATCH_SIZE_OLD = """        return sum(
            1 for context in self.flat_batch if not context._is_padding_ctx
        )
"""

BATCH_SIZE_NEW = """        return sum(map(len, self.batches)) - self._num_padding_ctx  # PROTOTYPE
"""

METRICS_OLD = """        return self._metrics + self.connector.metrics
"""

METRICS_NEW = """        # PROTOTYPE: a null/default connector always reports an empty KVCacheMetrics and
        # x + 0 == x for every field, so return the manager's own metrics without the add.
        # The only caller (BatchMetrics) reads the fields and then calls reset_metrics.
        if type(self.connector).metrics in _empty_connector_metrics():
            return self._metrics
        return self._metrics + self.connector.metrics
"""

METRICS_HELPER = """

# PROTOTYPE
_EMPTY_CONNECTOR_METRICS: tuple | None = None


def _empty_connector_metrics() -> tuple:
    global _EMPTY_CONNECTOR_METRICS
    if _EMPTY_CONNECTOR_METRICS is None:
        from max.pipelines.kv_cache.connectors.null_connector import NullConnector
        from max.pipelines.kv_cache.kv_connector import KVConnector

        _EMPTY_CONNECTOR_METRICS = (NullConnector.metrics, KVConnector.metrics)
    return _EMPTY_CONNECTOR_METRICS
"""

BLOCK_COUNTS = [
    (
        """            block_counts = [
                kv_cache.block_count(replica_idx)
                for replica_idx in range(num_replicas)
            ]
            total_kv_blocks = sum(bc.total for bc in block_counts)
            used_kv_blocks = sum(bc.used for bc in block_counts)
""",
        """            if num_replicas == 1:  # PROTOTYPE fast path, same values
                _bc = kv_cache.block_count(0)
                total_kv_blocks = _bc.total
                used_kv_blocks = _bc.used
            else:
                block_counts = [
                    kv_cache.block_count(replica_idx)
                    for replica_idx in range(num_replicas)
                ]
                total_kv_blocks = sum(bc.total for bc in block_counts)
                used_kv_blocks = sum(bc.used for bc in block_counts)
""",
    ),
    (
        """            host_block_counts = [
                kv_cache.host_block_count(replica_idx)
                for replica_idx in range(num_replicas)
            ]
            total_host_kv_blocks = sum(bc.total for bc in host_block_counts)
""",
        """            if num_replicas == 1:  # PROTOTYPE fast path, same values
                host_block_counts = (kv_cache.host_block_count(0),)
                total_host_kv_blocks = host_block_counts[0].total
            else:
                host_block_counts = [
                    kv_cache.host_block_count(replica_idx)
                    for replica_idx in range(num_replicas)
                ]
                total_host_kv_blocks = sum(bc.total for bc in host_block_counts)
""",
    ),
    (
        """            disk_block_counts = [
                kv_cache.disk_block_count(replica_idx)
                for replica_idx in range(num_replicas)
            ]
            total_disk_kv_blocks = sum(bc.total for bc in disk_block_counts)
""",
        """            if num_replicas == 1:  # PROTOTYPE fast path, same values
                disk_block_counts = (kv_cache.disk_block_count(0),)
                total_disk_kv_blocks = disk_block_counts[0].total
            else:
                disk_block_counts = [
                    kv_cache.disk_block_count(replica_idx)
                    for replica_idx in range(num_replicas)
                ]
                total_disk_kv_blocks = sum(bc.total for bc in disk_block_counts)
""",
    ),
]


def enabled() -> bool:
    return os.environ.get("PATCH_FAST_VALUES") == "1"


def fast_block_counts(utils_src: str) -> str:
    for old, new in BLOCK_COUNTS:
        utils_src = _replace_once(utils_src, old, new)
    return utils_src


def patch_inputs() -> None:
    src = _pristine(INPUTS, os.path.join(HERE, "text_generation.py.orig"))
    if enabled():
        src = _replace_once(src, INPUTS_POST_INIT_OLD, INPUTS_POST_INIT_NEW)
        src = _replace_once(src, BATCH_SIZE_OLD, BATCH_SIZE_NEW)
    compile(src, INPUTS, "exec")
    open(INPUTS, "w").write(src)


def patch_block_manager() -> None:
    src = _pristine(BLOCK_MANAGER, os.path.join(HERE, "block_manager.py.orig"))
    if enabled():
        src = _replace_once(src, METRICS_OLD, METRICS_NEW) + METRICS_HELPER
    compile(src, BLOCK_MANAGER, "exec")
    open(BLOCK_MANAGER, "w").write(src)


# ---- PATCH_KV_SNAPSHOT=1: one KV-cache call for every KV value BatchMetrics needs --------------

CACHE_MANAGER = os.path.join(PKG, "pipelines", "kv_cache", "paged_kv_cache", "cache_manager.py")

# Order of the tuple returned by PagedKVCacheManager.metrics_snapshot (compute_values locals).
KV_SNAPSHOT_NAMES = (
    "total_kv_blocks", "used_kv_pct", "total_host_kv_blocks", "used_host_kv_pct",
    "device_blocks_served", "h2d_blocks_copied", "d2h_blocks_copied",
    "cross_replica_blocks_copied", "cross_replica_bytes_copied", "disk_blocks_written",
    "disk_blocks_read", "inflight_disk_ops", "total_disk_kv_blocks", "used_disk_kv_pct",
    "nixl_read_latency_avg_ms", "nixl_write_latency_avg_ms", "rpc_acquire_latency_avg_ms",
    "rpc_read_latency_avg_ms", "nixl_read_gib_per_s", "nixl_write_gib_per_s",
    "dkv_connected_clients", "dkv_total_clients", "dkv_reconnect_attempts", "dkv_read_blocks",
)

SNAPSHOT_METHOD = '''
    def metrics_snapshot(self, num_replicas: int) -> tuple:  # PROTOTYPE
        """Every KV value BatchMetrics records, as one flat tuple, then resets the counters.

        Same values, in the same call order, as block_count / host_block_count /
        get_metrics_aggregated / disk_block_count per replica followed by reset_metrics;
        order: fast_values_patch.KV_SNAPSHOT_NAMES. Connectors that keep the default
        (empty) tier counts, metrics and reset skip those calls.
        """
        flags = self.__dict__.get("_snapshot_flags")
        if flags is None:
            flags = self._snapshot_flags = _snapshot_flags(self._block_manager, self._connector)
        default_counts, empty_metrics, default_manager = flags
        bm = self._block_manager
        pools = bm.device_block_pools
        total = bm.total_num_blocks
        if num_replicas == 1:
            total_kv_blocks = total
            used_kv_blocks = total - len(pools[0].free_block_queue)
        else:
            total_kv_blocks = 0
            used_kv_blocks = 0
            for replica_idx in range(num_replicas):
                total_kv_blocks += total
                used_kv_blocks += total - len(pools[replica_idx].free_block_queue)
        assert total_kv_blocks > 0
        used_kv_pct = used_kv_blocks / total_kv_blocks

        total_host_kv_blocks = 0
        used_host_kv_pct = 0.0
        total_disk_kv_blocks = 0
        used_disk_kv_pct = 0.0
        if not default_counts:
            host = [self._replica[i].connector.host_block_count for i in range(num_replicas)]
            total_host_kv_blocks = sum(bc.total for bc in host)
            if total_host_kv_blocks > 0:
                used_host_kv_pct = sum(bc.used for bc in host) / total_host_kv_blocks

        m = bm._metrics if empty_metrics and default_manager else bm.metrics

        if not default_counts:
            disk = [self._replica[i].connector.disk_block_count for i in range(num_replicas)]
            total_disk_kv_blocks = sum(bc.total for bc in disk)
            if total_disk_kv_blocks > 0:
                used_disk_kv_pct = sum(bc.used for bc in disk) / total_disk_kv_blocks

        read_ms = m.nixl_read_latency_total_ms
        write_ms = m.nixl_write_latency_total_ms
        c = m.nixl_read_latency_count
        nixl_read_latency_avg_ms = 0.0 if c == 0 else read_ms / c
        c = m.nixl_write_latency_count
        nixl_write_latency_avg_ms = 0.0 if c == 0 else write_ms / c
        c = m.rpc_acquire_latency_count
        rpc_acquire_latency_avg_ms = 0.0 if c == 0 else m.rpc_acquire_latency_total_ms / c
        c = m.rpc_read_latency_count
        rpc_read_latency_avg_ms = 0.0 if c == 0 else m.rpc_read_latency_total_ms / c
        nixl_read_gib_per_s = (
            0.0 if read_ms <= 0 else (m.nixl_read_bytes / (1 << 30)) / (read_ms / 1000)
        )
        nixl_write_gib_per_s = (
            0.0 if write_ms <= 0 else (m.nixl_write_bytes / (1 << 30)) / (write_ms / 1000)
        )
        out = (
            total_kv_blocks, used_kv_pct, total_host_kv_blocks, used_host_kv_pct,
            m.device_blocks_served, m.h2d_blocks_copied, m.d2h_blocks_copied,
            m.cross_replica_blocks_copied, m.cross_replica_bytes_copied, m.disk_blocks_written,
            m.disk_blocks_read, m.inflight_disk_ops, total_disk_kv_blocks, used_disk_kv_pct,
            nixl_read_latency_avg_ms, nixl_write_latency_avg_ms, rpc_acquire_latency_avg_ms,
            rpc_read_latency_avg_ms, nixl_read_gib_per_s, nixl_write_gib_per_s,
            m.dkv_connected_clients, m.dkv_total_clients, m.dkv_reconnect_attempts,
            m.nixl_read_blocks,
        )
        if empty_metrics and default_manager:
            # BlockManager.reset_metrics with a no-op connector reset: a fresh zeroed
            # KVCacheMetrics, built without running the 27-argument __init__.
            fresh = _KVCacheMetrics.__new__(_KVCacheMetrics)
            fresh.__dict__.update(_ZERO_KV_METRICS)
            bm._metrics = fresh
        else:
            self.reset_metrics()
        return out
'''

SNAPSHOT_HELPERS = '''

# PROTOTYPE: metrics_snapshot helpers
from max.nn.kv_cache.metrics import KVCacheMetrics as _KVCacheMetrics  # noqa: E402

_ZERO_KV_METRICS = dict(_KVCacheMetrics().__dict__)


def _snapshot_flags(block_manager, connector) -> tuple[bool, bool, bool]:
    from max.pipelines.kv_cache.connectors.null_connector import NullConnector
    from max.pipelines.kv_cache.kv_connector import KVConnector
    from max.pipelines.kv_cache.paged_kv_cache.block_manager import BlockManager

    ctype = type(connector)
    default_counts = (
        ctype.host_block_count is KVConnector.host_block_count
        and ctype.disk_block_count is KVConnector.disk_block_count
    )
    empty_metrics = (
        ctype.metrics in (NullConnector.metrics, KVConnector.metrics)
        and ctype.reset_metrics is KVConnector.reset_metrics
    )
    btype = type(block_manager)
    default_manager = (
        btype.metrics is BlockManager.metrics
        and btype.reset_metrics is BlockManager.reset_metrics
        and "_metrics" in vars(block_manager)
    )
    return default_counts, empty_metrics, default_manager
'''


def snapshot_enabled() -> bool:
    return os.environ.get("PATCH_KV_SNAPSHOT") == "1"


def patch_cache_manager() -> None:
    src = _pristine(CACHE_MANAGER, os.path.join(HERE, "cache_manager.py.orig"))
    if snapshot_enabled():
        anchor = "    def get_metrics_aggregated(self) -> KVCacheMetrics:\n"
        src = _replace_once(src, anchor, SNAPSHOT_METHOD.lstrip("\n") + "\n" + anchor) + SNAPSHOT_HELPERS
    compile(src, CACHE_MANAGER, "exec")
    open(CACHE_MANAGER, "w").write(src)


def kv_snapshot_call(utils_src: str) -> str:
    """compute_values: use kv_cache.metrics_snapshot when the manager has it; the original
    per-call block stays as the fallback (other managers, test mocks)."""
    start_anchor = "        if kv_cache is not None:\n            # TODO SERVOPT-939: Add some sugar\n"
    end_anchor = "            kv_cache.reset_metrics()\n"
    assert utils_src.count(start_anchor) == 1 and utils_src.count(end_anchor) == 1
    names = ",\n".join(f"                {n}" for n in KV_SNAPSHOT_NAMES)
    new_head = (
        "        _kv_snapshot = (  # PROTOTYPE: one KV-cache call\n"
        "            None if kv_cache is None else getattr(kv_cache, \"metrics_snapshot\", None)\n"
        "        )\n"
        "        if _kv_snapshot is not None:\n"
        "            (\n" + names + ",\n"
        "            ) = _kv_snapshot(num_replicas)\n"
        "        elif kv_cache is not None:\n            # TODO SERVOPT-939: Add some sugar\n"
    )
    return utils_src.replace(start_anchor, new_head, 1)
