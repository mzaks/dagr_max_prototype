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

Every edit is located through the AST (see astpatch.py): by class, function and the names being
assigned or called. Where a replacement has to keep MAX's own code, it is lifted from the file
being patched, so no MAX source is carried in this repository.
"""

import ast
import os
import shutil

import astpatch

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.join(HERE, ".venv", "lib", "python3.12", "site-packages", "max")
INPUTS = os.path.join(PKG, "pipelines", "modeling", "types", "pipeline_variants", "text_generation.py")
BLOCK_MANAGER = os.path.join(PKG, "pipelines", "kv_cache", "paged_kv_cache", "block_manager.py")


def _pristine(path: str, backup: str) -> str:
    if not os.path.exists(backup):
        shutil.copyfile(path, backup)
    return open(backup).read()


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

BATCH_SIZE_NEW = """        return sum(map(len, self.batches)) - self._num_padding_ctx  # PROTOTYPE
"""

METRICS_NEW = """        # PROTOTYPE: a null/default connector always reports an empty KVCacheMetrics and
        # x + 0 == x for every field, so return the manager's own metrics without the add.
        # The only caller (BatchMetrics) reads the fields and then calls reset_metrics.
        if type(self.connector).metrics in _empty_connector_metrics():
            return self._metrics
{original}
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

# The three per-tier block-count reads in compute_values, by the local each one assigns.
# At DP == 1 a single call replaces the comprehension; the original statements stay as the
# else branch, taken from the file being patched (never quoted here).
BLOCK_COUNT_FAST = {
    "block_counts": """            if num_replicas == 1:  # PROTOTYPE fast path, same values
                _bc = kv_cache.block_count(0)
                total_kv_blocks = _bc.total
                used_kv_blocks = _bc.used
            else:
{original}
""",
    "host_block_counts": """            if num_replicas == 1:  # PROTOTYPE fast path, same values
                host_block_counts = (kv_cache.host_block_count(0),)
                total_host_kv_blocks = host_block_counts[0].total
            else:
{original}
""",
    "disk_block_counts": """            if num_replicas == 1:  # PROTOTYPE fast path, same values
                disk_block_counts = (kv_cache.disk_block_count(0),)
                total_disk_kv_blocks = disk_block_counts[0].total
            else:
{original}
""",
}


def enabled() -> bool:
    return os.environ.get("PATCH_FAST_VALUES") == "1"


def fast_block_counts(utils_src: str) -> str:
    """Wrap each per-tier block-count read in a DP == 1 fast path.

    The three statements are found by the local they assign inside `compute_values`; the
    original comprehension and its sums become the else branch, lifted from the source.
    """
    for local, template in BLOCK_COUNT_FAST.items():
        tree = astpatch.parse(utils_src)
        fn = astpatch.find_function(tree, "compute_values", in_class="BatchMetrics")
        assign = astpatch.find_stmt(fn, lambda n, local=local: astpatch.assigns_to(n, local))
        # the assignment plus the sum(s) that immediately follow it, up to the next blank line
        block = [assign]
        body = _enclosing_body(fn, assign)
        idx = body.index(assign)
        for stmt in body[idx + 1:]:
            if isinstance(stmt, ast.Assign) and astpatch.calls(stmt, "sum"):
                block.append(stmt)
            elif isinstance(stmt, ast.Assert):
                block.append(stmt)
            else:
                break
        start, _ = astpatch.lines_of(utils_src, block[0])
        _, end = astpatch.lines_of(utils_src, block[-1])
        original = "".join(utils_src.splitlines(keepends=True)[start:end])
        utils_src = astpatch.replace_lines(
            utils_src, start, end,
            template.format(original=astpatch.reindent(original, "    ")),
        )
    return utils_src


def _enclosing_body(scope: ast.AST, stmt: ast.stmt) -> list:
    """The statement list that directly contains `stmt`."""
    for node in ast.walk(scope):
        for field in ("body", "orelse", "finalbody"):
            body = getattr(node, field, None)
            if isinstance(body, list) and stmt in body:
                return body
    raise astpatch.NotFound("enclosing body")


def patch_inputs() -> None:
    src = _pristine(INPUTS, os.path.join(HERE, "text_generation.py.orig"))
    if enabled():
        tree = astpatch.parse(src)
        post_init = astpatch.find_function(tree, "__post_init__",
                                           in_class="TextGenerationInputs")
        src = astpatch.replace_stmt(src, post_init, INPUTS_POST_INIT_NEW)
        tree = astpatch.parse(src)
        batch_size = astpatch.find_function(tree, "batch_size",
                                            in_class="TextGenerationInputs")
        src = astpatch.replace_body(src, batch_size, BATCH_SIZE_NEW)
    compile(src, INPUTS, "exec")
    open(INPUTS, "w").write(src)


def patch_block_manager() -> None:
    src = _pristine(BLOCK_MANAGER, os.path.join(HERE, "block_manager.py.orig"))
    if enabled():
        tree = astpatch.parse(src)
        metrics = astpatch.find_function(tree, "metrics", in_class="BlockManager")
        # keep MAX's own expression as the fallback, read from the file
        original = astpatch.reindent(astpatch.segment(src, metrics.body[-1]), "        ")
        src = astpatch.replace_body(src, metrics, METRICS_NEW.format(original=original))
        src += METRICS_HELPER
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
        tree = astpatch.parse(src)
        sibling = astpatch.find_function(tree, "get_metrics_aggregated",
                                         in_class="PagedKVCacheManager")
        src = astpatch.insert_before(src, sibling, SNAPSHOT_METHOD.lstrip("\n") + "\n")
        src += SNAPSHOT_HELPERS
    compile(src, CACHE_MANAGER, "exec")
    open(CACHE_MANAGER, "w").write(src)


def kv_snapshot_call(utils_src: str) -> str:
    """compute_values: use kv_cache.metrics_snapshot when the manager has it.

    The block to wrap is the `if kv_cache is not None:` branch inside compute_values; it stays
    as the elif fallback for managers without the method (and for test mocks), lifted from the
    file rather than quoted here.
    """
    tree = astpatch.parse(utils_src)
    fn = astpatch.find_function(tree, "compute_values", in_class="BatchMetrics")
    block = astpatch.find_stmt(
        fn, lambda n: isinstance(n, ast.If) and isinstance(n.test, ast.Compare)
        and astpatch.name_of(n.test.left) == ["kv_cache"]
        and isinstance(n.test.ops[0], ast.IsNot))
    start, end = astpatch.lines_of(utils_src, block)
    original = "".join(utils_src.splitlines(keepends=True)[start:end])
    original = original.replace("if kv_cache is not None:", "elif kv_cache is not None:", 1)
    names = ",\n".join(f"                {n}" for n in KV_SNAPSHOT_NAMES)
    head = (
        "        _kv_snapshot = (  # PROTOTYPE: one KV-cache call\n"
        "            None if kv_cache is None else getattr(kv_cache, \"metrics_snapshot\", None)\n"
        "        )\n"
        "        if _kv_snapshot is not None:\n"
        "            (\n" + names + ",\n"
        "            ) = _kv_snapshot(num_replicas)\n"
    )
    return astpatch.replace_lines(utils_src, start, end, head + original)
