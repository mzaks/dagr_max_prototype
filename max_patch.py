"""Patch the venv's MAX for design 1. Always starts from the pristine backups (*.orig).

  serve/scheduler/utils.py
    - BatchMetrics.compute_values(...)  = create()'s body, returning the values tuple in
      record_spec order (derived from create's own `return cls(...)` keyword arguments)
    - BatchMetrics.from_values(values)  -> BatchMetrics
    - BatchMetrics.create(...)          = from_values(compute_values(...))   (one code path)
    - log_metrics: MAX_SERVE_RECORD_METRICS=<path> appends one record per step (vision and
      video metrics included as sub-records) and never publishes in the worker; the periodic
      log line builds the BatchMetrics only when it is due. MAX_SERVE_STAGE_TIMING=1 logs stage times.
  serve/telemetry/process_controller.py
    - with MAX_SERVE_RECORD_METRICS set, the telemetry process tails the log and publishes.
  PATCH_FAST_VALUES=1: exact compute_values speedups, see fast_values_patch.py
  PATCH_KV_SNAPSHOT=1: PagedKVCacheManager.metrics_snapshot, one KV call in compute_values
  serve/pipelines/model_worker.py
    - every NO_PROGRESS iteration (and worker-loop exit) flushes records still buffered.

Run: python3 max_patch.py
"""

import ast
import os
import shutil

import astpatch

import fast_values_patch
from record_spec import NAMES

HERE = os.path.dirname(os.path.abspath(__file__))
SITE = os.path.join(HERE, ".venv", "lib", "python3.12", "site-packages", "max", "serve")
UTILS = os.path.join(SITE, "scheduler", "utils.py")
PROC = os.path.join(SITE, "telemetry", "process_controller.py")
WORKER = os.path.join(SITE, "pipelines", "model_worker.py")
TEL_WORKER = os.path.join(SITE, "pipelines", "telemetry_worker.py")


def pristine(path: str, backup: str) -> str:
    if not os.path.exists(backup):
        shutil.copyfile(path, backup)
    return open(backup).read()


def patch_utils() -> None:
    src = pristine(UTILS, os.path.join(HERE, "utils.py.orig"))
    tree = ast.parse(src)
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "BatchMetrics")
    create = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == "create")
    ret = create.body[-1]
    assert isinstance(ret, ast.Return) and isinstance(ret.value, ast.Call)
    kwargs = {kw.arg: ast.get_source_segment(src, kw.value) for kw in ret.value.keywords}
    missing = [n for n in NAMES if n not in kwargs]
    assert not missing, f"record_spec fields not set by create(): {missing}"
    not_recorded = sorted(set(kwargs) - set(NAMES))
    assert not_recorded == ["video_metrics", "vision_metrics"], not_recorded

    lines = src.splitlines(keepends=True)
    # 1) the tuple return, replacing `return cls(...)`
    ret_lines = "".join(lines[ret.lineno - 1:ret.end_lineno])
    indent = " " * ret.col_offset
    tuple_src = indent + "return (  # PROTOTYPE: values in record_spec order\n" + "".join(
        f"{indent}    {kwargs[n]},\n" for n in NAMES) + f"{indent})\n"
    # 2) rename create -> compute_values and add create / from_values after it
    def_line = lines[create.lineno - 1]
    assert "def create(" in def_line
    params = "".join(lines[create.lineno - 1:create.body[0].lineno - 1])
    call_args = ", ".join(f"{a.arg}={a.arg}" for a in create.args.args[1:])
    new_methods = f'''
    _RECORD_NAMES = {NAMES!r}  # PROTOTYPE

    @classmethod
    def from_values(
        cls,
        values: tuple,
        vision_metrics: VisionEncoderMetrics | None = None,
        video_metrics: VideoEncoderMetrics | None = None,
    ) -> BatchMetrics:  # PROTOTYPE
        return cls(
            **dict(zip(cls._RECORD_NAMES, values[: len(cls._RECORD_NAMES)], strict=True)),
            vision_metrics=vision_metrics,
            video_metrics=video_metrics,
        )

    @classmethod
{params.replace("def create(", "def create(", 1).rstrip()}
        return cls.from_values(
            cls.compute_values({call_args}),
            vision_metrics=batch_vision_metrics,
            video_metrics=batch_video_metrics,
        )
'''
    out = (
        "".join(lines[:create.lineno - 1])
        + def_line.replace("def create(", "def compute_values(", 1)
        + "".join(lines[create.lineno:ret.lineno - 1])
        + tuple_src
        + new_methods
        + "".join(lines[ret.end_lineno:])
    )
    # decorator of the original create already precedes compute_values; the new create needs
    # @classmethod — added above. The return annotation of compute_values now lies (tuple).

    # 3) log_metrics: record mode + stage timing, located through the AST
    tree = ast.parse(out)
    log_metrics = next(n for n in ast.walk(tree)
                       if isinstance(n, ast.FunctionDef) and n.name == "log_metrics")
    create_call = astpatch.find_stmt(
        log_metrics, lambda n: astpatch.assigns_to(n, "metrics")
        and astpatch.calls(n, "BatchMetrics.create"))
    # the record path calls compute_values with the call site's own arguments
    call = create_call.value
    args = "".join(f"                {kw.arg}={astpatch.segment(out, kw.value)},\n"
                   for kw in call.keywords)
    record_mode = f"""        _t0 = time.perf_counter_ns()  # PROTOTYPE
        if _REC_PATH:  # PROTOTYPE design 1: record instead of object + publish
            values = BatchMetrics.compute_values(
{args}            )
            _t1 = time.perf_counter_ns()
            metrics = None
            _t2 = time.perf_counter_ns()
            # vision / video metrics ride along as sub-records: nothing is published here
            _rec_append(values + (False, batch_vision_metrics, batch_video_metrics))
            _t3 = time.perf_counter_ns()
            now = time.monotonic()
            if self.log_interval_s < now - self.time_of_last_log:
                self.time_of_last_log = now
                if metrics is None:
                    metrics = BatchMetrics.from_values(values, batch_vision_metrics, batch_video_metrics)
                logger.info(metrics.pretty_format(), extra=metrics.to_log_extra())
            _stage_record(_t1 - _t0, _t2 - _t1, _t3 - _t2, time.perf_counter_ns() - _t3)
            return

"""
    out = astpatch.insert_before(out, create_call, record_mode)

    # bracket the publish transaction with stage timers, and time the log line after it
    tree = ast.parse(out)
    log_metrics = next(n for n in ast.walk(tree)
                       if isinstance(n, ast.FunctionDef) and n.name == "log_metrics")
    publish = astpatch.find_stmt(
        log_metrics, lambda n: isinstance(n, ast.With) and astpatch.calls(n, "METRICS.transaction"))
    _, publish_end = astpatch.lines_of(out, publish)
    out = astpatch.replace_lines(
        out, publish_end, publish_end,
        "        _t2 = time.perf_counter_ns()  # PROTOTYPE\n")
    out = astpatch.insert_before(
        out, publish, "        _t1 = time.perf_counter_ns()  # PROTOTYPE\n")
    # after the periodic log line, which is the last statement of log_metrics
    tree = ast.parse(out)
    log_metrics = next(n for n in ast.walk(tree)
                       if isinstance(n, ast.FunctionDef) and n.name == "log_metrics")
    _, fn_end = astpatch.lines_of(out, log_metrics.body[-1])
    out = astpatch.replace_lines(
        out, fn_end, fn_end,
        "        _stage_record(_t1 - _t0, _t2 - _t1, 0, time.perf_counter_ns() - _t2)"
        "  # PROTOTYPE\n")
    out += '''

# ---- PROTOTYPE (~/dev/dagr_max_prototype, design 1) ----------------------------------
_REC_PATH = os.environ.get("MAX_SERVE_RECORD_METRICS")
_REC_LOG = None
_REC_LAST_FLUSH = 0.0
_REC_FLUSH_S = float(os.environ.get("MAX_SERVE_RECORD_METRICS_FLUSH_S", "0.05"))
_REC_MMAP = os.environ.get("MAX_SERVE_RECORD_METRICS_MMAP") == "1"
_REC_PENDING = False
_REC_FLUSH_COUNTS = [0, 0, 0]  # append-triggered flushes, idle flushes, idle flush ns


def _rec_append(values: tuple) -> None:
    global _REC_LOG, _REC_LAST_FLUSH, _REC_PENDING
    if _REC_LOG is None:
        import importlib
        import sys

        sys.path.insert(0, os.environ["MAX_SERVE_RECORD_METRICS_MODULE_DIR"])
        metrics_log = importlib.import_module("metrics_log")
        log_type = metrics_log.MetricsLogMmap if _REC_MMAP else metrics_log.MetricsLog
        _REC_LOG = log_type(
            _REC_PATH, f"max-serve pid={os.getpid()}",
            os.environ.get("MAX_SERVE_RECORD_METRICS_MODEL", ""), (64 << 20) if _REC_MMAP else 65536,
        )
        logger.info("PROTOTYPE record metrics -> %s (%s)", _REC_PATH, "mmap" if _REC_MMAP else "buffered")
    if _SPLIT_ON:
        _rec_append_split(values)
        return
    if _REC_MMAP:
        # Bytes are in the page cache on return, so a flush is only about surviving an OS
        # crash: msync on the same interval, bounding what is at risk to that window.
        _REC_LOG.append_values(values)
        now = time.monotonic()
        if now - _REC_LAST_FLUSH >= _REC_FLUSH_S:
            _REC_LOG.flush()
            _REC_LAST_FLUSH = now
            _REC_PENDING = False
            _REC_FLUSH_COUNTS[0] += 1
        else:
            _REC_PENDING = True
        return
    _REC_LOG.append_values(values)
    now = time.monotonic()
    if now - _REC_LAST_FLUSH >= _REC_FLUSH_S:   # bound telemetry delay and data at risk
        _REC_LOG.flush()
        _REC_LAST_FLUSH = now
        _REC_PENDING = False
        _REC_FLUSH_COUNTS[0] += 1
    else:
        _REC_PENDING = True


_SPLIT_ON = os.environ.get("MAX_SERVE_APPEND_SPLIT") == "1"
_SPLIT_LAST = [0, 0, False, 0]  # mojo call ns, flush ns, flushed, _rec_append_split total ns
_SPLIT: dict[str, list[int]] = {"call": [], "flush": [], "outside": [], "total_noflush": [], "total_flush": []}


def _rec_append_split(values: tuple) -> None:
    """_rec_append with timers: Mojo call (conversion + encode timed inside Mojo), flush."""
    global _REC_LAST_FLUSH, _REC_PENDING
    pc = time.perf_counter_ns
    t0 = pc()
    _REC_LOG.append_values_timed(values)
    t1 = pc()
    now = time.monotonic()
    flushed = now - _REC_LAST_FLUSH >= _REC_FLUSH_S
    t2 = t3 = 0
    if flushed:
        t2 = pc()
        _REC_LOG.flush()
        t3 = pc()
        _REC_LAST_FLUSH = now
        _REC_PENDING = False
        _REC_FLUSH_COUNTS[0] += 1
    else:
        _REC_PENDING = True
    _SPLIT_LAST[0] = t1 - t0
    _SPLIT_LAST[1] = t3 - t2
    _SPLIT_LAST[2] = flushed
    _SPLIT_LAST[3] = pc() - t0


def _split_record(append_ns: int) -> None:
    _SPLIT["call"].append(_SPLIT_LAST[0])
    if _SPLIT_LAST[2]:
        _SPLIT["flush"].append(_SPLIT_LAST[1])
        _SPLIT["total_flush"].append(append_ns)
    else:
        _SPLIT["total_noflush"].append(append_ns)
    _SPLIT["outside"].append(append_ns - _SPLIT_LAST[3])


def _split_report() -> str:
    def med(d: list[int]) -> str:
        if not d:
            return "-"
        d = sorted(d)
        return f"{d[len(d) // 2] / 1e3:.2f}/{sum(d) / len(d) / 1e3:.2f}"

    conv, enc = _REC_LOG.drain_timings()
    parts = [f"conv {med(conv)}", f"enc {med(enc)}",
             f"boundary {med([c - a - b for c, a, b in zip(_SPLIT['call'], conv, enc)])}",
             f"flush {med(_SPLIT['flush'])} n={len(_SPLIT['flush'])}",
             f"outside {med(_SPLIT['outside'])}",
             f"total_noflush {med(_SPLIT['total_noflush'])}",
             f"total_flush {med(_SPLIT['total_flush'])}"]
    for v in _SPLIT.values():
        v.clear()
    return " | ".join(parts)


def _rec_shutdown() -> None:
    """Worker-loop exit: write out buffered records, or close (trim) the mmap log."""
    if _REC_LOG is None:
        return
    if _REC_MMAP:
        n = _REC_LOG.close()
        logger.info("PROTOTYPE record metrics closed after %d records", n)
    else:
        _rec_idle_flush()
        logger.info("PROTOTYPE record metrics flushed at worker exit")


def _rec_idle_flush() -> None:
    """Called by the worker loop on every NO_PROGRESS iteration (and at loop exit): write out
    records still buffered, so the tail reaches the file when the scheduler goes idle."""
    global _REC_PENDING, _REC_LAST_FLUSH
    if not _REC_PENDING:
        return
    t0 = time.perf_counter_ns()
    _REC_LOG.flush()
    _REC_LAST_FLUSH = time.monotonic()
    _REC_PENDING = False
    _REC_FLUSH_COUNTS[1] += 1
    _REC_FLUSH_COUNTS[2] += time.perf_counter_ns() - t0


_STAGE_ON = os.environ.get("MAX_SERVE_STAGE_TIMING") == "1"
_STAGES: list[list[int]] = [[], [], [], []]
_STAGE_N = 0
_STAGE_NAMES = ("create", "publish", "record_append", "log_line")


def _stage_record(create_ns: int, publish_ns: int, append_ns: int, log_ns: int) -> None:
    global _STAGE_N
    if not _STAGE_ON:
        return
    for lst, v in zip(_STAGES, (create_ns, publish_ns, append_ns, log_ns), strict=True):
        lst.append(v)
    if _SPLIT_ON and _REC_LOG is not None:
        _split_record(append_ns)
    _STAGE_N += 1
    if _STAGE_N % 200 == 0:
        parts = []
        for name, lst in zip(_STAGE_NAMES, _STAGES, strict=True):
            d = sorted(lst)
            parts.append(
                f"{name} med {d[len(d) // 2] / 1e3:.1f} p90 {d[int(len(d) * 0.9)] / 1e3:.1f} "
                f"mean {sum(d) / len(d) / 1e3:.1f} max {d[-1] / 1e3:.1f}"
            )
            lst.clear()
        c = _REC_FLUSH_COUNTS
        parts.append(f"flushes append {c[0]} idle {c[1]} idle_mean {c[2] / max(c[1], 1) / 1e3:.1f}")
        logger.info("PROTOTYPE stages (us) after %d steps: %s", _STAGE_N, " | ".join(parts))
        if _SPLIT_ON and _REC_LOG is not None:
            logger.info("PROTOTYPE append split (us, med/mean): %s", _split_report())
'''
    if fast_values_patch.enabled():
        out = fast_values_patch.fast_block_counts(out)
    if fast_values_patch.snapshot_enabled():
        out = fast_values_patch.kv_snapshot_call(out)
    compile(out, UTILS, "exec")
    open(UTILS, "w").write(out)


def patch_telemetry_worker() -> None:
    """MAX_SERVE_MEASUREMENT_LOG=<dir>: the API process (and, through cross_process_factory,
    the model worker) records measurements into a Dagr stream instead of the queue.

    Only the `yield` inside the branch is replaced: the surrounding `async with
    start_process_consumer(...)` must still run, because that is what spawns the telemetry
    process that serves /metrics. The original yield stays as the else branch.
    """
    src = pristine(TEL_WORKER, os.path.join(HERE, "telemetry_worker.py.orig"))
    tree = astpatch.parse(src)
    consumer = astpatch.find_function(tree, "start_telemetry_consumer")
    branch = astpatch.find_stmt(
        consumer, lambda n: isinstance(n, ast.AsyncWith)
        and astpatch.calls(n, "start_process_consumer"))
    original_yield = astpatch.find_stmt(
        branch, lambda n: isinstance(n, ast.Expr) and isinstance(n.value, ast.Yield))
    indent = astpatch.indent_of(src, original_yield)
    original = astpatch.reindent(astpatch.segment(src, original_yield) + "\n", indent + "    ")
    replacement = astpatch.reindent(f"""_dir = os.environ.get("MAX_SERVE_MEASUREMENT_LOG")  # PROTOTYPE
if _dir:
    import sys

    sys.path.insert(0, os.environ["MAX_SERVE_RECORD_METRICS_MODULE_DIR"])
    from dagr_metric_client import DagrMetricClient

    client = DagrMetricClient(_dir)
    try:
        yield client
    finally:
        client.close()
else:
""", indent) + original
    src = astpatch.replace_stmt(src, original_yield, replacement)
    if "\nimport os\n" not in src:
        src = "import os  # PROTOTYPE\n" + src
    compile(src, TEL_WORKER, "exec")
    open(TEL_WORKER, "w").write(src)


def patch_process_controller() -> None:
    src = pristine(PROC, os.path.join(HERE, "process_controller.py.orig"))
    # MAX_SERVE_TELEMETRY_TIMING=1: time the commit loop (OTel aggregation) in the telemetry
    # process, the same work record mode does via record_publish. One batch per scheduler step.
    # Time the commit loop (OTel aggregation) — the same work record mode does via
    # record_publish. The loop is the `for m in ms:` inside process_telemetry; it is wrapped,
    # not rewritten, so MAX's own body carries through.
    tree = astpatch.parse(src)
    telemetry = astpatch.find_function(tree, "process_telemetry")
    commit_loop = astpatch.find_stmt(
        telemetry, lambda n: isinstance(n, ast.For) and astpatch.calls(n, "commit_fn"))
    start, end = astpatch.lines_of(src, commit_loop)
    body = "".join(src.splitlines(keepends=True)[start:end])
    src = astpatch.replace_lines(src, start, end,
        "                if _TEL_TIMING:  # PROTOTYPE\n"
        "                    _t0 = time.perf_counter_ns()\n"
        + astpatch.reindent(body, "    ")
        + "                    _tel_record(time.perf_counter_ns() - _t0, len(ms))\n"
        "                else:\n"
        + astpatch.reindent(body, "    "))
    src += '''

# ---- PROTOTYPE: telemetry-process commit timing -------------------------------------
_TEL_TIMING = os.environ.get("MAX_SERVE_TELEMETRY_TIMING") == "1"
_TEL_PATH = os.environ.get("MAX_SERVE_TELEMETRY_TIMING_PATH", "/tmp/max_telemetry_timing.txt")
_TEL_BATCHES: list[int] = []
_TEL_MEASUREMENTS: list[int] = []
_TEL_LAST = 0.0


def _tel_record(ns: int, n: int) -> None:
    global _TEL_LAST
    import time as _time

    _TEL_BATCHES.append(ns)
    _TEL_MEASUREMENTS.append(n)
    now = _time.monotonic()
    if now - _TEL_LAST < 10:
        return
    _TEL_LAST = now
    d = sorted(_TEL_BATCHES)
    m = sorted(_TEL_MEASUREMENTS)
    with open(_TEL_PATH, "a") as fh:
        print(
            f"commit: n={len(d)} med {d[len(d) // 2] / 1e3:.2f} mean {sum(d) / len(d) / 1e3:.2f} "
            f"p90 {d[int(len(d) * 0.9)] / 1e3:.2f} max {d[-1] / 1e3:.2f} us | "
            f"measurements/batch med {m[len(m) // 2]} mean {sum(m) / len(m):.1f} max {m[-1]}",
            file=fh,
        )
    _TEL_BATCHES.clear()
    _TEL_MEASUREMENTS.clear()
'''
    tree = astpatch.parse(src)
    worker = astpatch.find_stmt(
        tree, lambda n: isinstance(n, ast.Return) and astpatch.calls(n, "process_telemetry"))
    new = '''    if os.environ.get("MAX_SERVE_RECORD_METRICS"):  # PROTOTYPE design 1
        import sys

        _dir = os.environ["MAX_SERVE_RECORD_METRICS_MODULE_DIR"]
        sys.path.insert(0, _dir)
        from record_publish import tail_and_publish

        threading.Thread(
            target=tail_and_publish,
            args=(os.environ["MAX_SERVE_RECORD_METRICS"], _dir),
            daemon=True,
        ).start()

    if os.environ.get("MAX_SERVE_MEASUREMENT_LOG"):  # PROTOTYPE: generic measurements
        import sys

        _mdir = os.environ["MAX_SERVE_RECORD_METRICS_MODULE_DIR"]
        sys.path.insert(0, _mdir)
        from measurement_publish import tail_measurements

        threading.Thread(
            target=tail_measurements,
            args=(os.environ["MAX_SERVE_MEASUREMENT_LOG"], _mdir),
            daemon=True,
        ).start()

'''
    src = astpatch.insert_before(src, worker, new)
    if "\nimport os\n" not in src:
        src = src.replace("\nimport threading\n", "\nimport os\nimport threading\n", 1)
    if "\nimport time\n" not in src:
        src = src.replace("\nimport threading\n", "\nimport threading\nimport time\n", 1)
    compile(src, PROC, "exec")
    open(PROC, "w").write(src)


def patch_model_worker() -> None:
    """Flush buffered records when the scheduler idles, and on worker-loop exit.

    Both spots are found structurally: the `while True:` scheduler loop inside `run`, and the
    branch that tests SchedulerProgress.NO_PROGRESS inside it.
    """
    src = pristine(WORKER, os.path.join(HERE, "model_worker.py.orig"))
    tree = astpatch.parse(src)
    run = astpatch.find_function(tree, "run", in_class="ModelWorker")
    loop = astpatch.find_stmt(
        run, lambda n: isinstance(n, ast.While) and isinstance(n.test, ast.Constant)
        and n.test.value is True)
    no_progress = astpatch.find_stmt(
        loop, lambda n: astpatch.compares_to(n, "SchedulerProgress.NO_PROGRESS"))
    src = astpatch.insert_at_body_start(
        src, no_progress,
        "                    _rec_idle_flush()  # PROTOTYPE: no-op unless records are buffered\n")
    tree = astpatch.parse(src)
    run = astpatch.find_function(tree, "run", in_class="ModelWorker")
    loop = astpatch.find_stmt(
        run, lambda n: isinstance(n, ast.While) and isinstance(n.test, ast.Constant)
        and n.test.value is True)
    src = astpatch.insert_before(src, loop, """            from max.serve.scheduler.utils import _rec_idle_flush, _rec_shutdown  # PROTOTYPE

            if os.environ.get("MAX_SERVE_RECORD_METRICS"):  # PROTOTYPE: flush / close on exit
                exit_stack.callback(_rec_shutdown)
""")
    compile(src, WORKER, "exec")
    open(WORKER, "w").write(src)


if __name__ == "__main__":
    patch_utils()
    patch_process_controller()
    patch_model_worker()
    patch_telemetry_worker()
    fast_values_patch.patch_inputs()
    fast_values_patch.patch_block_manager()
    fast_values_patch.patch_cache_manager()
    print("patched", UTILS)
    print("patched", PROC)
    print("patched", WORKER, TEL_WORKER)
    print("patched", fast_values_patch.INPUTS, fast_values_patch.BLOCK_MANAGER,
          fast_values_patch.CACHE_MANAGER,
          "fast values:", fast_values_patch.enabled(), "kv snapshot:", fast_values_patch.snapshot_enabled())
