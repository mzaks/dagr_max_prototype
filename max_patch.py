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
  PATCH_CV_TIMING=1: per-section timers inside compute_values
  serve/pipelines/model_worker.py
    - every NO_PROGRESS iteration (and worker-loop exit) flushes records still buffered.

Run: python3 max_patch.py
"""

import ast
import os
import shutil

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

    # 3) log_metrics: record mode + stage timing
    old = """        metrics = BatchMetrics.create(
            sch_config=sch_config,
            inputs=inputs,
            kv_cache=kv_cache,"""
    assert out.count(old) == 1
    out = out.replace(old, """        _t0 = time.perf_counter_ns()  # PROTOTYPE
        if _REC_PATH:  # PROTOTYPE design 1: record instead of object + publish
            values = BatchMetrics.compute_values(
                sch_config=sch_config,
                inputs=inputs,
                kv_cache=kv_cache,
                batch_creation_time_s=batch_creation_time_s,
                batch_execution_time_s=batch_execution_time_s,
                num_pending_reqs=num_pending_reqs,
                num_terminated_reqs=num_terminated_reqs,
                total_preemption_count=total_preemption_count,
                batch_spec_decode_metrics=batch_spec_decode_metrics,
                batch_vision_metrics=batch_vision_metrics,
                batch_video_metrics=batch_video_metrics,
                overlap_active=overlap_active,
                completed_batch_stats=completed_batch_stats,
            )
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

        metrics = BatchMetrics.create(
            sch_config=sch_config,
            inputs=inputs,
            kv_cache=kv_cache,""", 1)
    old = """        with METRICS.transaction():
            metrics.publish_metrics(defer_execution_metrics=overlap_active)
            if completed_batch_stats is not None:
                publish_completed_batch_metrics(
                    completed_batch_stats, num_terminated_reqs
                )

        # Only periodically log batch info to the console to avoid log spam.
        now = time.monotonic()
        time_since_last_log = now - self.time_of_last_log
        if self.log_interval_s < time_since_last_log:
            # Reset the time of the last log.
            self.time_of_last_log = now
            logger.info(metrics.pretty_format(), extra=metrics.to_log_extra())
"""
    assert out.count(old) == 1
    out = out.replace(old, """        _t1 = time.perf_counter_ns()  # PROTOTYPE
        with METRICS.transaction():
            metrics.publish_metrics(defer_execution_metrics=overlap_active)
            if completed_batch_stats is not None:
                publish_completed_batch_metrics(
                    completed_batch_stats, num_terminated_reqs
                )
        _t2 = time.perf_counter_ns()  # PROTOTYPE

        # Only periodically log batch info to the console to avoid log spam.
        now = time.monotonic()
        time_since_last_log = now - self.time_of_last_log
        if self.log_interval_s < time_since_last_log:
            # Reset the time of the last log.
            self.time_of_last_log = now
            logger.info(metrics.pretty_format(), extra=metrics.to_log_extra())
        _stage_record(_t1 - _t0, _t2 - _t1, 0, time.perf_counter_ns() - _t2)  # PROTOTYPE
""", 1)
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
    if _REC_MMAP:  # bytes are in the page cache on return: nothing to flush
        _REC_LOG.append_values(values)
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
    flushed = not _REC_MMAP and now - _REC_LAST_FLUSH >= _REC_FLUSH_S
    t2 = t3 = 0
    if flushed:
        t2 = pc()
        _REC_LOG.flush()
        t3 = pc()
        _REC_LAST_FLUSH = now
        _REC_PENDING = False
        _REC_FLUSH_COUNTS[0] += 1
    elif not _REC_MMAP:
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
    if os.environ.get("PATCH_CV_TIMING") == "1":
        out = add_cv_timing(out)
    compile(out, UTILS, "exec")
    open(UTILS, "w").write(out)


CV_SECTIONS = (
    # (anchor the checkpoint is inserted before, name of the section that ends there)
    ("        total_kv_blocks = 0\n", "throughput"),
    ("        if kv_cache is not None:\n            # TODO SERVOPT-939", "zero_init_dp"),
    ("            total_kv_blocks = sum(bc.total for bc in block_counts)\n", "block_count_calls"),
    ("            host_block_counts = [\n", "device_sums"),
    ("            total_host_kv_blocks = sum(bc.total for bc in host_block_counts)\n", "host_block_count_calls"),
    ("            metrics_agg = kv_cache.get_metrics_aggregated()\n", "host_sum"),
    ("\n            if total_host_kv_blocks > 0:\n", "get_metrics_aggregated"),
    ("            disk_block_counts = [\n", "agg_attrs"),
    ("            # dKV latency metrics: sum across replicas then average.\n", "disk_block_counts"),
    ("            kv_cache.reset_metrics()\n", "agg_props"),
    ("        # Capture per-request KV cache hit rates", "reset_metrics"),
    ("        draft_tokens_generated = 0\n", "ce_scan"),
    ("        return (  # PROTOTYPE: values in record_spec order\n", "spec"),
)


def add_cv_timing(out: str) -> str:
    start = out.index("    def compute_values(")
    end = out.index("        return (  # PROTOTYPE: values in record_spec order\n") + 200
    body = out[start:end]
    first = "        num_input_tokens = inputs.input_tokens\n"
    body = body.replace(first, "        _pc = time.perf_counter_ns; _cv = [_pc()]  # PROTOTYPE cv timing\n" + first, 1)
    for anchor, name in CV_SECTIONS:
        # match whole lines only: a fast-path patch may re-indent the same statement
        key = anchor if anchor.startswith("\n") else "\n" + anchor
        if body.count(key) != 1:  # removed by a fast-path patch: merges into the next section
            continue
        line = key[1:]
        indent = line[: len(line) - len(line.lstrip(" "))]
        body = body.replace(key, f"\n{indent}_cv.append(_pc())  # {name}" + key, 1)
    body = body.replace(
        "        return (  # PROTOTYPE: values in record_spec order\n",
        "        _cv_record(_cv)\n        return (  # PROTOTYPE: values in record_spec order\n", 1)
    out = out[:start] + body + out[end:]
    names = [n for _, n in CV_SECTIONS if f"_cv.append(_pc())  # {n}\n" in body]
    out += f'''

_CV_NAMES = {names!r}
_CV_SAMPLES: list[list[int]] = []


def _cv_record(cv: list[int]) -> None:
    _CV_SAMPLES.append([b - a for a, b in zip(cv, cv[1:])])
    if len(_CV_SAMPLES) == 200:
        cols = list(zip(*_CV_SAMPLES))
        parts = []
        for name, col in zip(_CV_NAMES, cols):
            d = sorted(col)
            parts.append(f"{{name}} {{d[len(d) // 2] / 1e3:.2f}}/{{sum(d) / len(d) / 1e3:.2f}}")
        logger.info("PROTOTYPE compute_values sections (us, med/mean, n=%d): %s", len(cols[0]), " | ".join(parts))
        _CV_SAMPLES.clear()
'''
    return out


def patch_telemetry_worker() -> None:
    """MAX_SERVE_MEASUREMENT_LOG=<dir>: the API process (and, through cross_process_factory,
    the model worker) records measurements into a Dagr stream instead of the queue."""
    src = pristine(TEL_WORKER, os.path.join(HERE, "telemetry_worker.py.orig"))
    old = """    elif method == MetricRecordingMethod.PROCESS:
        async with start_process_consumer(settings) as controller:
            yield controller.Client()
"""
    assert src.count(old) == 1
    src = src.replace(old, """    elif method == MetricRecordingMethod.PROCESS:
        async with start_process_consumer(settings) as controller:
            _dir = os.environ.get("MAX_SERVE_MEASUREMENT_LOG")  # PROTOTYPE
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
                yield controller.Client()
""", 1)
    if "\nimport os\n" not in src:
        src = "import os  # PROTOTYPE\n" + src
    compile(src, TEL_WORKER, "exec")
    open(TEL_WORKER, "w").write(src)


def patch_process_controller() -> None:
    src = pristine(PROC, os.path.join(HERE, "process_controller.py.orig"))
    # MAX_SERVE_TELEMETRY_TIMING=1: time the commit loop (OTel aggregation) in the telemetry
    # process, the same work record mode does via record_publish. One batch per scheduler step.
    old_loop = """            try:
                for m in ms:
                    commit_fn(m)
            except:
"""
    assert src.count(old_loop) == 1
    src = src.replace(old_loop, """            try:
                if _TEL_TIMING:  # PROTOTYPE
                    _t0 = time.perf_counter_ns()
                    for m in ms:
                        commit_fn(m)
                    _tel_record(time.perf_counter_ns() - _t0, len(ms))
                else:
                    for m in ms:
                        commit_fn(m)
            except:
""", 1)
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
    old = "    return process_telemetry(metrics_q, alive, commit_fn)\n"
    assert src.count(old) == 1
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

    return process_telemetry(metrics_q, alive, commit_fn)
'''
    src = src.replace(old, new, 1)
    if "\nimport os\n" not in src:
        src = src.replace("\nimport threading\n", "\nimport os\nimport threading\n", 1)
    if "\nimport time\n" not in src:
        src = src.replace("\nimport threading\n", "\nimport threading\nimport time\n", 1)
    compile(src, PROC, "exec")
    open(PROC, "w").write(src)


def patch_model_worker() -> None:
    src = pristine(WORKER, os.path.join(HERE, "model_worker.py.orig"))
    old = """            count_no_progress = 0
            while True:
"""
    assert src.count(old) == 1
    src = src.replace(old, """            from max.serve.scheduler.utils import _rec_idle_flush, _rec_shutdown  # PROTOTYPE

            if os.environ.get("MAX_SERVE_RECORD_METRICS"):  # PROTOTYPE: flush / close on exit
                exit_stack.callback(_rec_shutdown)
            count_no_progress = 0
            while True:
""", 1)
    old = """                if progress == SchedulerProgress.NO_PROGRESS:
                    await sleep_with_backoff(count_no_progress)
"""
    assert src.count(old) == 1
    src = src.replace(old, """                if progress == SchedulerProgress.NO_PROGRESS:
                    _rec_idle_flush()  # PROTOTYPE: no-op unless records are buffered
                    await sleep_with_backoff(count_no_progress)
""", 1)
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
