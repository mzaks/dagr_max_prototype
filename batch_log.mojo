# Python extension: append MAX serve BatchMetrics to a MaxBatchLog DataSink file.
#
# Build: .venv/bin/mojo build --emit shared-lib -I gen/mojo batch_log.mojo -o batch_log.so
#
# Two ways to hand one step to Mojo:
#   log.append(metrics)          read a serve.scheduler.utils.BatchMetrics via its __dict__
#   log.attach(address)          once: address of a BatchStepSlot record (136 bytes) Python owns
#   log.commit()                 per step: read that record in place, append to the sink
# `append_at(metrics, ts_ns)` / `commit_at(ts_ns)` take an explicit timestamp (tests).
# `flush()` writes buffered records to the file; `close()` flushes and closes it.
from std.ffi import external_call
from std.os import abort
from std.python import Python, PythonObject
from std.python._cpython import PyObjectPtr
from std.python.bindings import PythonModuleBuilder
from std.sys import CompilationTarget

from dagr_writer import BufferedFileDestination
from BatchStepSlotSharedBuffer import BatchStepRec
from MaxBatchLogSink import (
    BatchLogHeader,
    BatchStep,
    CompletedBatch,
    MaxBatchLogStreamWriter,
)

comptime BATCH_TYPE_CE: UInt8 = 0
comptime BATCH_TYPE_TG: UInt8 = 1

comptime _K_BATCH_TYPE = 0
comptime _K_BATCH_SIZE = 1
comptime _K_TERMINATED = 2
comptime _K_PENDING = 3
comptime _K_INPUT = 4
comptime _K_CONTEXT = 5
comptime _K_CREATION = 6
comptime _K_EXECUTION = 7
comptime _K_PROMPT_TPUT = 8
comptime _K_GEN_TPUT = 9
comptime _K_PREEMPT = 10
comptime _K_USED_KV = 11
comptime _K_TOTAL_KV = 12
comptime _K_CACHE_HIT = 13
comptime _K_CACHE_MISS = 14
comptime _K_DRAFT_GEN = 15
comptime _K_DRAFT_ACC = 16
comptime _K_OVERLAP = 17
comptime _K_COMPLETED = 18
comptime _C_BATCH_TYPE = 0
comptime _C_BATCH_SIZE = 1
comptime _C_INPUT = 2
comptime _C_CONTEXT = 3
comptime _C_EXECUTION = 4


def _keys(names: List[String]) raises -> List[PythonObject]:
    var out = List[PythonObject]()
    for n in names:
        out.append(PythonObject(n))
    return out^


@always_inline
def _get(d: PyObjectPtr, key: PythonObject) raises -> PyObjectPtr:
    ref cpy = Python().cpython()
    var p = cpy.PyDict_GetItemWithError(d, key._obj_ptr)   # borrowed reference
    if not p:
        if cpy.PyErr_Occurred():
            raise cpy.unsafe_get_error()
        raise Error("BatchMetrics field missing")
    return p


@always_inline
def _as_int(p: PyObjectPtr) raises -> Int:
    ref cpy = Python().cpython()
    var v = cpy.PyLong_AsSsize_t(p)
    if v == -1 and cpy.PyErr_Occurred():
        raise cpy.unsafe_get_error()
    return Int(v)


@always_inline
def _as_f64(p: PyObjectPtr) raises -> Float64:
    ref cpy = Python().cpython()
    var v = cpy.PyFloat_AsDouble(p)
    if v == -1.0 and cpy.PyErr_Occurred():
        raise cpy.unsafe_get_error()
    return Float64(v)


@always_inline
def _as_bool(p: PyObjectPtr) raises -> Bool:
    ref cpy = Python().cpython()
    var r = cpy.PyObject_IsTrue(p)
    if r < 0:
        raise cpy.unsafe_get_error()
    return r == 1


@always_inline
def _opt_u32_nonzero(v: Int) -> Optional[UInt32]:
    if v == 0:
        return None
    return Optional(UInt32(v))


@always_inline
def _opt_u32_nonzero(v: Optional[UInt32]) -> Optional[UInt32]:
    if v and v.value() != 0:
        return v
    return None


@fieldwise_init
struct _TimeSpec(Copyable, Movable):
    var sec: Int
    var nsec: Int


@always_inline
def _now_ns() -> UInt64:
    # CLOCK_REALTIME (id 0 on macOS and Linux) — the clock time.time_ns() reads.
    comptime if CompilationTarget.is_linux():
        var ts = _TimeSpec(0, 0)
        _ = external_call["clock_gettime", Int32](Int32(0), Pointer(to=ts))
        return UInt64(ts.sec * 1_000_000_000 + ts.nsec)
    else:
        return UInt64(external_call["clock_gettime_nsec_np", Int64](Int32(0)))


def _batch_type_by_value(obj: PythonObject) raises -> UInt8:
    if String(py=obj.value) == "TG":
        return BATCH_TYPE_TG
    return BATCH_TYPE_CE


struct BatchLog(Defaultable, Movable, Writable):
    var writer: Optional[MaxBatchLogStreamWriter[BufferedFileDestination]]
    var records: Int
    var _keys: List[PythonObject]
    var _ckeys: List[PythonObject]
    var _ce: PythonObject
    var _tg: PythonObject
    var _slot: Pointer[UInt8, MutUntrackedOrigin]
    var _attached: Bool

    def __init__(out self):
        self.writer = None
        self.records = 0
        self._keys = List[PythonObject]()
        self._ckeys = List[PythonObject]()
        self._ce = Python.none()
        self._tg = Python.none()
        self._slot = Pointer[UInt8, MutUntrackedOrigin](unsafe_from_address=1)
        self._attached = False

    def write_to(self, mut writer: Some[Writer]):
        writer.write("BatchLog(records=", self.records, ")")

    def write_repr_to(self, mut writer: Some[Writer]):
        self.write_to(writer)

    @staticmethod
    def py_init(
        out self: BatchLog, args: PythonObject, kwargs: PythonObject
    ) raises:
        # BatchLog(path, producer, model, max_batch_size, buffer_bytes)
        if len(args) != 5:
            raise Error("BatchLog(path, producer, model, max_batch_size, buffer_bytes)")
        self = BatchLog()
        var header = BatchLogHeader(
            String(py=args[1]), String(py=args[2]), UInt32(Int(py=args[3]))
        )
        self.writer = Optional(
            MaxBatchLogStreamWriter(
                BufferedFileDestination(open(String(py=args[0]), "w"), Int(py=args[4])),
                header^,
            )
        )
        self._keys = _keys([
            "batch_type", "batch_size", "terminated_reqs", "num_pending_reqs",
            "num_input_tokens", "num_context_tokens", "batch_creation_time_s",
            "batch_execution_time_s", "prompt_throughput", "generation_throughput",
            "total_preemption_count", "used_kv_pct", "total_kv_blocks",
            "cache_hit_tokens", "cache_miss_tokens", "draft_tokens_generated",
            "draft_tokens_accepted", "overlap_active", "completed",
        ])
        self._ckeys = _keys([
            "batch_type", "batch_size", "num_input_tokens", "num_context_tokens",
            "execution_time_s",
        ])
        self.writer.value().ensure_framing()

    # ── path 1: read the BatchMetrics object ──────────────────────────────────────
    def _batch_type(mut self, p: PyObjectPtr) raises -> UInt8:
        if self._tg._obj_ptr == p:
            return BATCH_TYPE_TG
        if self._ce._obj_ptr == p:
            return BATCH_TYPE_CE
        var obj = PythonObject(from_borrowed=p)          # first sighting of this member
        var v = _batch_type_by_value(obj)
        if v == BATCH_TYPE_TG:
            self._tg = obj^
        else:
            self._ce = obj^
        return v

    def _from_metrics(mut self, m: PythonObject, ts_ns: UInt64) raises -> BatchStep:
        var dict_obj = m.__dict__
        var d = dict_obj._obj_ptr
        var completed: Optional[CompletedBatch] = None
        var cp = _get(d, self._keys[_K_COMPLETED])
        if not (cp == Python.none()._obj_ptr):
            var cdict_obj = PythonObject(from_borrowed=cp).__dict__
            var cd = cdict_obj._obj_ptr
            completed = Optional(
                CompletedBatch(
                    self._batch_type(_get(cd, self._ckeys[_C_BATCH_TYPE])),
                    UInt32(_as_int(_get(cd, self._ckeys[_C_BATCH_SIZE]))),
                    Optional(UInt32(_as_int(_get(cd, self._ckeys[_C_INPUT])))),
                    Optional(UInt64(_as_int(_get(cd, self._ckeys[_C_CONTEXT])))),
                    _as_f64(_get(cd, self._ckeys[_C_EXECUTION])),
                )
            )
        return BatchStep(
            ts_ns=ts_ns,
            batch_type=self._batch_type(_get(d, self._keys[_K_BATCH_TYPE])),
            batch_size=UInt32(_as_int(_get(d, self._keys[_K_BATCH_SIZE]))),
            terminated_reqs=Optional(UInt32(_as_int(_get(d, self._keys[_K_TERMINATED])))),
            pending_reqs=Optional(UInt32(_as_int(_get(d, self._keys[_K_PENDING])))),
            input_tokens=Optional(UInt32(_as_int(_get(d, self._keys[_K_INPUT])))),
            context_tokens=Optional(UInt64(_as_int(_get(d, self._keys[_K_CONTEXT])))),
            creation_time_s=_as_f64(_get(d, self._keys[_K_CREATION])),
            execution_time_s=Optional(_as_f64(_get(d, self._keys[_K_EXECUTION]))),
            prompt_throughput=Optional(_as_f64(_get(d, self._keys[_K_PROMPT_TPUT]))),
            generation_throughput=Optional(_as_f64(_get(d, self._keys[_K_GEN_TPUT]))),
            preemptions_total=Optional(UInt64(_as_int(_get(d, self._keys[_K_PREEMPT])))),
            used_kv_pct=Optional(_as_f64(_get(d, self._keys[_K_USED_KV]))),
            total_kv_blocks=Optional(UInt32(_as_int(_get(d, self._keys[_K_TOTAL_KV])))),
            cache_hit_tokens=_opt_u32_nonzero(_as_int(_get(d, self._keys[_K_CACHE_HIT]))),
            cache_miss_tokens=_opt_u32_nonzero(_as_int(_get(d, self._keys[_K_CACHE_MISS]))),
            draft_tokens_generated=_opt_u32_nonzero(_as_int(_get(d, self._keys[_K_DRAFT_GEN]))),
            draft_tokens_accepted=_opt_u32_nonzero(_as_int(_get(d, self._keys[_K_DRAFT_ACC]))),
            overlap_active=Optional(_as_bool(_get(d, self._keys[_K_OVERLAP]))),
            completed=completed^,
        )

    @staticmethod
    def append(
        self_ptr: Pointer[Self, MutAnyOrigin], m: PythonObject
    ) raises -> PythonObject:
        ref s = self_ptr[]
        var step = s._from_metrics(m, _now_ns())
        s.writer.value().append_batch_step(step)
        s.records += 1
        return Python.none()

    @staticmethod
    def append_at(
        self_ptr: Pointer[Self, MutAnyOrigin], m: PythonObject, ts_ns: PythonObject
    ) raises -> PythonObject:
        ref s = self_ptr[]
        var step = s._from_metrics(m, UInt64(Int(py=ts_ns)))
        s.writer.value().append_batch_step(step)
        s.records += 1
        return Python.none()

    # ── path 2: read a BatchStepSlot record Python filled in place ────────────────
    @staticmethod
    def attach(
        self_ptr: Pointer[Self, MutAnyOrigin], address: PythonObject
    ) raises -> PythonObject:
        ref s = self_ptr[]
        s._slot = Pointer[UInt8, MutUntrackedOrigin](unsafe_from_address=Int(py=address))
        s._attached = True
        return Python.none()

    def _from_slot(self, ts_ns: UInt64) raises -> BatchStep:
        if not self._attached:
            raise Error("BatchLog.commit() before attach(address)")
        var r = BatchStepRec.wrap(self._slot)
        var completed: Optional[CompletedBatch] = None
        var c = r.completed()
        if c:
            ref cr = c.value()
            completed = Optional(
                CompletedBatch(
                    cr.batch_type(), cr.batch_size(), cr.input_tokens(),
                    cr.context_tokens(), cr.execution_time_s(),
                )
            )
        return BatchStep(
            ts_ns=ts_ns,
            batch_type=r.batch_type(),
            batch_size=r.batch_size(),
            terminated_reqs=r.terminated_reqs(),
            pending_reqs=r.pending_reqs(),
            input_tokens=r.input_tokens(),
            context_tokens=r.context_tokens(),
            creation_time_s=r.creation_time_s(),
            execution_time_s=r.execution_time_s(),
            prompt_throughput=r.prompt_throughput(),
            generation_throughput=r.generation_throughput(),
            preemptions_total=r.preemptions_total(),
            used_kv_pct=r.used_kv_pct(),
            total_kv_blocks=r.total_kv_blocks(),
            cache_hit_tokens=r.cache_hit_tokens(),
            cache_miss_tokens=r.cache_miss_tokens(),
            draft_tokens_generated=r.draft_tokens_generated(),
            draft_tokens_accepted=r.draft_tokens_accepted(),
            overlap_active=r.overlap_active(),
            completed=completed^,
        )

    @staticmethod
    def commit(self_ptr: Pointer[Self, MutAnyOrigin]) raises -> PythonObject:
        ref s = self_ptr[]
        var step = s._from_slot(_now_ns())
        s.writer.value().append_batch_step(step)
        s.records += 1
        return Python.none()

    @staticmethod
    def commit_at(
        self_ptr: Pointer[Self, MutAnyOrigin], ts_ns: PythonObject
    ) raises -> PythonObject:
        ref s = self_ptr[]
        var step = s._from_slot(UInt64(Int(py=ts_ns)))
        s.writer.value().append_batch_step(step)
        s.records += 1
        return Python.none()

    @staticmethod
    def flush(self_ptr: Pointer[Self, MutAnyOrigin]) raises -> PythonObject:
        self_ptr[].writer.value().flush()
        return Python.none()

    @staticmethod
    def close(self_ptr: Pointer[Self, MutAnyOrigin]) raises -> PythonObject:
        ref s = self_ptr[]
        ref w = s.writer.value()
        w.flush()
        w.destination.close()
        return Python.tuple(s.records, w.records_start + w.current_offset())


@export
def PyInit_batch_log() abi("C") -> PythonObject:
    try:
        var m = PythonModuleBuilder("batch_log")
        _ = (
            m.add_type[BatchLog]("BatchLog")
            .def_py_init[BatchLog.py_init]()
            .def_method[BatchLog.append]("append")
            .def_method[BatchLog.append_at]("append_at")
            .def_method[BatchLog.attach]("attach")
            .def_method[BatchLog.commit]("commit")
            .def_method[BatchLog.commit_at]("commit_at")
            .def_method[BatchLog.flush]("flush")
            .def_method[BatchLog.close]("close")
        )
        return m.finalize()
    except e:
        abort(String("failed to create module batch_log: ", e))
