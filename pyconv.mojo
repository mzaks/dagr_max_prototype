# Python <-> Mojo value conversion helpers shared by the prototype extensions.
# Reads use borrowed pointers (no refcount churn); creations return owned PythonObjects.
from std.ffi import c_long, external_call
from std.python import Python, PythonObject
from std.python._cpython import PyObjectPtr
from std.sys import CompilationTarget

comptime BATCH_TYPE_CE: UInt8 = 0
comptime BATCH_TYPE_TG: UInt8 = 1


@always_inline
def is_none(p: PyObjectPtr) -> Bool:
    return p == Python.none()._obj_ptr


@always_inline
def dict_get(d: PyObjectPtr, key: PythonObject) raises -> PyObjectPtr:
    ref cpy = Python().cpython()
    var p = cpy.PyDict_GetItemWithError(d, key._obj_ptr)   # borrowed
    if not p:
        if cpy.PyErr_Occurred():
            raise cpy.unsafe_get_error()
        raise Error("missing field")
    return p


@always_inline
def tuple_item(t: PyObjectPtr, i: Int) raises -> PyObjectPtr:
    ref cpy = Python().cpython()
    var p = cpy.PyTuple_GetItem(t, i)                       # borrowed
    if not p:
        raise cpy.unsafe_get_error()
    return p


@always_inline
def as_int(p: PyObjectPtr) raises -> Int:
    ref cpy = Python().cpython()
    var v = cpy.PyLong_AsSsize_t(p)
    if v == -1 and cpy.PyErr_Occurred():
        raise cpy.unsafe_get_error()
    return Int(v)


@always_inline
def as_f64(p: PyObjectPtr) raises -> Float64:
    ref cpy = Python().cpython()
    var v = cpy.PyFloat_AsDouble(p)
    if v == -1.0 and cpy.PyErr_Occurred():
        raise cpy.unsafe_get_error()
    return Float64(v)


@always_inline
def as_bool(p: PyObjectPtr) raises -> Bool:
    ref cpy = Python().cpython()
    var r = cpy.PyObject_IsTrue(p)
    if r < 0:
        raise cpy.unsafe_get_error()
    return r == 1


def as_opt_f64(p: PyObjectPtr) raises -> Optional[Float64]:
    if is_none(p):
        return None
    return Optional(as_f64(p))


def as_opt_u64(p: PyObjectPtr) raises -> Optional[UInt64]:
    if is_none(p):
        return None
    return Optional(UInt64(as_int(p)))


def as_f64_list(p: PyObjectPtr) raises -> Optional[List[Float64]]:
    # Dagr arrays do not distinguish empty from absent: an empty list is stored absent.
    ref cpy = Python().cpython()
    var n = Int(cpy.PyObject_Length(p))
    if n < 0:
        raise cpy.unsafe_get_error()
    if n == 0:
        return None
    var out = List[Float64](capacity=n)
    for i in range(n):
        var item = cpy.PyList_GetItem(p, i)                 # borrowed
        if not item:
            raise cpy.unsafe_get_error()
        out.append(as_f64(item))
    return Optional(out^)


@always_inline
def py_int(v: Int) -> PythonObject:
    return PythonObject(from_owned=Python().cpython().PyLong_FromSsize_t(v))


@always_inline
def py_f64(v: Float64) -> PythonObject:
    return PythonObject(from_owned=Python().cpython().PyFloat_FromDouble(v))


@always_inline
def py_bool(v: Bool) -> PythonObject:
    return PythonObject(from_owned=Python().cpython().PyBool_FromLong(c_long(1) if v else c_long(0)))


def py_opt_f64(v: Optional[Float64]) -> PythonObject:
    if v:
        return py_f64(v.value())
    return Python.none()


def py_opt_u64(v: Optional[UInt64]) -> PythonObject:
    if v:
        return py_int(Int(v.value()))
    return Python.none()


def as_u64_list(p: PyObjectPtr) raises -> Optional[List[UInt64]]:
    # Dagr arrays do not distinguish empty from absent: an empty list is stored absent.
    ref cpy = Python().cpython()
    var n = Int(cpy.PyObject_Length(p))
    if n < 0:
        raise cpy.unsafe_get_error()
    if n == 0:
        return None
    var out = List[UInt64](capacity=n)
    for i in range(n):
        var item = cpy.PyList_GetItem(p, i)                 # borrowed
        if not item:
            raise cpy.unsafe_get_error()
        out.append(UInt64(as_int(item)))
    return Optional(out^)


def py_u64_list(v: Optional[List[UInt64]]) raises -> PythonObject:
    var out = Python.list()
    if v:
        for x in v.value():
            out.append(py_int(Int(x)))
    return out


def py_f64_list(v: Optional[List[Float64]]) raises -> PythonObject:
    var out = Python.list()
    if v:
        for x in v.value():
            out.append(py_f64(x))
    return out


def py_tuple(var items: List[PythonObject]) raises -> PythonObject:
    ref cpy = Python().cpython()
    var t = cpy.PyTuple_New(len(items))
    if not t:
        raise cpy.unsafe_get_error()
    for i in range(len(items)):
        var ptr = items[i]._obj_ptr
        cpy.Py_IncRef(ptr)                                  # PyTuple_SetItem steals one ref
        _ = cpy.PyTuple_SetItem(t, i, ptr)
    return PythonObject(from_owned=t)


struct ConvCache(Movable):
    """Enum-member identity cache (BatchType.CE / .TG) + one key-object list per sub-record
    (CompletedBatchStats, VisionEncoderMetrics, VideoEncoderMetrics), in record_spec order."""
    var _ce: PythonObject
    var _tg: PythonObject
    var keys: List[List[PythonObject]]

    def __init__(out self, var sub_names: List[List[String]]) raises:
        self._ce = Python.none()
        self._tg = Python.none()
        self.keys = List[List[PythonObject]]()
        for names in sub_names:
            var k = List[PythonObject]()
            for n in names:
                k.append(PythonObject(n))
            self.keys.append(k^)

    def batch_type(mut self, p: PyObjectPtr) raises -> UInt8:
        if self._tg._obj_ptr == p:
            return BATCH_TYPE_TG
        if self._ce._obj_ptr == p:
            return BATCH_TYPE_CE
        var obj = PythonObject(from_borrowed=p)             # first sighting of this member
        var v = BATCH_TYPE_TG if String(py=obj.value) == "TG" else BATCH_TYPE_CE
        if v == BATCH_TYPE_TG:
            self._tg = obj^
        else:
            self._ce = obj^
        return v


@fieldwise_init
struct _TimeSpec(Copyable, Movable):
    var sec: Int
    var nsec: Int


@always_inline
def now_ns() -> UInt64:
    # CLOCK_REALTIME (id 0 on macOS and Linux) — the clock time.time_ns() reads.
    comptime if CompilationTarget.is_linux():
        var ts = _TimeSpec(0, 0)
        _ = external_call["clock_gettime", Int32](Int32(0), Pointer(to=ts))
        return UInt64(ts.sec * 1_000_000_000 + ts.nsec)
    else:
        return UInt64(external_call["clock_gettime_nsec_np", Int64](Int32(0)))


@always_inline
def mono_ns() -> UInt64:
    # Monotonic clock for in-process timing (CLOCK_MONOTONIC_RAW = 4 on Linux, CLOCK_UPTIME_RAW = 8 on macOS).
    comptime if CompilationTarget.is_linux():
        var ts = _TimeSpec(0, 0)
        _ = external_call["clock_gettime", Int32](Int32(4), Pointer(to=ts))
        return UInt64(ts.sec * 1_000_000_000 + ts.nsec)
    else:
        return UInt64(external_call["clock_gettime_nsec_np", Int64](Int32(8)))


def _msync_enabled() -> Bool:
    """DAGR_LOG_MSYNC=0 turns msync off: flushes become no-ops and the page cache is the
    durability boundary. On by default — msync costs ~34 us per flush here."""
    from std.os import getenv

    return String(getenv("DAGR_LOG_MSYNC")) != "0"
