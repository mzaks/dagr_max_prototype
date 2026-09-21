# Generic MetricClient measurements as a Dagr stream, replacing the pickled
# multiprocessing.Queue between the API process / model worker and the telemetry process.
#
# Instrument names and attribute sets are interned by the Python client: the first use of a
# name or attribute set writes a NameRec / AttrSetRec, and every measurement is
# (ts_ns, name_id, attrs_id, value). One `append_rows` call carries a whole batch, so the
# per-measurement cost is one tuple read, not one extension call.
#
# Build: part of metrics_log.so (see metrics_log.mojo).
from std.python import Python, PythonObject
from std.python._cpython import PyObjectPtr

from MaxMeasurementsSink import (
    AttrSetRec,
    MaxMeasurementsStreamWriter,
    MeasurementLogHeader,
    MeasurementRec,
    NameRec,
    _restore_attr_set_rec,
    _restore_measurement_rec,
    _restore_name_rec,
)
from dagr_reader import read_leb
from dagr_writer import leb_length
from mmap_destination import MmapFileDestination
from pyconv import _msync_enabled, as_f64, as_int, py_f64, py_int, py_tuple, tuple_item


struct MeasurementLog(Defaultable, Movable, Writable):
    """Writer side: one Dagr stream per producing process."""

    var writer: Optional[MaxMeasurementsStreamWriter[MmapFileDestination]]
    var records: Int

    def __init__(out self):
        self.writer = None
        self.records = 0

    def write_to(self, mut writer: Some[Writer]):
        writer.write("MeasurementLog(records=", self.records, ")")

    def write_repr_to(self, mut writer: Some[Writer]):
        self.write_to(writer)

    @staticmethod
    def py_init(out self: MeasurementLog, args: PythonObject, kwargs: PythonObject) raises:
        if len(args) != 3:
            raise Error("MeasurementLog(path, producer, pid)")
        self = MeasurementLog()
        var header = MeasurementLogHeader(String(py=args[1]), UInt64(Int(py=args[2])))
        self.writer = Optional(
            MaxMeasurementsStreamWriter(MmapFileDestination(String(py=args[0]), 64 << 20, _msync_enabled()), header^)
        )
        self.writer.value().ensure_framing()

    @staticmethod
    def define_name(
        self_ptr: Pointer[Self, MutAnyOrigin], id: PythonObject, name: PythonObject
    ) raises -> PythonObject:
        ref s = self_ptr[]
        s.writer.value().append_name_rec(NameRec(UInt32(Int(py=id)), String(py=name)))
        s.records += 1
        return Python.none()

    @staticmethod
    def define_attrs(
        self_ptr: Pointer[Self, MutAnyOrigin], id: PythonObject, kv: PythonObject
    ) raises -> PythonObject:
        """kv: flat list [k0, v0, k1, v1, ...]; an empty list stores as absent."""
        ref s = self_ptr[]
        var n = len(kv)
        var items = List[String](capacity=n)
        for i in range(n):
            items.append(String(py=kv[i]))
        var arr = Optional(items^) if n > 0 else Optional[List[String]](None)
        s.writer.value().append_attr_set_rec(AttrSetRec(UInt32(Int(py=id)), arr^))
        s.records += 1
        return Python.none()

    @staticmethod
    def append_rows(
        self_ptr: Pointer[Self, MutAnyOrigin], rows: PythonObject
    ) raises -> PythonObject:
        """rows: a list of (ts_ns, name_id, attrs_id, value) tuples — one call per batch."""
        ref s = self_ptr[]
        ref w = s.writer.value()
        var n = len(rows)
        ref cpy = Python().cpython()
        var lst = rows._obj_ptr
        for i in range(n):
            var item = cpy.PyList_GetItem(lst, i)               # borrowed
            if not item:
                raise cpy.unsafe_get_error()
            w.append_measurement_rec(
                MeasurementRec(
                    UInt64(as_int(tuple_item(item, 0))),
                    UInt32(as_int(tuple_item(item, 1))),
                    UInt32(as_int(tuple_item(item, 2))),
                    as_f64(tuple_item(item, 3)),
                )
            )
        s.records += n
        return py_int(n)

    @staticmethod
    def flush(self_ptr: Pointer[Self, MutAnyOrigin]) raises -> PythonObject:
        self_ptr[].writer.value().flush()
        return Python.none()

    @staticmethod
    def close(self_ptr: Pointer[Self, MutAnyOrigin]) raises -> PythonObject:
        ref w = self_ptr[].writer.value()
        w.flush()
        w.destination.close()
        return py_int(self_ptr[].records)


def decode_measurements(
    address: PythonObject, length: PythonObject, pos: PythonObject
) raises -> PythonObject:
    """Decode every COMPLETE record from `pos`.

    Returns (rows, names, attrs, next_pos): rows are (ts_ns, name_id, attrs_id, value) tuples,
    names are (id, name) and attrs are (id, [k0, v0, ...]) — the reader keeps the tables.
    """
    var buf = _meas_span(address, length)
    var n = len(buf)
    var p = Int(py=pos)
    var rows = Python.list()
    var names = Python.list()
    var attrs = Python.list()
    while p < n:
        var rec_begin = p
        if not _meas_leb_complete(buf, p, n):
            break
        var t = read_leb(buf, p)
        var body = p + t[1]
        if not _meas_leb_complete(buf, body, n):
            break
        var bl = read_leb(buf, body)
        var rec_end = body + bl[1] + Int(bl[0])
        var next = rec_end + leb_length(UInt64(rec_end - rec_begin))
        if next > n:
            break
        var tag = Int(t[0])
        if tag == 0:
            var r = _restore_measurement_rec(buf, body)
            var items = List[PythonObject](capacity=4)
            items.append(py_int(Int(r.ts_ns)))
            items.append(py_int(Int(r.name_id)))
            items.append(py_int(Int(r.attrs_id)))
            items.append(py_f64(r.value))
            rows.append(py_tuple(items^))
        elif tag == 1:
            var nr = _restore_name_rec(buf, body)
            var ni = List[PythonObject](capacity=2)
            ni.append(py_int(Int(nr.id)))
            ni.append(PythonObject(nr.name))
            names.append(py_tuple(ni^))
        elif tag == 2:
            var ar = _restore_attr_set_rec(buf, body)
            var kv = Python.list()
            if ar.kv:
                for sv in ar.kv.value():
                    kv.append(PythonObject(sv))
            var ai = List[PythonObject](capacity=2)
            ai.append(py_int(Int(ar.id)))
            ai.append(kv)
            attrs.append(py_tuple(ai^))
        p = next
    var out = List[PythonObject](capacity=4)
    out.append(rows)
    out.append(names)
    out.append(attrs)
    out.append(py_int(p))
    return py_tuple(out^)


def measurements_start(address: PythonObject, length: PythonObject) raises -> PythonObject:
    """Byte offset of the first record (after framing word + header), or -1 if incomplete."""
    var buf = _meas_span(address, length)
    var n = len(buf)
    if n == 0:
        return py_int(-1)
    var fr = read_leb(buf, 0)
    if fr[1] >= n:
        return py_int(-1)
    var hbl = read_leb(buf, fr[1])
    var start = fr[1] + hbl[1] + Int(hbl[0])
    return py_int(start if start <= n else -1)


def _meas_span(address: PythonObject, length: PythonObject) raises -> Span[UInt8, ImmutAnyOrigin]:
    return Span[UInt8, ImmutAnyOrigin](
        unsafe_ptr=Pointer[UInt8, ImmutAnyOrigin](unsafe_from_address=Int(py=address)),
        length=Int(py=length),
    )


@always_inline
def _meas_leb_complete(buf: Span[UInt8, ImmutAnyOrigin], at: Int, n: Int) -> Bool:
    var i = at
    while i < n:
        if (buf[i] & 0x80) == 0:
            return True
        i += 1
    return False
