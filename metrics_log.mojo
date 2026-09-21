# Python extension for design 1: the MAX worker logs one BatchMetricsRec per scheduler step,
# the telemetry process decodes them back and publishes Prometheus metrics.
#
# Build: python3 gen_record_code.py &&
#        .venv/bin/mojo build --emit shared-lib -I gen/mojo -I . metrics_log.mojo -o metrics_log.so
#
# Worker:     log = metrics_log.MetricsLog(path, producer, model, buffer_bytes)
#             log = metrics_log.MetricsLogMmap(path, producer, model, chunk_bytes)  # + <path>.len
#             log.append_values(values)          values: tuple in record_spec order
#             log.flush() / log.close()
# Telemetry:  metrics_log.records_start(address, length) -> int
#             metrics_log.decode_records(address, length, pos) -> (list[values tuple], next_pos)
#             (address/length of a readable byte buffer holding the stream prefix read so far)
from std.os import abort
from std.python import Python, PythonObject
from std.python.bindings import PythonModuleBuilder

from dagr_reader import read_leb
from dagr_writer import BufferedFileDestination, leb_length
from mmap_destination import MmapFileDestination
from MaxBatchMetricsSink import (
    MaxBatchMetricsStreamWriter,
    MetricsLogHeader,
    _restore_batch_metrics_rec,
)
from metrics_record_gen import N_VALUES, rec_to_values, subrecord_names, values_to_rec
from measurement_log import MeasurementLog, decode_measurements, measurements_start
from pyconv import ConvCache, mono_ns, now_ns, py_int, py_tuple


struct MetricsLog(Defaultable, Movable, Writable):
    var writer: Optional[MaxBatchMetricsStreamWriter[BufferedFileDestination]]
    var cache: Optional[ConvCache]
    var records: Int
    var conv_ns: List[UInt64]   # append_values_timed samples: tuple -> record
    var enc_ns: List[UInt64]    # append_values_timed samples: record -> buffer

    def __init__(out self):
        self.writer = None
        self.cache = None
        self.records = 0
        self.conv_ns = List[UInt64]()
        self.enc_ns = List[UInt64]()

    def write_to(self, mut writer: Some[Writer]):
        writer.write("MetricsLog(records=", self.records, ")")

    def write_repr_to(self, mut writer: Some[Writer]):
        self.write_to(writer)

    @staticmethod
    def py_init(out self: MetricsLog, args: PythonObject, kwargs: PythonObject) raises:
        if len(args) != 4:
            raise Error("MetricsLog(path, producer, model, buffer_bytes)")
        self = MetricsLog()
        var header = MetricsLogHeader(String(py=args[1]), String(py=args[2]))
        self.writer = Optional(
            MaxBatchMetricsStreamWriter(
                BufferedFileDestination(open(String(py=args[0]), "w"), Int(py=args[3])),
                header^,
            )
        )
        self.cache = Optional(ConvCache(subrecord_names()))
        self.writer.value().ensure_framing()

    @staticmethod
    def append_values(self_ptr: Pointer[Self, MutAnyOrigin], values: PythonObject) raises -> PythonObject:
        ref s = self_ptr[]
        var rec = values_to_rec(values, s.cache.value(), now_ns())
        s.writer.value().append_batch_metrics_rec(rec)
        s.records += 1
        return Python.none()

    @staticmethod
    def append_values_timed(self_ptr: Pointer[Self, MutAnyOrigin], values: PythonObject) raises -> PythonObject:
        """Append_values, recording conversion and encode times (read with drain_timings)."""
        ref s = self_ptr[]
        var t0 = mono_ns()
        var rec = values_to_rec(values, s.cache.value(), now_ns())
        var t1 = mono_ns()
        s.writer.value().append_batch_metrics_rec(rec)
        var t2 = mono_ns()
        s.records += 1
        s.conv_ns.append(t1 - t0)
        s.enc_ns.append(t2 - t1)
        return Python.none()

    @staticmethod
    def drain_timings(self_ptr: Pointer[Self, MutAnyOrigin]) raises -> PythonObject:
        """(conv_ns list, enc_ns list) since the last drain."""
        ref s = self_ptr[]
        var conv = Python.list()
        var enc = Python.list()
        for v in s.conv_ns:
            conv.append(py_int(Int(v)))
        for v in s.enc_ns:
            enc.append(py_int(Int(v)))
        s.conv_ns.clear()
        s.enc_ns.clear()
        var items = List[PythonObject]()
        items.append(conv)
        items.append(enc)
        return py_tuple(items^)

    @staticmethod
    def append_values_at(
        self_ptr: Pointer[Self, MutAnyOrigin], values: PythonObject, ts_ns: PythonObject
    ) raises -> PythonObject:
        ref s = self_ptr[]
        var rec = values_to_rec(values, s.cache.value(), UInt64(Int(py=ts_ns)))
        s.writer.value().append_batch_metrics_rec(rec)
        s.records += 1
        return Python.none()

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


struct MetricsLogMmap(Defaultable, Movable, Writable):
    var writer: Optional[MaxBatchMetricsStreamWriter[MmapFileDestination]]
    var cache: Optional[ConvCache]
    var records: Int
    var conv_ns: List[UInt64]   # append_values_timed samples: tuple -> record
    var enc_ns: List[UInt64]    # append_values_timed samples: record -> buffer

    def __init__(out self):
        self.writer = None
        self.cache = None
        self.records = 0
        self.conv_ns = List[UInt64]()
        self.enc_ns = List[UInt64]()

    def write_to(self, mut writer: Some[Writer]):
        writer.write("MetricsLogMmap(records=", self.records, ")")

    def write_repr_to(self, mut writer: Some[Writer]):
        self.write_to(writer)

    @staticmethod
    def py_init(out self: MetricsLogMmap, args: PythonObject, kwargs: PythonObject) raises:
        if len(args) != 4:
            raise Error("MetricsLogMmap(path, producer, model, chunk_bytes)")
        self = MetricsLogMmap()
        var header = MetricsLogHeader(String(py=args[1]), String(py=args[2]))
        self.writer = Optional(
            MaxBatchMetricsStreamWriter(
                MmapFileDestination(String(py=args[0]), Int(py=args[3])),
                header^,
            )
        )
        self.cache = Optional(ConvCache(subrecord_names()))
        self.writer.value().ensure_framing()

    @staticmethod
    def append_values(self_ptr: Pointer[Self, MutAnyOrigin], values: PythonObject) raises -> PythonObject:
        ref s = self_ptr[]
        var rec = values_to_rec(values, s.cache.value(), now_ns())
        s.writer.value().append_batch_metrics_rec(rec)
        s.records += 1
        return Python.none()

    @staticmethod
    def append_values_timed(self_ptr: Pointer[Self, MutAnyOrigin], values: PythonObject) raises -> PythonObject:
        """Append_values, recording conversion and encode times (read with drain_timings)."""
        ref s = self_ptr[]
        var t0 = mono_ns()
        var rec = values_to_rec(values, s.cache.value(), now_ns())
        var t1 = mono_ns()
        s.writer.value().append_batch_metrics_rec(rec)
        var t2 = mono_ns()
        s.records += 1
        s.conv_ns.append(t1 - t0)
        s.enc_ns.append(t2 - t1)
        return Python.none()

    @staticmethod
    def drain_timings(self_ptr: Pointer[Self, MutAnyOrigin]) raises -> PythonObject:
        """(conv_ns list, enc_ns list) since the last drain."""
        ref s = self_ptr[]
        var conv = Python.list()
        var enc = Python.list()
        for v in s.conv_ns:
            conv.append(py_int(Int(v)))
        for v in s.enc_ns:
            enc.append(py_int(Int(v)))
        s.conv_ns.clear()
        s.enc_ns.clear()
        var items = List[PythonObject]()
        items.append(conv)
        items.append(enc)
        return py_tuple(items^)

    @staticmethod
    def append_values_at(
        self_ptr: Pointer[Self, MutAnyOrigin], values: PythonObject, ts_ns: PythonObject
    ) raises -> PythonObject:
        ref s = self_ptr[]
        var rec = values_to_rec(values, s.cache.value(), UInt64(Int(py=ts_ns)))
        s.writer.value().append_batch_metrics_rec(rec)
        s.records += 1
        return Python.none()

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


def _span(address: PythonObject, length: PythonObject) raises -> Span[UInt8, ImmutAnyOrigin]:
    return Span[UInt8, ImmutAnyOrigin](
        unsafe_ptr=Pointer[UInt8, ImmutAnyOrigin](unsafe_from_address=Int(py=address)),
        length=Int(py=length),
    )


def records_start(address: PythonObject, length: PythonObject) raises -> PythonObject:
    """Byte offset of the first record (after framing word + header), or -1 if not yet complete."""
    var buf = _span(address, length)
    var n = len(buf)
    if n == 0:
        return py_int(-1)
    var fr = read_leb(buf, 0)                               # framing word (header bit set)
    if fr[1] >= n:
        return py_int(-1)
    var hbl = read_leb(buf, fr[1])                          # [blockLen][header fields]
    var start = fr[1] + hbl[1] + Int(hbl[0])
    return py_int(start if start <= n else -1)


def decode_records(address: PythonObject, length: PythonObject, pos: PythonObject) raises -> PythonObject:
    """Decode every COMPLETE record from `pos`; a trailing partial record is left for later."""
    var buf = _span(address, length)
    var n = len(buf)
    var p = Int(py=pos)
    var out = Python.list()
    while p < n:
        var rec_begin = p
        # A LEB read at the very end of the buffer may be truncated: only read when the
        # max LEB width (10 bytes) fits, or when the remaining bytes already hold a terminator.
        if not _leb_complete(buf, p, n):
            break
        var t = read_leb(buf, p)
        var body = p + t[1]
        if not _leb_complete(buf, body, n):
            break
        var bl = read_leb(buf, body)
        var rec_end = body + bl[1] + Int(bl[0])
        var next = rec_end + leb_length(UInt64(rec_end - rec_begin))   # trailing RLEB span
        if next > n:
            break
        if Int(t[0]) == 0:
            out.append(rec_to_values(_restore_batch_metrics_rec(buf, body)))
        p = next
    var items = List[PythonObject]()
    items.append(out)
    items.append(py_int(p))
    return py_tuple(items^)


@always_inline
def _leb_complete(buf: Span[UInt8, ImmutAnyOrigin], at: Int, n: Int) -> Bool:
    var i = at
    while i < n:
        if (buf[i] & 0x80) == 0:
            return True
        i += 1
    return False


@export
def PyInit_metrics_log() abi("C") -> PythonObject:
    try:
        var m = PythonModuleBuilder("metrics_log")
        _ = (
            m.add_type[MetricsLog]("MetricsLog")
            .def_py_init[MetricsLog.py_init]()
            .def_method[MetricsLog.append_values]("append_values")
            .def_method[MetricsLog.append_values_timed]("append_values_timed")
            .def_method[MetricsLog.drain_timings]("drain_timings")
            .def_method[MetricsLog.append_values_at]("append_values_at")
            .def_method[MetricsLog.flush]("flush")
            .def_method[MetricsLog.close]("close")
        )
        _ = (
            m.add_type[MetricsLogMmap]("MetricsLogMmap")
            .def_py_init[MetricsLogMmap.py_init]()
            .def_method[MetricsLogMmap.append_values]("append_values")
            .def_method[MetricsLogMmap.append_values_timed]("append_values_timed")
            .def_method[MetricsLogMmap.drain_timings]("drain_timings")
            .def_method[MetricsLogMmap.append_values_at]("append_values_at")
            .def_method[MetricsLogMmap.flush]("flush")
            .def_method[MetricsLogMmap.close]("close")
        )
        _ = (
            m.add_type[MeasurementLog]("MeasurementLog")
            .def_py_init[MeasurementLog.py_init]()
            .def_method[MeasurementLog.define_name]("define_name")
            .def_method[MeasurementLog.define_attrs]("define_attrs")
            .def_method[MeasurementLog.append_rows]("append_rows")
            .def_method[MeasurementLog.flush]("flush")
            .def_method[MeasurementLog.close]("close")
        )
        m.def_function[decode_measurements]("decode_measurements")
        m.def_function[measurements_start]("measurements_start")
        m.def_function[records_start]("records_start")
        m.def_function[decode_records]("decode_records")
        return m.finalize()
    except e:
        abort(String("failed to create module metrics_log: ", e))
