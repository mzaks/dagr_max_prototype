# Prometheus aggregation core: read measurement streams, aggregate, render exposition.
# Shared by the prom_agg CLI and the flare-served telemetry endpoint.
#
# This is the work the telemetry process does today in Python + OpenTelemetry: counters summed,
# gauges held at their last value, histograms bucketed, then rendered as Prometheus text. It
# reads the same `meas-<pid>.dagr` streams the DagrMetricClient writes, with no Python involved.
#
# Build: mojo build -I gen/mojo -I . prom_agg.mojo -o prom_agg
# Run:   ./prom_agg <measurement dir> <output file>
from std.collections import Dict

from MaxMeasurementsSink import (
    _restore_attr_set_rec,
    _restore_measurement_rec,
    _restore_name_rec,
)
from dagr_reader import read_leb
from dagr_writer import leb_length
from prom_table import (
    KIND_COUNTER,
    KIND_GAUGE,
    KIND_HISTOGRAM,
    KIND_SUM_GAUGE,
    Instrument,
    instruments,
)


struct Series(Copyable, Movable):
    """One (family, label set) time series."""

    var labels: String          # rendered once: '' or '{k="v",k2="v2"}'
    var value: Float64          # counter total / gauge last value / histogram sum
    var count: UInt64           # histogram observation count
    var buckets: List[UInt64]   # histogram cumulative-bucket counts (+Inf implied by count)
    var start_ns: UInt64        # first observation (OTLP start_time_unix_nano)
    var last_ns: UInt64         # most recent observation (OTLP time_unix_nano)

    def __init__(out self, var labels: String, n_buckets: Int):
        self.labels = labels^
        self.value = 0.0
        self.count = 0
        self.start_ns = 0
        self.last_ns = 0
        self.buckets = List[UInt64]()
        for _ in range(n_buckets):
            self.buckets.append(0)


def _read_file(path: String) raises -> List[UInt8]:
    with open(path, "r") as f:
        return f.read_bytes()


def _committed_length(dir: String, name: String) raises -> Int:
    """The writer publishes how many bytes are whole records in <file>.len."""
    var raw = _read_file(dir + "/" + name + ".len")
    if len(raw) < 8:
        return 0
    var v: UInt64 = 0
    for i in range(8):
        v |= UInt64(Int(raw[i])) << UInt64(8 * i)
    return Int(v)


def _records_start(buf: Span[UInt8, ImmutAnyOrigin], n: Int) raises -> Int:
    if n == 0:
        return -1
    var fr = read_leb(buf, 0)
    if fr[1] >= n:
        return -1
    var hbl = read_leb(buf, fr[1])
    var start = fr[1] + hbl[1] + Int(hbl[0])
    return start if start <= n else -1


def _escape(s: String) -> String:
    """Prometheus label-value escaping: backslash, quote, newline."""
    var out = String()
    for ch in s.codepoint_slices():
        if ch == "\\":
            out += "\\\\"
        elif ch == '"':
            out += '\\"'
        elif ch == "\n":
            out += "\\n"
        else:
            out += ch
    return out^


def _fmt(v: Float64) -> String:
    """Render like Python's float repr for the values Prometheus carries."""
    if v == Float64(Int(v)) and abs(v) < 1e15:
        return String(Int(v)) + ".0"
    return String(v)


struct Aggregator(Movable):
    var table: List[Instrument]
    var by_name: Dict[String, Int]          # OTel instrument name -> table index
    var series: List[Dict[String, Series]]  # per instrument, keyed by rendered labels

    def __init__(out self) raises:
        self.table = instruments()
        self.by_name = Dict[String, Int]()
        self.series = List[Dict[String, Series]]()
        for i in range(len(self.table)):
            self.by_name[self.table[i].otel_name] = i
            self.series.append(Dict[String, Series]())

    def observe(mut self, name: String, labels: String, value: Float64,
                ts_ns: UInt64 = 0) raises:
        var idx = self.by_name.get(name)
        if not idx:
            return                          # an instrument this build does not know
        var i = idx.value()
        ref inst = self.table[i]
        if labels not in self.series[i]:
            self.series[i][labels] = Series(labels, len(inst.buckets))
        ref s = self.series[i][labels]
        if s.start_ns == 0:
            s.start_ns = ts_ns
        s.last_ns = ts_ns
        if inst.kind == KIND_COUNTER or inst.kind == KIND_SUM_GAUGE:
            s.value += value
            s.count += 1
        elif inst.kind == KIND_GAUGE:
            s.value = value
            s.count += 1
        else:
            s.value += value
            s.count += 1
            for b in range(len(inst.buckets)):
                if value <= inst.buckets[b]:
                    s.buckets[b] += 1

    def render(self) raises -> String:
        var out = String()   # (capacity hint differs between Mojo 1.0 and 1.1)
        for i in range(len(self.table)):
            ref inst = self.table[i]
            if len(self.series[i]) == 0:
                continue                    # never observed: MAX does not export it either
            # `family` already carries the counter's _total suffix (gen_prom_table).
            out += "# HELP " + inst.family + " " + inst.help + "\n# TYPE " + inst.family
            if inst.kind == KIND_COUNTER:
                out += " counter\n"
            elif inst.kind == KIND_HISTOGRAM:
                out += " histogram\n"
            else:
                out += " gauge\n"
            for entry in self.series[i].items():
                ref s = entry.value
                if inst.kind == KIND_HISTOGRAM:
                    for b in range(len(inst.buckets)):
                        out += (inst.family + "_bucket" + _with_le(s.labels, _fmt(inst.buckets[b]))
                                + " " + _fmt(Float64(Int(s.buckets[b]))) + "\n")
                    out += (inst.family + "_bucket" + _with_le(s.labels, "+Inf") + " "
                            + _fmt(Float64(Int(s.count))) + "\n")
                    out += inst.family + "_count" + s.labels + " " + _fmt(Float64(Int(s.count))) + "\n"
                    out += inst.family + "_sum" + s.labels + " " + _fmt(s.value) + "\n"
                else:
                    out += inst.family + s.labels + " " + _fmt(s.value) + "\n"
        return out^


def _with_le(labels: String, le: String) -> String:
    """Insert le="…" into a rendered label set (Prometheus puts it last)."""
    var n = labels.byte_length()
    if n == 0:
        return '{le="' + le + '"}'
    return String(labels[byte=0:n - 1]) + ',le="' + le + '"}'



def aggregate_dir(dir: String, until: UInt64) raises -> Tuple[String, Int]:
    """Aggregate every meas-*.dagr stream in `dir` (ignoring rows after `until`) and render."""
    from std.os import listdir

    var agg = Aggregator()
    var total_rows = 0
    for entry in listdir(dir):
        if not (entry.startswith("meas-") and entry.endswith(".dagr")):
            continue
        var committed = _committed_length(dir, entry)
        if committed <= 0:
            continue
        var raw = _read_file(dir + "/" + entry)
        var buf = Span[UInt8, ImmutAnyOrigin](
            unsafe_ptr=Pointer[UInt8, ImmutAnyOrigin](
                unsafe_from_address=Int(raw.unsafe_ptr())
            ),
            length=min(committed, len(raw)),
        )
        var n = len(buf)
        var names = Dict[Int, String]()
        var attrs = Dict[Int, String]()
        var p = _records_start(buf, n)
        if p < 0:
            continue
        while p < n:
            var rec_begin = p
            var t = read_leb(buf, p)
            var body = p + t[1]
            var bl = read_leb(buf, body)
            var rec_end = body + bl[1] + Int(bl[0])
            var next = rec_end + leb_length(UInt64(rec_end - rec_begin))
            if next > n:
                break
            var tag = Int(t[0])
            if tag == 0:
                var r = _restore_measurement_rec(buf, body)
                var nm = names.get(Int(r.name_id))
                if nm and r.ts_ns <= until:
                    var lb = attrs.get(Int(r.attrs_id))
                    agg.observe(nm.value(), lb.value() if lb else String(""), r.value, r.ts_ns)
                    total_rows += 1
            elif tag == 1:
                var nr = _restore_name_rec(buf, body)
                names[Int(nr.id)] = nr.name
            else:
                var ar = _restore_attr_set_rec(buf, body)
                var lb = String()
                if ar.kv:
                    ref kv = ar.kv.value()
                    var i = 0
                    while i + 1 < len(kv):
                        lb += ("," if lb.byte_length() > 0 else "{") + kv[i] + '="' + _escape(kv[i + 1]) + '"'
                        i += 2
                    if lb.byte_length() > 0:
                        lb += "}"
                attrs[Int(ar.id)] = lb^
            p = next
    return (agg.render(), total_rows)


struct StreamState(Movable):
    """What we remember about one producer's stream between scrapes."""

    var offset: Int              # bytes of the file already consumed
    var pending: List[UInt8]     # trailing partial record, carried to the next read
    var started: Bool            # framing word + header consumed
    var names: Dict[Int, String]
    var attrs: Dict[Int, String]

    def __init__(out self):
        self.offset = 0
        self.pending = List[UInt8]()
        self.started = False
        self.names = Dict[Int, String]()
        self.attrs = Dict[Int, String]()


struct IncrementalAggregator(Movable):
    """Aggregate across scrapes: each ingest reads only the bytes a stream has added.

    A pull-based exporter is naturally incremental — the registry is the state, and the cost of
    a scrape is the new measurements plus rendering, not the whole history.
    """

    var agg: Aggregator
    var streams: Dict[String, StreamState]
    var rows_total: Int

    def __init__(out self) raises:
        self.agg = Aggregator()
        self.streams = Dict[String, StreamState]()
        self.rows_total = 0

    def ingest(mut self, dir: String) raises -> Int:
        """Read what is new in every stream; returns the number of measurements ingested."""
        return self.ingest_until(dir, UInt64.MAX)

    def ingest_until(mut self, dir: String, until: UInt64) raises -> Int:
        """`ingest`, ignoring measurements newer than `until` (for comparing against a scrape
        taken at a known instant)."""
        from std.os import listdir
        from std.os.path import isdir

        var new_rows = 0
        if not isdir(dir):      # the producers create it when the first measurement lands
            return 0
        for entry in listdir(dir):
            if not (entry.startswith("meas-") and entry.endswith(".dagr")):
                continue
            if entry not in self.streams:
                self.streams[entry] = StreamState()
            ref st = self.streams[entry]
            var committed = _committed_length(dir, entry)
            if committed <= st.offset:
                continue
            var fresh = _read_range(dir + "/" + entry, st.offset, committed - st.offset)
            st.offset = committed
            var data = st.pending.copy()   # 1.0 does not allow moving out of a Dict entry
            data.extend(fresh^)
            st.pending.clear()
            var buf = Span[UInt8, ImmutAnyOrigin](
                unsafe_ptr=Pointer[UInt8, ImmutAnyOrigin](
                    unsafe_from_address=Int(data.unsafe_ptr())
                ),
                length=len(data),
            )
            var n = len(buf)
            var p = 0
            if not st.started:
                p = _records_start(buf, n)
                if p < 0:
                    st.pending = data.copy()    # header still incomplete
                    continue
                st.started = True
            while p < n:
                var rec_begin = p
                if not _leb_complete(buf, p, n):
                    break
                var t = read_leb(buf, p)
                var body = p + t[1]
                if not _leb_complete(buf, body, n):
                    break
                var bl = read_leb(buf, body)
                var rec_end = body + bl[1] + Int(bl[0])
                var next = rec_end + leb_length(UInt64(rec_end - rec_begin))
                if next > n:
                    break
                var tag = Int(t[0])
                if tag == 0:
                    var r = _restore_measurement_rec(buf, body)
                    var nm = st.names.get(Int(r.name_id))
                    if nm and r.ts_ns <= until:
                        var lb = st.attrs.get(Int(r.attrs_id))
                        self.agg.observe(nm.value(), lb.value() if lb else String(""),
                                         r.value, r.ts_ns)
                        new_rows += 1
                elif tag == 1:
                    var nr = _restore_name_rec(buf, body)
                    st.names[Int(nr.id)] = nr.name
                else:
                    var ar = _restore_attr_set_rec(buf, body)
                    st.attrs[Int(ar.id)] = _labels_of(ar.kv)
                p = next
            # keep the trailing partial record for the next ingest
            var leftover = List[UInt8]()
            for i in range(p, n):
                leftover.append(buf[i])
            st.pending = leftover^
        self.rows_total += new_rows
        return new_rows

    def render(self) raises -> String:
        return self.agg.render()


def _labels_of(kv: Optional[List[String]]) -> String:
    var lb = String()
    if kv:
        ref items = kv.value()
        var i = 0
        while i + 1 < len(items):
            lb += ("," if lb.byte_length() > 0 else "{") + items[i] + '="' + _escape(items[i + 1]) + '"'
            i += 2
        if lb.byte_length() > 0:
            lb += "}"
    return lb^


def _read_range(path: String, offset: Int, count: Int) raises -> List[UInt8]:
    with open(path, "r") as f:
        _ = f.seek(offset)
        return f.read_bytes(count)


@always_inline
def _leb_complete(buf: Span[UInt8, ImmutAnyOrigin], at: Int, n: Int) -> Bool:
    var i = at
    while i < n:
        if (buf[i] & 0x80) == 0:
            return True
        i += 1
    return False
