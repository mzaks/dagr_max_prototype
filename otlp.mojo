# OTLP metrics egress in Mojo: encode the aggregator's state as an
# ExportMetricsServiceRequest (protobuf) for OTLP/HTTP.
#
# Field numbers are taken from opentelemetry-proto v1 (verified against the generated Python
# descriptors, not from memory):
#   ExportMetricsServiceRequest.resource_metrics = 1
#   ResourceMetrics{resource=1, scope_metrics=2}   ScopeMetrics{scope=1, metrics=2}
#   Metric{name=1, description=2, unit=3, gauge=5, sum=7, histogram=9}
#   Sum{data_points=1, aggregation_temporality=2, is_monotonic=3}   Gauge{data_points=1}
#   Histogram{data_points=1, aggregation_temporality=2}
#   NumberDataPoint{start=2 fixed64, time=3 fixed64, as_double=4, attributes=7}
#   HistogramDataPoint{start=2, time=3, count=4 fixed64, sum=5 double,
#                      bucket_counts=6 packed fixed64, explicit_bounds=7 packed double,
#                      attributes=9}
#   KeyValue{key=1, value=2}   AnyValue{string_value=1}   Resource{attributes=1}
#   InstrumentationScope{name=1, version=2}
#
# Temporality is CUMULATIVE (2): the aggregator never resets, which is what a scrape-shaped
# registry holds. A delta exporter would have to subtract the previously exported state.
from std.memory import bitcast

from prom_core import Aggregator, Series
from prom_table import KIND_COUNTER, KIND_GAUGE, KIND_HISTOGRAM, KIND_SUM_GAUGE

comptime WIRE_VARINT = 0
comptime WIRE_FIXED64 = 1
comptime WIRE_LEN = 2
comptime TEMPORALITY_CUMULATIVE = 2


struct Proto(Movable):
    """A minimal protobuf writer: the wire types OTLP metrics need, nothing else."""

    var buf: List[UInt8]

    def __init__(out self):
        self.buf = List[UInt8]()

    def tag(mut self, field: Int, wire: Int):
        self.varint(UInt64(field << 3 | wire))

    def varint(mut self, value: UInt64):
        var v = value
        while v >= 0x80:
            self.buf.append(UInt8((v & 0x7F) | 0x80))
            v >>= 7
        self.buf.append(UInt8(v))

    def fixed64(mut self, value: UInt64):
        var v = value
        for _ in range(8):
            self.buf.append(UInt8(v & 0xFF))
            v >>= 8

    def bytes(mut self, data: Span[UInt8, _]):
        self.buf.extend(data)

    # -- fields ------------------------------------------------------------------
    def string_field(mut self, field: Int, value: String):
        if value.byte_length() == 0:
            return                              # proto3 omits empty scalars
        self.tag(field, WIRE_LEN)
        self.varint(UInt64(value.byte_length()))
        self.bytes(value.as_bytes())

    def message_field(mut self, field: Int, var sub: Proto):
        self.tag(field, WIRE_LEN)
        self.varint(UInt64(len(sub.buf)))
        self.bytes(Span(sub.buf))

    def double_field(mut self, field: Int, value: Float64):
        self.tag(field, WIRE_FIXED64)
        self.fixed64(bitcast[DType.uint64](value))

    def fixed64_field(mut self, field: Int, value: UInt64):
        self.tag(field, WIRE_FIXED64)
        self.fixed64(value)

    def varint_field(mut self, field: Int, value: UInt64):
        if value == 0:
            return
        self.tag(field, WIRE_VARINT)
        self.varint(value)

    def bool_field(mut self, field: Int, value: Bool):
        if not value:
            return
        self.tag(field, WIRE_VARINT)
        self.varint(1)

    def packed_fixed64(mut self, field: Int, values: List[UInt64]):
        if len(values) == 0:
            return
        self.tag(field, WIRE_LEN)
        self.varint(UInt64(len(values) * 8))
        for v in values:
            self.fixed64(v)

    def packed_double(mut self, field: Int, values: List[Float64]):
        if len(values) == 0:
            return
        self.tag(field, WIRE_LEN)
        self.varint(UInt64(len(values) * 8))
        for v in values:
            self.fixed64(bitcast[DType.uint64](v))


def _key_value(key: String, value: String) raises -> Proto:
    var any = Proto()
    any.string_field(1, value)                  # AnyValue.string_value
    var kv = Proto()
    kv.string_field(1, key)
    kv.message_field(2, any^)
    return kv^


def _attributes_of(labels: String) raises -> List[Proto]:
    """Turn a rendered Prometheus label set back into OTLP KeyValues.

    The aggregator keeps labels as the rendered string because that is what a scrape needs;
    OTLP needs them apart again. Values never contain a quote or comma here (the producer
    writes plain attribute strings), so a simple split is enough.
    """
    var out = List[Proto]()
    var n = labels.byte_length()
    if n < 2:
        return out^
    var inner = String(labels[byte=1 : n - 1])   # strip { }
    for part in inner.split(","):
        var eq = part.find("=")
        if eq < 0:
            continue
        var key = String(part[byte=0:eq])
        var val = String(part[byte=eq + 2 : part.byte_length() - 1])   # ="…"
        out.append(_key_value(key, val))
    return out^


def _number_point(s: Series, labels: String) raises -> Proto:
    var p = Proto()
    p.fixed64_field(2, s.start_ns)
    p.fixed64_field(3, s.last_ns)
    p.double_field(4, s.value)                  # as_double
    var attrs = _attributes_of(labels)
    while len(attrs) > 0:
        p.message_field(7, attrs.pop(0))
    return p^


def _histogram_point(s: Series, labels: String, bounds: List[Float64]) raises -> Proto:
    var p = Proto()
    p.fixed64_field(2, s.start_ns)
    p.fixed64_field(3, s.last_ns)
    p.fixed64_field(4, s.count)
    p.double_field(5, s.value)                  # sum
    # OTLP wants per-bucket counts; the aggregator keeps cumulative ones, plus +Inf = count.
    var counts = List[UInt64]()
    var prev: UInt64 = 0
    for b in range(len(s.buckets)):
        counts.append(s.buckets[b] - prev)
        prev = s.buckets[b]
    counts.append(s.count - prev)
    p.packed_fixed64(6, counts)
    p.packed_double(7, bounds)
    var attrs = _attributes_of(labels)
    while len(attrs) > 0:
        p.message_field(9, attrs.pop(0))
    return p^


def build_request(
    agg: Aggregator, service_name: String, scope_name: String
) raises -> List[UInt8]:
    """Encode every series the aggregator holds as one ExportMetricsServiceRequest."""
    var scope_metrics = Proto()
    var scope = Proto()
    scope.string_field(1, scope_name)
    scope_metrics.message_field(1, scope^)

    for i in range(len(agg.table)):
        ref inst = agg.table[i]
        if len(agg.series[i]) == 0:
            continue
        var metric = Proto()
        metric.string_field(1, inst.otel_name)
        metric.string_field(2, inst.help)
        metric.string_field(3, inst.unit)
        var data = Proto()
        for entry in agg.series[i].items():
            ref s = entry.value
            if inst.kind == KIND_HISTOGRAM:
                data.message_field(1, _histogram_point(s, s.labels, inst.buckets))
            else:
                data.message_field(1, _number_point(s, s.labels))
        if inst.kind == KIND_HISTOGRAM:
            data.varint_field(2, TEMPORALITY_CUMULATIVE)
            metric.message_field(9, data^)
        elif inst.kind == KIND_GAUGE:
            metric.message_field(5, data^)      # Gauge has no temporality
        else:
            data.varint_field(2, TEMPORALITY_CUMULATIVE)
            data.bool_field(3, inst.kind == KIND_COUNTER)   # is_monotonic
            metric.message_field(7, data^)
        scope_metrics.message_field(2, metric^)

    var resource = Proto()
    resource.message_field(1, _key_value("service.name", service_name))
    var resource_metrics = Proto()
    resource_metrics.message_field(1, resource^)
    resource_metrics.message_field(2, scope_metrics^)
    var request = Proto()
    request.message_field(1, resource_metrics^)
    var out = request.buf.copy()   # `buf` cannot be moved out of a live Proto
    return out^
