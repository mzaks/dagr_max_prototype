"""Verify the Mojo OTLP payload against the Prometheus text the same aggregator renders.

Decodes ExportMetricsServiceRequest with the official protobuf, maps each OTel instrument to
its Prometheus family (reusing gen_prom_table's rules), and compares every data point:
sums/gauges by value, histograms by count, sum and cumulative bucket counts.

Usage: .venv/bin/python -I tests/otlp_check.py <payload.bin> <metrics.txt>
"""
import os, re, sys
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
import max._core_mojo  # noqa: F401  (this import shadows the builtin `max`)

def close(a, b):
    return a is not None and abs(a - b) <= 1e-9 * (abs(a) if abs(a) > 1 else 1)

import gen_prom_table as g
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest

payload, text_path = sys.argv[1], sys.argv[2]

def parse_text(path):
    out = {}
    for line in open(path):
        line = line.strip()
        if line.startswith("#") or not line:
            continue
        m = re.match(r"^(\S+?)(\{.*\})?\s+([0-9.eE+-]+|NaN|\+Inf)$", line)
        if not m:
            continue
        labels = m.group(2) or ""
        pairs = tuple(sorted(labels.strip("{}").split(","))) if labels else ()
        out[(m.group(1), pairs)] = float(m.group(3))
    return out

text = parse_text(text_path)
scrape = g.scrape_families(text_path)
raw = []
from max.serve.telemetry.metrics import SERVE_METRICS, HISTOGRAM_SHADOW_SUFFIX
from max.serve.telemetry.common import HISTOGRAM_BUCKETS_BY_METRIC
for name, inst in sorted(SERVE_METRICS.items()):
    if name.endswith(HISTOGRAM_SHADOW_SUFFIX):
        continue
    kind = g.kind_of(inst)
    unit = getattr(inst, "unit", None) or getattr(inst, "_unit", None) or ""
    raw.append((name, kind, unit, [], ""))
suffixes = g.learn_suffixes(raw, scrape)
family = {n: g.family_name(n, u, k, suffixes) for n, k, u, _, _ in raw}
kind_of = {n: k for n, k, _, _, _ in raw}

req = ExportMetricsServiceRequest.FromString(open(payload, "rb").read())
checked = mismatch = 0
def attrs_of(dp):
    return tuple(sorted(f'{a.key}="{a.value.string_value}"' for a in dp.attributes))

for sm in req.resource_metrics[0].scope_metrics:
    for m in sm.metrics:
        fam = family[m.name]
        kind = m.WhichOneof("data")
        if kind in ("sum", "gauge"):
            for dp in (m.sum if kind == "sum" else m.gauge).data_points:
                key = (fam, attrs_of(dp))
                want = text.get(key)
                checked += 1
                if not close(want, dp.as_double):
                    mismatch += 1
                    print(f"  MISMATCH {fam}{attrs_of(dp)}: text {want} vs otlp {dp.as_double}")
            if kind == "sum":
                assert m.sum.aggregation_temporality == 2, m.name
                assert m.sum.is_monotonic == (kind_of[m.name] == "counter"), m.name
        else:
            for dp in m.histogram.data_points:
                a = attrs_of(dp)
                checked += 3
                for suffix, got in (("_count", dp.count), ("_sum", dp.sum)):
                    want = text.get((fam + suffix, a))
                    if not close(want, got):
                        mismatch += 1
                        print(f"  MISMATCH {fam}{suffix}{a}: text {want} vs otlp {got}")
                # cumulative buckets: OTLP carries per-bucket counts + explicit bounds
                cum, running = [], 0
                for c in dp.bucket_counts[:-1]:
                    running += c
                    cum.append(running)
                for bound, c in zip(dp.explicit_bounds, cum, strict=True):
                    want = text.get((fam + "_bucket", tuple(sorted(a + (f'le="{bound}"',)))))
                    if want is None:
                        # the text renders bounds with Python float formatting
                        want = text.get((fam + "_bucket", tuple(sorted(a + (f'le="{bound:g}"',)))))
                    if want is None or want != c:
                        mismatch += 1
                        print(f"  MISMATCH {fam}_bucket le={bound}{a}: text {want} vs otlp {c}")
                        break
                assert m.histogram.aggregation_temporality == 2, m.name
print(f"{len(req.resource_metrics[0].scope_metrics[0].metrics)} metrics, {checked} value checks, "
      f"{mismatch} mismatches")
