# CLI over prom_core: aggregate the measurement streams into a Prometheus exposition file.
#
# Build: mojo build -I gen/mojo -I . prom_agg.mojo -o prom_agg
# Run:   ./prom_agg <measurement dir> <output file> [until_ns] [--otlp <payload file>]
from std.sys import argv

from otlp import build_request
from prom_core import IncrementalAggregator


def main() raises:
    var args = argv()
    if len(args) < 3:
        print("usage: prom_agg <measurement dir> <output file> [until_ns] [--otlp <file>]")
        return
    var until: UInt64 = UInt64.MAX
    var otlp_path = String("")
    var i = 3
    while i < len(args):
        var a = String(args[i])
        if a == "--otlp" and i + 1 < len(args):
            otlp_path = String(args[i + 1])
            i += 2
        else:
            until = UInt64(Int(a))
            i += 1

    var state = IncrementalAggregator()
    var rows = state.ingest_until(String(args[1]), until)
    with open(String(args[2]), "w") as f:
        f.write(state.render())
    print("aggregated", rows, "measurements ->", String(args[2]))
    if otlp_path.byte_length() > 0:
        var payload = build_request(state.agg, "max-serve", "max.serve")
        with open(otlp_path, "w") as f:
            f.write_bytes(Span(payload))
        print("OTLP ExportMetricsServiceRequest:", len(payload), "bytes ->", otlp_path)
