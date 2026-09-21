# OTLP/HTTP egress: follow the measurement streams and POST the aggregate on an interval.
#
# The push side of the Mojo telemetry process. It shares the incremental aggregator with the
# /metrics endpoint, so a scrape and an export describe the same registry; only the encoding
# differs (Prometheus text vs an ExportMetricsServiceRequest in protobuf).
#
# Build (flare's pixi env, Mojo 1.0):
#   cd ~/dev/flare_check && pixi run mojo build -I . -I <proto>/gen/mojo -I <proto> \
#       <proto>/otlp_push.mojo -o <proto>/otlp_push
# Run (from flare's directory): ./otlp_push <dir> <endpoint url> [interval_s] [iterations]
from std.sys import argv
from std.time import perf_counter_ns, sleep

from flare.prelude import *
from flare.http import HttpClient

from otlp import build_request
from prom_core import IncrementalAggregator


def main() raises:
    var args = argv()
    if len(args) < 3:
        print("usage: otlp_push <measurement dir> <endpoint> [interval_s] [iterations]")
        return
    var dir = String(args[1])
    var url = String(args[2])
    var interval = Float64(1.0)
    if len(args) > 3:
        interval = Float64(String(args[3]))
    var iterations = -1
    if len(args) > 4:
        iterations = Int(String(args[4]))

    var state = IncrementalAggregator()
    var sent = 0
    while iterations < 0 or sent < iterations:
        var t0 = perf_counter_ns()
        var new_rows = state.ingest(dir)
        var payload = build_request(state.agg, "max-serve", "max.serve")
        var t1 = perf_counter_ns()
        with HttpClient() as c:
            var req = Request(method="POST", url=url, body=payload.copy())
            req.headers.set("Content-Type", "application/x-protobuf")
            var resp = c.send(req^)
            var t2 = perf_counter_ns()
            print("[otlp] +" + String(new_rows) + " measurements, " + String(len(payload))
                  + " bytes, encode " + String((t1 - t0) // 1000) + " us, post "
                  + String((t2 - t1) // 1000) + " us -> " + String(resp.status))
        sent += 1
        if iterations < 0 or sent < iterations:
            sleep(interval)
