# A telemetry endpoint in Mojo: flare serves /metrics, aggregated incrementally from the Dagr
# measurement streams. No Python, no OpenTelemetry, no prometheus_client — the job MAX's
# telemetry process does today, in one binary.
#
# The registry lives across scrapes: each request ingests only the bytes the streams have added
# since the last one, then renders. flare's Handler contract borrows `self` read-only, so the
# state sits in a heap cell the handler addresses by pointer — the same shape flare's own
# metrics middleware uses.
#
# Build (the project's own env — flare tracks Mojo 1.1 since 2026-09-20):
#   .venv/bin/mojo build -I ../flare_check -I gen/mojo -I . -I third_party \
#       telemetry_server.mojo -o telemetry_server
# Run (from flare's directory: its FFI wrappers are dlopen'd by relative path):
#   MAX_SERVE_MEASUREMENT_LOG=<dir> <proto>/telemetry_server [port]
from std.os import getenv
from std.sys import argv
from std.time import perf_counter_ns

from flare.prelude import *
from flare.runtime.pool import Pool

from prom_core import IncrementalAggregator

comptime CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


struct TelemetryHandler(Copyable, Handler):
    """Serves /metrics and /health from one shared IncrementalAggregator."""

    var state_addr: Int
    var dir: String

    def __init__(out self, var dir: String) raises:
        # flare's own pattern: the cell lives on the heap, the handler carries its address, so
        # `serve` can mutate through the read-only `self` borrow. Leaked at exit by design.
        self.state_addr = Pool[IncrementalAggregator].alloc_move(IncrementalAggregator())
        self.dir = dir^

    def serve(self, req: Request) raises -> Response:
        var path = req.url
        if path == "/health":
            return ok("ok")
        if path != "/metrics":
            return not_found()
        var state = Pool[IncrementalAggregator].get_ptr(self.state_addr)
        var t0 = perf_counter_ns()
        var new_rows = state[].ingest(self.dir)
        var t1 = perf_counter_ns()
        var body = state[].render()
        var t2 = perf_counter_ns()
        print("[telemetry] scrape: +" + String(new_rows) + " measurements, ingest "
              + String((t1 - t0) // 1000) + " us, render " + String((t2 - t1) // 1000)
              + " us, total " + String(state[].rows_total))
        var r = ok(body)
        r.headers.set("Content-Type", CONTENT_TYPE)
        return r^


def main() raises:
    var args = argv()
    var port = UInt16(8300)
    if len(args) > 1:
        port = UInt16(Int(String(args[1])))
    var dir = String(getenv("MAX_SERVE_MEASUREMENT_LOG"))
    if dir.byte_length() == 0:
        print("MAX_SERVE_MEASUREMENT_LOG is not set")
        return

    var handler = TelemetryHandler(dir)
    var srv = HttpServer.bind(SocketAddr.localhost(port))
    print("[telemetry] /metrics on 127.0.0.1:" + String(srv.local_addr().port) + " from " + dir)
    srv.serve(handler^)
