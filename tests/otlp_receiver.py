"""A minimal OTLP/HTTP receiver: decode ExportMetricsServiceRequest POSTs and log a summary.

Usage: .venv/bin/python -I tests/otlp_receiver.py <port> <out.bin>   (writes the last payload)
"""
import http.server, os, sys
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
import max._core_mojo  # noqa: F401
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest

port, out = int(sys.argv[1]), sys.argv[2]

class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers["Content-Length"]))
        open(out, "wb").write(body)
        try:
            req = ExportMetricsServiceRequest.FromString(body)
            sm = req.resource_metrics[0].scope_metrics[0]
            points = sum(len(getattr(m, m.WhichOneof("data")).data_points) for m in sm.metrics)
            print(f"received {len(body)} B, content-type={self.headers['Content-Type']}, "
                  f"{len(sm.metrics)} metrics, {points} data points", flush=True)
        except Exception as e:  # noqa: BLE001
            print("DECODE FAILED:", e, flush=True)
        self.send_response(200)
        self.send_header("Content-Type", "application/x-protobuf")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *a):
        pass

http.server.HTTPServer(("127.0.0.1", port), Handler).serve_forever()
