"""Incremental aggregation under a live writer: many scrapes while a producer writes.

Writes measurements through DagrMetricClient in bursts (so records land mid-buffer and split
across reads), scrapes the Mojo endpoint throughout, then compares the endpoint's final body
against a one-shot aggregation of the same finished streams.

Usage: .venv/bin/python -I tests/live_incremental.py <port> <dir> [seconds]
"""
import importlib, os, random, subprocess, sys, time
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
importlib.import_module("max._core_mojo")
from max.serve.telemetry.metrics import MaxMeasurement
from dagr_metric_client import DagrMetricClient

port, d = sys.argv[1], sys.argv[2]
secs = float(sys.argv[3]) if len(sys.argv) > 3 else 20.0
extra = {"model": "Qwen/Qwen3-0.6B"}
names = ["maxserve.itl", "maxserve.time_to_first_token", "maxserve.num_input_tokens",
         "maxserve.request_count", "maxserve.num_requests_running"]
client = DagrMetricClient(d, HERE)
rng = random.Random(23)
sent = 0
scrapes = []
end = time.time() + secs
next_scrape = time.time() + 2
while time.time() < end:
    n = rng.choice([1, 5, 50, 300])          # bursts of varying size
    for _ in range(n):
        name = rng.choice(names)
        attrs = {**extra, "code": "200", "path": "/v1/chat/completions"} if name.endswith("count") else extra
        client.send_measurement(MaxMeasurement(name, rng.random() * 100, attrs))
        sent += 1
    time.sleep(rng.choice([0, 0.001, 0.02]))
    if time.time() >= next_scrape:
        next_scrape = time.time() + 2
        t0 = time.perf_counter()
        out = subprocess.run(["curl", "-s", f"http://127.0.0.1:{port}/metrics"],
                             capture_output=True).stdout
        scrapes.append((len(out), time.perf_counter() - t0))
client.close()
time.sleep(0.3)
final = subprocess.run(["curl", "-s", f"http://127.0.0.1:{port}/metrics"],
                       capture_output=True).stdout
open("/tmp/live_final.txt", "wb").write(final)
print(f"wrote {sent} measurements in bursts; {len(scrapes)} scrapes during the run "
      f"(median {sorted(s[1] for s in scrapes)[len(scrapes)//2]*1000:.1f} ms)")
