"""Per-measurement cost on the producing thread: queue client vs Dagr client.
Usage: .venv/bin/python -I tests/bench_client.py <queue|dagr> <dense|sparse> [gap_ms] [n]"""
import importlib, multiprocessing as mp, os, sys, tempfile, time
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
importlib.import_module("max._core_mojo")
from max.serve.telemetry.metrics import MaxMeasurement
from max.serve.telemetry.process_controller import ProcessMetricClient

kind, mode = sys.argv[1], sys.argv[2]
gap = (float(sys.argv[3]) if len(sys.argv) > 3 else 0.0) / 1e3
n = int(sys.argv[4]) if len(sys.argv) > 4 else 20000
extra = {"model": "Qwen/Qwen3-0.6B"}

if kind == "queue":
    # Drain in a thread: with spawn, a child process re-imports __main__ and re-runs this file.
    import threading
    q = mp.get_context("spawn").Queue()
    client = ProcessMetricClient(q)
    threading.Thread(target=lambda: [q.get() for _ in iter(int, 1)], daemon=True).start()
else:
    from dagr_metric_client import DagrMetricClient
    client = DagrMetricClient(tempfile.mkdtemp(prefix="bench_meas_"), HERE)

ms = [MaxMeasurement("maxserve.itl", 1.0 + i, extra) for i in range(n)]
for m in ms[:200]:
    client.send_measurement(m)
pc = time.perf_counter_ns
ts = []
for m in ms:
    if mode == "sparse":
        end = pc() + int(gap * 1e9)
        while pc() < end:
            pass
    t0 = pc()
    client.send_measurement(m)
    ts.append(pc() - t0)
ts.sort()
print(f"{kind:<5} {mode:<6} gap={gap*1e3:.0f}ms n={n}: med {ts[len(ts)//2]/1e3:.2f} "
      f"p90 {ts[int(len(ts)*0.9)]/1e3:.2f} mean {sum(ts)/len(ts)/1e3:.2f} us")
if kind == "dagr":
    client.close()
