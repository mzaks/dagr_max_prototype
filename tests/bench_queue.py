"""What the multiprocessing queue costs per step in base mode: pickling a batch of measurements
(worker feeder thread) and unpickling it (telemetry process). One variant per process."""
import importlib, pickle, sys, time
importlib.import_module("max._core_mojo")
from max.serve.telemetry.metrics import MaxMeasurement

n = int(sys.argv[1]) if len(sys.argv) > 1 else 24
batch = [MaxMeasurement(f"maxserve.batch_metric_{i}", float(i) * 1.5,
                        {"batch_type": "TG"} if i % 2 else None) for i in range(n)]
blob = pickle.dumps(batch)
pc = time.perf_counter_ns
for _ in range(2000):
    pickle.loads(pickle.dumps(batch))
d, l = [], []
for _ in range(7):
    t0 = pc()
    for _ in range(2000):
        pickle.dumps(batch)
    d.append((pc() - t0) / 2000)
    t0 = pc()
    for _ in range(2000):
        pickle.loads(blob)
    l.append((pc() - t0) / 2000)
d.sort(); l.sort()
print(f"batch of {n} measurements ({len(blob)} B): dumps {d[3]/1e3:.2f} us, loads {l[3]/1e3:.2f} us")
