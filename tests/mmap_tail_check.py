"""record_publish.tail_and_publish on an mmap log, in process: every record arrives, in order."""
import importlib, os, random, sys, tempfile, threading, time
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
os.environ["MAX_SERVE_RECORD_METRICS_MMAP"] = "1"
importlib.import_module("max._core_mojo")
import metrics_log, parity_test, record_publish
from max.serve.scheduler.utils import BatchMetrics

rng = random.Random(4)
vals = [BatchMetrics.compute_values(**parity_test.scenario(rng)) + (False, None, None) for _ in range(3000)]
seen = []
record_publish.make_publisher = lambda: (None, lambda v: seen.append(v) or True)
path = os.path.join(tempfile.mkdtemp(), "t.dagr")
threading.Thread(target=record_publish.tail_and_publish, args=(path, HERE), daemon=True).start()
log = metrics_log.MetricsLogMmap(path, "p", "m", 1 << 16)   # small chunk: growth while tailing
for i, v in enumerate(vals):
    log.append_values_at(v, 10**18 + i)
    if i % 97 == 0:
        time.sleep(0.003)
deadline = time.time() + 5
while len(seen) < len(vals) and time.time() < deadline:
    time.sleep(0.02)
open_count = len(seen)
log.close()
# reference: the same appends through the buffered log, decoded in one go
import numpy as np
ref_path = path + ".ref"
ref = metrics_log.MetricsLog(ref_path, "p", "m", 65536)
for i, v in enumerate(vals):
    ref.append_values_at(v, 10**18 + i)
ref.close()
data = open(ref_path, "rb").read()
arr = np.frombuffer(data, dtype=np.uint8)
expected, _ = metrics_log.decode_records(arr.ctypes.data, len(data), metrics_log.records_start(arr.ctypes.data, len(data)))
assert open_count == len(vals), (open_count, len(vals))
assert seen == expected
print(f"tailer published {open_count}/{len(vals)} records while the mmap log was open; "
      f"every decoded record equals the buffered log's")
