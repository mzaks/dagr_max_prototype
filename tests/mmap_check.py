"""MetricsLogMmap: same bytes as MetricsLog, readable mid-stream via <path>.len, growth, close trims."""
import importlib, os, random, struct, sys, tempfile
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
importlib.import_module("max._core_mojo")
import numpy as np
import metrics_log, parity_test
from max.serve.scheduler.utils import BatchMetrics

def decode(data: bytes):
    arr = np.frombuffer(data, dtype=np.uint8)
    start = metrics_log.records_start(arr.ctypes.data, len(data))
    return metrics_log.decode_records(arr.ctypes.data, len(data), start)

rng = random.Random(9)
vals = [BatchMetrics.compute_values(**parity_test.scenario(rng)) + (False, None, None) for _ in range(5000)]
d = tempfile.mkdtemp()
ref = metrics_log.MetricsLog(os.path.join(d, "ref.dagr"), "p", "m", 65536)
for i, v in enumerate(vals):
    ref.append_values_at(v, 10**18 + i)
ref.close()
ref_bytes = open(os.path.join(d, "ref.dagr"), "rb").read()

for chunk in (4096, 64 << 20):
    path = os.path.join(d, f"mm{chunk}.dagr")
    log = metrics_log.MetricsLogMmap(path, "p", "m", chunk)
    reader = open(path, "rb")
    lenf = open(path + ".len", "rb")
    for i, v in enumerate(vals):
        log.append_values_at(v, 10**18 + i)
        if i in (0, 1234, 4999):   # mid-stream read, before close
            committed = struct.unpack("<Q", os.pread(lenf.fileno(), 8, 0))[0]
            # os.pread, not a buffered reader: a BufferedReader keeps bytes it read past the
            # committed length earlier (zeros of the pre-sized file) and serves them after seek.
            data = os.pread(reader.fileno(), committed, 0)
            assert data == ref_bytes[:committed], (chunk, i, committed, data[:16].hex(), ref_bytes[:16].hex())
            recs, end = decode(data)
            assert len(recs) == i + 1 and end == committed, (chunk, i, len(recs), end, committed)
            assert data == ref_bytes[:committed]
    size_open = os.path.getsize(path)
    log.close()
    final = open(path, "rb").read()
    assert final == ref_bytes, (chunk, len(final), len(ref_bytes))
    print(f"chunk={chunk}: {len(vals)} records, mid-stream reads OK, file while open {size_open} B, "
          f"after close {len(final)} B == buffered log")
