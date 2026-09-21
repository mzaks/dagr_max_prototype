"""Append cost outside the server, dense vs sparse (one append per sleep, like a scheduler step).
Usage: [taskpolicy -c <clamp>] .venv/bin/python -I tests/bench_append.py <dense|sparse|spin> [sleep_ms] [n] [buffered|mmap]"""
import importlib, os, random, sys, tempfile, time
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE); sys.path.insert(0, os.path.join(HERE, "cv"))
importlib.import_module("max._core_mojo")
import metrics_log, parity_test, qos  # noqa: E401
from max.serve.scheduler.utils import BatchMetrics

mode = sys.argv[1]
sleep_s = (float(sys.argv[2]) if len(sys.argv) > 2 else 60.0) / 1e3
n = int(sys.argv[3]) if len(sys.argv) > 3 else 200
dest = sys.argv[4] if len(sys.argv) > 4 else "buffered"
rng = random.Random(5)
vals = [BatchMetrics.compute_values(**parity_test.scenario(rng)) + (False, None, None) for _ in range(n)]
path = os.path.join(tempfile.mkdtemp(), "b.dagr")
log = (metrics_log.MetricsLogMmap(path, "p", "m", 64 << 20) if dest == "mmap"
       else metrics_log.MetricsLog(path, "p", "m", 65536))
for v in vals[:50]:
    log.append_values_timed(v)
log.drain_timings()
pc = time.perf_counter_ns
calls = []
for v in vals:
    if mode == "sparse":
        time.sleep(sleep_s)
    elif mode == "spin":
        end = pc() + int(sleep_s * 1e9)
        while pc() < end:
            pass
    t0 = pc(); log.append_values_timed(v); calls.append(pc() - t0)
conv, enc = log.drain_timings()
med = lambda d: sorted(d)[len(d) // 2] / 1e3  # noqa: E731
print(f"{dest:<8} qos={qos.current():<16} {mode:<6} sleep={sleep_s*1e3:.0f}ms n={n}: conv {med(conv):.2f} enc {med(enc):.2f} call {med(calls):.2f} µs")
