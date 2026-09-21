"""DagrMetricClient -> measurement log -> tail_measurements: every measurement, same values."""
import importlib, os, random, sys, tempfile, threading, time
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
importlib.import_module("max._core_mojo")
from max.serve.telemetry.metrics import MaxMeasurement
import dagr_metric_client, measurement_publish

d = tempfile.mkdtemp(prefix="meas_")
client = dagr_metric_client.DagrMetricClient(d, HERE)
rng = random.Random(17)
extra = {"model": "Qwen/Qwen3-0.6B", "instance": "local"}   # the shared per-process dict
names = ["maxserve.itl", "maxserve.ttft", "maxserve.num_input_tokens", "maxserve.request_count"]
sent = []
for i in range(5000):
    name = rng.choice(names)
    if name == "maxserve.request_count":
        attrs = {**extra, "code": rng.choice(["200", "400"]), "path": "/v1/chat/completions"}
    elif rng.random() < 0.2:
        attrs = None
    else:
        attrs = extra
    m = MaxMeasurement(name, rng.random() * 1000, attrs)
    sent.append((m.instrument_name, m.value, dict(attrs) if attrs else None, m.time_unix_nano))
    if rng.random() < 0.1:
        with client.transaction():
            client.send_measurement(m)
    else:
        client.send_measurement(m)

got = []
class Cap:
    def commit(self):
        pass
orig = MaxMeasurement.commit
MaxMeasurement.commit = lambda self: got.append(
    (self.instrument_name, self.value, dict(self.attributes) if self.attributes else None,
     self.time_unix_nano))
threading.Thread(target=measurement_publish.tail_measurements, args=(d, HERE), daemon=True).start()
client._flush()
client._log.flush()
deadline = time.time() + 15
while len(got) < len(sent) and time.time() < deadline:
    time.sleep(0.05)
MaxMeasurement.commit = orig
assert len(got) == len(sent), (len(got), len(sent))
for i, (a, b) in enumerate(zip(sent, got, strict=True)):
    assert a == b, (i, a, b)
size = os.path.getsize(os.path.join(d, os.listdir(d)[0]))
print(f"{len(sent)} measurements round-tripped identically (name, value, attributes, timestamp); "
      f"{len(client._names)} names / {len(client._attrs)} attribute sets interned")
