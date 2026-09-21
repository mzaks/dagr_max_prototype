# Dagr × MAX serve batch-log prototype

Log MAX serve's per-step `BatchMetrics` to a Dagr DataSink from Python via a Mojo extension,
and compare ways of handing the data to Mojo.

## Layout

| File | What |
|---|---|
| `schema.py` | `MaxBatchLog` DataSink (packed `BatchStep` per step) + `BatchStepSlot` SharedBuffer (same fields, fixed 136-byte record) |
| `gen/` | `dagr build` output (Mojo sink + SharedBuffer overlay, reflective Python runtime) |
| `batch_log.mojo` | Python extension: `append(metrics)` reads the object; `attach(addr)` + `commit()` reads a SharedBuffer record Python filled |
| `slot_writers.py` | `BatchMetrics` fixture and three Python fillers for the record: `struct`, `numpy`, `dagr_py` |
| `check.py` | all four hand-off paths must log byte-identical records; reflective reader checks every field |
| `bench.py` | per-step cost, one variant per process |
| `results_handoff.txt` | raw results of the 3-run sweep |

## Setup and run

```sh
uv venv -p 3.12 .venv
uv pip install -p .venv/bin/python --prerelease=allow "max[serve]==26.6.0.dev2026082707" \
    "mojo==1.1.0.dev2026082707" msgspec \
    --extra-index-url https://whl.modular.com/nightly/simple/ --index-strategy unsafe-best-match
dagr build                                     # needs Dagr >= 0fa5154 (StreamWriter, BufferedFileDestination)
.venv/bin/mojo build --emit shared-lib -I gen/mojo batch_log.mojo -o batch_log.so
.venv/bin/python -I check.py
for v in append struct_commit numpy_commit dagr_py_commit struct_only numpy_only commit_only jsonl_subset; do
  .venv/bin/python -I bench.py $v 20000
done
```

## Results (M4 Max, 2026-09-17, 20,000 steps, 3 runs, medians)

| Variant | Median | Mean | p99 |
|---|---|---|---|
| `append` — Mojo reads the object | **0.541 µs** | 0.544–0.550 | 0.67–0.75 |
| `struct_commit` — `struct.pack_into` + `commit()` | 0.708 µs | 0.708–0.712 | 0.88–0.92 |
| `numpy_commit` — numpy field views + `commit()` | 1.208 µs | 1.25–1.27 | 1.50–1.63 |
| `dagr_py_commit` — reflective `write_region` + `commit()` | 39.6–40.9 µs | 39.4–40.6 | 46.5–47.3 |
| `struct_only` — Python fill, no Mojo | 0.458 µs | 0.46–0.47 | 0.54–0.63 |
| `numpy_only` — Python fill, no Mojo | 0.96–1.00 µs | 1.00–1.03 | 1.21 |
| `commit_only` — Mojo read + encode + buffered append | 0.250 µs | 0.25 | 0.29–0.33 |
| `jsonl_subset` — msgspec JSONL, same fields | 1.375 µs | 1.87–1.90 | 5.29–5.42 |

Filling the SharedBuffer record from Python (0.458 µs at best) costs more than Mojo reading
the object's attributes (~0.29 µs = `append` − `commit_only`), so the SharedBuffer hand-off is
slower for data that starts as a Python object.

## Design 1: the per-step record replaces `BatchMetrics` + worker-side publishing

| File | What |
|---|---|
| `record_spec.py` | single source for the 55-field `BatchMetricsRec` (+ `CompletedRec`) |
| `gen_record_code.py` → `metrics_record_gen.mojo` | values tuple ↔ record conversion |
| `pyconv.mojo`, `metrics_log.mojo` | `MetricsLog.append_values`, `records_start`, `decode_records` |
| `max_patch.py` | patches the venv MAX: `BatchMetrics.compute_values` / `from_values` (create = both), record mode in `log_metrics`, telemetry-process tailer |
| `record_publish.py` | rebuilds `BatchMetrics` from a record and calls MAX's original publish functions |
| `parity_test.py` | exact measurement parity, in-process |
| `run_server.sh <label> 1 base|record`, `compare_metrics.py` | GPU runs and `/metrics` comparison |

Env: `MAX_SERVE_RECORD_METRICS=<path>`, `MAX_SERVE_RECORD_METRICS_MODULE_DIR=<this dir>`,
`MAX_SERVE_RECORD_METRICS_FLUSH_S` (default 0.05; 1.0 recommended with the idle flush), `MAX_SERVE_STAGE_TIMING=1`.

### Correctness
- `parity_test.py 5000`: 121,560 measurements across 41 instruments identical (name, value,
  attributes, order) between original create+publish and record → file → decode → publish;
  rebuilt `BatchMetrics` == original for every step.
- GPU runs: same batch-metric series in base and record runs; deterministic sums identical at
  0.05 s flush (input tokens, batch sizes, cache misses, terminated reqs); record files match
  each run's Prometheus step counts.

### Worker stage cost (GPU, steady state, medians)
| Mode | create / compute_values | publish | record append | total |
|---|---|---|---|---|
| base (2 runs) | 40.7–41.8 µs | 31.7–33.3 µs | – | ~73–75 µs |
| record, flush 0.05 s (2 runs) | 28.5–29.4 µs | 0.2 µs | 37–44 µs | ~67–73 µs |
| record, flush 1 s (2 runs) | 28.8–30.1 µs | 0.2 µs | 13.7–14.7 µs | ~43–45 µs |

### Idle flush (fixes the missing tail)
`model_worker.py` patch: every `NO_PROGRESS` iteration of the worker loop calls
`_rec_idle_flush()`, which writes out buffered records (a bool check when nothing is pending);
the same call is registered on the worker's exit stack. Under load the scheduler never reported
`NO_PROGRESS` between steps: in 600 steps there were 77–79 interval flushes and 1 idle flush
(8.5–17 µs).

Runs `record_idle1s_1/2` (`MAX_SERVE_RECORD_METRICS_FLUSH_S=1.0`, raw output in `runs_idle1s.txt`):
- record file 607 records (CE 5, TG 602), ends on a record boundary; Prometheus CE 5 / TG 602,
  same as base_1/base_2; input tokens (10224 / 115200), batch size, cache misses, terminated
  reqs identical to base.
- steady state (windows ending at 400 and 600 steps): create 29.9–31.8 µs, publish 0.2 µs,
  record append 14.2–15.7 µs, log line 0.5 µs → ~45–48 µs total vs base ~73–75 µs.
- TG `batch_execution_time` sums were lower in both runs (~85 s vs 109–117 s); per-step worker
  metrics cost is µs, so this is not attributable to the change and was not investigated.

### Correction: the "steady state" windows were the batch-32 load
`run_server.sh` runs the batch-512 load first (~200 TG steps), then batch 32. The stage log's
first 200-step window is therefore batch 512, not warmup; windows 400/600 are batch 32. Per
load, `create` / `compute_values` median: base 71.4–73.2 µs @512, 40.7–41.8 µs @32; record
69.6–71.7 µs @512, 29.9–31.8 µs @32.

### Reducing compute_values (`PATCH_FAST_VALUES=1`, `fast_values_patch.py`)
Section timers (`PATCH_CV_TIMING=1`) at batch 512 showed `inputs.batch_size` ~33 µs (property
rebuilds `flat_batch` and rescans 512 contexts) and `get_metrics_aggregated` ~10–12 µs (empty
`KVCacheMetrics` + 27-field `__add__` with the null connector); block counts ~8 µs.
Exact changes: single-pass `TextGenerationInputs.__post_init__` that counts padding contexts
(`batch_size` = Σlen − padding), `BlockManager.metrics` returns its own metrics when the
connector's are the empty default, DP == 1 block counts without comprehensions.

Checks: `tests/fast_values_equiv.py` (20,000 random DP/padding/CE-TG batches vs the pristine class;
20,000 random `KVCacheMetrics`, 27 fields + 10 properties), `parity_test.py`, GPU runs
`record_fast_1/2`: Prometheus TG 602, input/batch-size/terminated/cache-miss sums as base.

| compute_values median | batch 512 | batch 32 |
|---|---|---|
| record (record_idle1s_1/2) | 69.6–71.7 µs | 29.9–31.8 µs |
| record + fast values (record_fast_1/2) | 26.8 µs | 20.4–21.9 µs |

Remaining (record_fast_cv_1, section medians, 512 / 32): device+host block counts 7.7 / 5.8–6.0,
reset_metrics 3.5 / 2.7–2.8, latency/throughput properties 3.0 / 2.4–2.5, throughput
2.9 / 1.7–1.8, disk counts + agg attrs 1.5, get_metrics_aggregated 1.6 / 1.2, zero-init 0.9–1.2,
CE scan 0.5–0.7; the rest of the stage is call overhead and timers.
TG batch_creation_time sums 478 / 463 ms vs 481–495 ms (single-pass __post_init__); within
run-to-run spread, not claimed.

### One KV-cache call (`PATCH_KV_SNAPSHOT=1`)
`PagedKVCacheManager.metrics_snapshot(num_replicas)` returns the 24 KV values compute_values
needs as a flat tuple and resets the per-batch counters. For connectors that keep the default
tier counts / empty metrics / no-op reset (null connector) it reads pool free counts and the
block manager's `_metrics` directly, inlines the six latency/throughput properties, and resets
with a zeroed `KVCacheMetrics` built via `__dict__.update`; otherwise it makes the original calls.
compute_values uses it when present and keeps the original block as fallback.

Checks: `tests/kv_snapshot_equiv.py 20000` runs compute_values twice from the same state on a
real BlockManager (DP 1/2/4, null connector and a tiered fake overriding counts/metrics/reset):
value tuples (values and types) and post-reset state identical; a planted swap of two values is
caught. GPU runs `record_snap_1/2`: Prometheus TG 602, deterministic sums as base.

| compute_values median | batch 512 | batch 32 |
|---|---|---|
| record | 69.6–71.7 µs | 29.9–31.8 µs |
| + fast values | 26.8 µs | 20.4–21.9 µs |
| + fast values + KV snapshot | 19.4–19.5 µs | 15.0–16.6 µs |

The section timers do not line up with the snapshot path (the fallback branch still contains
their anchors), so `record_snap_cv_1`'s section breakdown is not valid; only its stage totals are.

### Where the record append goes (`MAX_SERVE_APPEND_SPLIT=1`, runs `record_split_1/2`)
`MetricsLog.append_values_timed` times tuple → record conversion and record encode into the
64 KB buffer inside Mojo (monotonic clock); Python times the Mojo call and the flush.
Median µs (batch 512 window / batch 32 windows), 1 s flush, fast values + KV snapshot:

| part | batch 512 | batch 32 |
|---|---|---|
| conversion (CPython API reads of 55 values) | 7.3–7.5 | 5.2–5.6 |
| encode into buffer (pure Mojo) | 7.9–8.0 | 4.5–4.8 |
| extension call boundary | 4.3 | 3.0–3.3 |
| wrapper outside the call (tuple concat, clock, branch) | 1.9 | 1.6–1.8 |
| total, step without flush | 23.1–23.6 | 15.6–16.4 |
| flush (write syscall), per flushing step | 38.8–40.0 (65–67 of 200 steps) | 26.6–41.6 (13 of 200) |

Flush amortized over all steps: ~13 µs/step at batch 512 (a flush every ~3 steps of ~270 ms),
~2.7 µs/step at batch 32. The same microbench gives conversion 0.7 µs and encode 0.6 µs, so even
pure Mojo encode runs ~8× slower in the live worker; not explained.

### Priority test: the live slowdown is the gap between steps, not QoS
`tests/bench_append.py` (append microbench; `tests/qos.py` reads the thread QoS). Processes started
from this shell report QoS USER_INTERACTIVE (nice 5). Per append call, median µs:

| pattern | default QoS | `taskpolicy -c utility` | `taskpolicy -c background` |
|---|---|---|---|
| dense (back to back) | 1.08 | 1.08 | 4.00 |
| one append per 60 ms sleep | 17.4 | 20.7 | 43.1 |

Gap sweep (sleep before each append): 0 ms 1.7, 1 ms 2.5, 5 ms 4.4, 20 ms 10.0, 60 ms 16.0,
270 ms 20.2 µs. Busy-waiting instead of sleeping gives the same (60 ms 16.4, 270 ms 21.2), so it
is not frequency scaling or wake-up; code that runs once per step pays a cold-CPU-state cost
(caches / predictors / core migration — not separated). The sparse microbench reproduces the
live split (conversion ~7, encode ~5 µs), so the worker's QoS is not making it worse.

### mmap destination (`mmap_destination.mojo`, `MAX_SERVE_RECORD_METRICS_MMAP=1`)
`MmapFileDestination`: MAP_SHARED mapping of a file pre-sized in 64 MB chunks (grown by remap),
committed length in a mapped 8-byte sidecar `<path>.len` updated after each record,
`close()` trims the file. `MetricsLogMmap` exposes it; the worker skips all flush logic; the
telemetry tailer reads `.len` and the log with `os.pread` (a buffered reader serves stale
zeros from past the committed length — this broke the first version of the check).
Checks: `tests/mmap_check.py` (byte-identical to the buffered log at chunk 4 KB and 64 MB,
mid-stream reads, trim), `tests/mmap_tail_check.py` (real `tail_and_publish` gets all 3000
records while the log is open, equal to the buffered log's decode).
Dense / 60 ms-sparse microbench: same append cost as buffered (1.0 / 16–16.5 µs).

GPU runs (fast values + KV snapshot; buffered = `record_snap_1/2` at 1 s flush):

| record_append | batch 512 med / p90 | batch 32 med / p90 |
|---|---|---|
| buffered, 1 s flush | 23.8–24.5 / 65.6–65.7 | 14.7–16.4 / 18.7–26.8 |
| mmap (`record_mmap_1/2`) | 20.2–20.7 / 23.4–25.5 | 13.7–14.2 / 15.1–16.9 |

Split with mmap (`record_mmap_split_1/2`): conversion, encode (now including page faults),
boundary and wrapper unchanged vs `record_split_1/2`; no flushes. Prometheus TG 602 and
deterministic sums as base; record files complete.
The worker's exit-stack callback never ran in any of these shutdowns (no close log line,
file left at 64 MB with a valid `.len`): with mmap the tail is still complete because the
bytes were already in the page cache; readers must use `.len` until a trim happens.

### Vision and video metrics in the record (and a VLM run)
`VisionEncoderMetrics` (5 fields) and `VideoEncoderMetrics` (6, incl. per-clip `frame_counts`)
are now sub-records of `BatchMetricsRec` alongside `CompletedRec`, so the worker never
publishes a step itself (`published_in_worker` stays False; it is kept FIRST in EXTRA because
inserting fields before it changes its id — Dagr's gate flagged that as a wire break).
`record_spec.SUBRECORDS` drives schema + codegen; `pyconv` gained `as_u64_list` / `py_u64_list`
and a per-sub-record key cache.

- `parity_test.py 3000` with random vision/video metrics: 78,816 measurements across **50**
  instruments (was 41) identical; 757 steps published vision metrics, 522 video; 252 B/step.
- No architecture in this build emits `VideoEncoderMetrics`, and `VisionEncoderCache` is used
  only by gemma4 and kimik2_5 (31B+ / far larger) — neither fits in 64 GB, so vision-encoder
  metrics have no local end-to-end run; the randomized parity above is their only coverage.

GPU run with a real VLM (`Qwen/Qwen2.5-VL-3B-Instruct`, `--devices gpu`, batch 32, image
requests via `load_vlm.py`, mmap log). It has no vision-encoder cache, so it reports no vision
metrics, but it exercises image prefill and `TextAndVisionContext` end to end:

| Qwen2.5-VL, 24 streams × 400 tokens | base_vlm_1 | record_vlm_1 |
|---|---|---|
| Prometheus steps (CE / TG) | 2 / 399 | 2 / 399 |
| input tokens CE / TG, cache misses, terminated | 2184 / 9576, 2184, 24 | identical |
| create / compute_values | 29.8–30.8 µs | 16.7–17.1 µs |
| publish | 29.2–29.9 µs | 0.1 µs |
| record append | – | 12.9–13.5 µs |
| worker total | ~60 µs | ~30 µs |
| client streams, total median | 24/24, 34.41 s | 24/24, 34.15 s |

Record file: 401 records (CE 2, TG 399), 58,532 bytes, matching the Prometheus counts.

### Telemetry-process cost (`MAX_SERVE_TELEMETRY_TIMING=1`, run `record_tel_1`)
Timings are appended to `<log>.telemetry.txt` every 10 s (the telemetry process has no usable
logger). Per record: rebuild `BatchMetrics` 18.0–21.1 µs, publish (OTel commit of ~24
measurements) 82–101 µs. Per poll (20 ms): read 14–18 µs, decode 19.5–27 µs — the decode covers
every record that arrived in that poll. Scrape: 4.4–4.8 ms for a 140 KB body, 5 scrapes.

So the telemetry side spends ~100–120 µs of CPU per scheduler step, dominated by OpenTelemetry
aggregation, against ~30 µs now left in the worker. At 600 steps/lifetime that is nothing; at
1000 steps/s it is ~10% of a core, at 10k steps/s about one core.
Not measured: the same aggregation in base mode (it happens there too, minus the rebuild and
plus unpickling), so this is not yet a before/after.

### flare (github.com/ehsanmok/flare) as the basis for a Mojo telemetry process
- Does NOT build with MAX's pinned Mojo 1.1.0.dev2026082707: flare main locks Mojo 1.0.0
  (`pixi.lock`), and `flare/http/_client/parse.mojo` uses `String(capacity=…)`, renamed to
  `capacity_bytes` in 1.1. Version skew, not a deep incompatibility.
- `flare/http/metrics.mojo` is a fixed middleware (requests by method/status, one default-bucket
  latency histogram, in-flight gauge, error counter), NOT a general registry: MAX's 41 families
  with log-spaced buckets and arbitrary labels would have to be written. Its one-String-per-scrape
  text renderer is a good template.
- Extra deps: the `json` Mojo package (separate repo) and C FFI wrappers (OpenSSL, zlib, brotli)
  built on environment activation; none needed for a metrics endpoint.
- MIT, v0.10.0, active (pushed 2026-09-19), macOS + Linux, 56 stars.

### Baseline telemetry process, for comparison (`base_tel_1`)
Same timer, on MAX's own commit loop (`MAX_SERVE_TELEMETRY_TIMING=1` patches
`process_telemetry`): **3.00–3.21 µs median per commit batch**, with a median of ONE measurement
per batch and 24,630 batches per 10 s window. Those are the API process's per-request /
per-token measurements, which are sent one at a time (only the worker opens transactions) — at
~2,500/s they dominate the telemetry process's work, not the ~10/s of batch metrics.

Per measurement the aggregation costs the same in both modes: base ~3 µs × ~24 measurements
per step ≈ 72–96 µs, record mode 82–101 µs measured directly. So design 1 did not add
aggregation cost to the telemetry process; it added the rebuild (18–21 µs/step) and removed the
queue: `pickle.dumps` 8.11 µs + `loads` 6.38 µs for a 24-measurement batch (1747 B), i.e.
~14 µs/step of serialization that record mode does not pay (`tests/bench_queue.py`).

Conclusion for a Mojo/flare rewrite: the target is OpenTelemetry aggregation at ~3 µs per
measurement (~2,500/s here from the API side alone) plus a 4.4–4.8 ms scrape of a 140 KB body —
a pre-existing cost in stock MAX, not something design 1 introduced.

### Generic measurements out of multiprocessing.Queue (`MAX_SERVE_MEASUREMENT_LOG=<dir>`)
`MaxMeasurements` Dagr sink: `MeasurementRec(ts_ns, name_id, attrs_id, value)` plus interned
`NameRec` / `AttrSetRec`, one mmap stream per producing process (`meas-<pid>.dagr`).
`dagr_metric_client.DagrMetricClient` is a `MetricClient` that interns names/attribute sets
(identity fast path for the shared per-process attribute dict), buffers rows and hands them to
Mojo in batches (`append_rows`, default 64 / 0.05 s / transaction close);
`measurement_publish.tail_measurements` follows every producer's file in the telemetry process
and commits. `max_patch.patch_telemetry_worker` swaps the client in, so the API process and —
through `cross_process_factory` — the model worker both write streams.

Checks: `tests/measurement_roundtrip.py` — 5,000 measurements (4 names, 4 attribute sets, mixed
transactions) round-trip identically in name, value, attributes and timestamp. GPU runs
`meas_base_2` (everything through the log) and `meas_record_1` (+ record log for batch metrics):
Prometheus TG steps 602, ITL count 114,656, TTFT count 544 — identical to the queue baseline
`base_tel_1`. ~230k measurements per lifetime, 5.9 MB ≈ 25 B/measurement.

| per measurement, producing thread | dense | 1 ms gap |
|---|---|---|
| `ProcessMetricClient` (queue) | 0.58 µs | 1.04 µs |
| `DagrMetricClient` | 0.25 µs | 0.38 µs |

Plus the queue's serialization, which disappears: 8.11 µs `dumps` per 24-measurement batch in
the worker's feeder thread and 6.38 µs `loads` in the telemetry process (~0.34 / 0.27 µs per
measurement).

Telemetry side per measurement: commit 3.79–3.94 µs (queue baseline 3.00–3.21 µs for the same
OTel work — the aggregation is unchanged), decode 14.9–15.2 µs per poll for ~1.5k measurements
(~0.01 µs each), read 7.7–7.9 µs per poll.

Worker publish stage (base mode, 24 measurements per step in one transaction): 43.1–53.5 µs
through the Dagr client vs 31.0–31.2 µs through the queue — for a burst, `put_nowait` plus
off-thread pickling beats interning 24 rows and one extension call. That path is moot in the
final design (batch metrics go to the record log, publish 0.1 µs), but it is the honest number
for "everything through the measurement log".

### A Prometheus aggregator in Mojo, diffed against MAX's endpoint
`prom_agg.mojo` reads the `meas-*.dagr` streams, aggregates (counters summed, UpDownCounters
summed, Gauges last-value, histograms bucketed) and renders Prometheus text — no Python, no
OpenTelemetry. `gen_prom_table.py` generates its instrument table from MAX's own `SERVE_METRICS`
and `HISTOGRAM_BUCKETS_BY_METRIC`, learning each unit's family-name suffix from a real scrape
(so a rule that drifts is caught at generation, not in the diff); `tests/diff_metrics.py` compares
two exposition files by series, ignoring formatting, order, `python_*` and `_created`.

Run `meas_base_3` (everything through the measurement log, 246,286 measurements up to the
scrape instant):

| | MAX `/metrics` | `prom_agg` |
|---|---|---|
| families / series | 36 / 1779 | 36 / 1779 |
| equal | — | 1771 |
| within 1e-9 (float summation order) | — | 8 |
| differing | — | 0 |
| series only on one side | 0 | 0 |

Root cause of the off-by-one (found, fixed): `DagrMetricClient` flushed only when a measurement
was appended, so once the load stopped, the two once-per-second API samplers
(`requests_awaiting_admission`, `responses_buffered`) left their last sample in the Python
buffer — it was taken 899 ms before the scrape and reached the stream after it. The reader's
first committed timestamp equalled the stream's first, which ruled out a startup race; the
sample count at the scrape instant (128) versus the endpoint (127) pinned it to the producer.
The client now has a background flusher thread enforcing the delay bound, with a lock so a
flush cannot swap the buffer out from under an append (producer cost 0.25 -> 0.29 µs dense).

After the fix, run `meas_base_6`: **1771 series equal, 8 within 1e-9 (float summation order),
0 differing, 0 on only one side** — the Mojo aggregator reproduces MAX's endpoint exactly.

Cost: **0.04 s wall / 0.03 s user for 246k measurements** (~0.16 µs each, including rendering
1779 series), against ~3.8 µs per measurement for OTel aggregation in Python plus a 4.4–4.8 ms
scrape — roughly 20× cheaper per measurement.

Two harness bugs this exposed: the scrape loop kept the LAST of 5 scrapes, and an OTel
synchronous Gauge only reports if recorded since the previous collection, so 4 gauge families
silently vanished from every recent scrape; `run_server.sh` now keeps the first scrape and
records its timestamp for the cutoff.

Not covered: serving the text over HTTP (flare does not build against MAX's pinned Mojo),
`_created` lines, exemplars, OTLP export, and incremental aggregation (it reads whole streams).

### A full telemetry endpoint in Mojo 1.0 (flare + Dagr), byte-for-byte against MAX
The standalone endpoint links no MAX, so it is free to use flare's Mojo. Checks:

- **Dagr's generated Mojo compiles on Mojo 1.0.0 unchanged** — the sink readers/writers, framing
  and `comptime` all build with 1.0. The only 1.1-ism was mine: `String(capacity_bytes=…)`
  (1.0 spells it `capacity=`), now written version-neutrally.
- The 1.0 build of the aggregator produces **byte-identical output** to the 1.1 build.
- flare (main, in its own pixi env) builds on Mojo 1.0.0, and a binary importing **both** flare
  and the generated Dagr code links and runs.

`telemetry_server.mojo` (built in flare's env: `pixi run mojo build -I . -I <proto>/gen/mojo
-I <proto> <proto>/telemetry_server.mojo`) serves `GET /metrics` from the measurement streams,
with `/health` alongside. Scraped over HTTP:

```
HTTP/1.1 200 OK    Content-Type: text/plain; version=0.0.4; charset=utf-8
139,322 bytes   1779 series / 36 families
vs MAX's endpoint: 1771 equal, 8 within 1e-9, 0 differing, 0 on one side only
```

#### Incremental aggregation
The registry now lives across scrapes (`IncrementalAggregator` in `prom_core.mojo`): each
request reads only the bytes past each stream's last offset, carries a trailing partial record
to the next read, and picks up producer files as they appear. flare's `Handler` borrows `self`
read-only, so the state sits in a heap cell addressed by `Int` — flare's own middleware pattern.

| scrape | total | ingest | render |
|---|---|---|---|
| first (absorbs the 246k backlog) | 36.7 ms | 34.4 ms | 0.9 ms |
| steady state, nothing new | 2.7–3.4 ms | 0.2 ms | 0.9–2.2 ms |
| ingesting 24.4k new measurements | — | 5.7 ms (0.23 µs each) | 0.07 ms |

MAX's own endpoint takes 4.4–4.8 ms per scrape, so the steady-state Mojo scrape is now faster
while producing the same bytes.

Correctness under a live writer (`tests/live_incremental.py`): 219,205 measurements written in
bursts of 1–300 with varying pauses, 11 scrapes during the run, then the endpoint's final body
compared against a one-shot aggregation of the finished streams — **byte-identical**. That is
what exercises the offsets and split records; repeat scrapes with no new data are identical too.

Mojo 1.0 rejected two things the 1.1 compiler accepted (moving a value out of a `Dict` entry;
`seek` taking `UInt64`), so the source is now written to compile on both: the CLI builds with
MAX's 1.1, the server with flare's 1.0, from the same `prom_core.mojo`.

Packaging note: flare `dlopen`s its FFI wrappers by relative path (`build/libflare_tls.so`), so
the binary must run with flare's directory as its working directory until that is packaged.

### OTLP egress in Mojo (`otlp.mojo`, `otlp_push.mojo`)
The push side of the same registry: `build_request` encodes the aggregator's series as an
`ExportMetricsServiceRequest` (protobuf), `otlp_push` follows the streams incrementally and
POSTs it with `Content-Type: application/x-protobuf` via flare's client. Field numbers were
taken from the generated opentelemetry-proto descriptors (dumped and verified, not recalled):
counters and up/down counters map to `Sum` (is_monotonic distinguishes them), synchronous
gauges to `Gauge`, histograms to `Histogram` with per-bucket counts and explicit bounds.
Temporality is CUMULATIVE — the registry never resets; a delta exporter would have to subtract
the previously exported state.

Checks: `tests/otlp_check.py` decodes the payload with the official protobuf and compares every
data point against the Prometheus text the same aggregator renders — 36 metrics, 130 value
checks (sums, gauges, histogram counts/sums and every bucket bound), 0 mismatches.
`tests/otlp_receiver.py` (a minimal OTLP/HTTP endpoint) accepted three real posts: 35,801 bytes,
36 metrics, 52 data points, 200 OK each. Steady-state cost per export: encode 219–232 µs,
POST 738–747 µs.

Not covered: delta temporality (MAX configures DELTA for a user-supplied OTLP endpoint),
exponential shadow histograms (MAX sends those to OTLP only), `start_time_unix_nano` semantics
beyond "first observation seen", resource attributes beyond `service.name`, retries/backoff,
gzip, and gRPC transport.


### Patches located by AST, not by quoting MAX
The patch scripts used to anchor on verbatim excerpts of MAX source (~280 lines across the two
files), which the Modular MAX Community License does not permit redistributing. Every anchor is
now an AST lookup (`astpatch.py`): the assignment calling `BatchMetrics.create`, the `With` on
`METRICS.transaction`, the `Return` calling `process_telemetry`, the `While True` scheduler loop
and its `SchedulerProgress.NO_PROGRESS` branch, the `async with start_process_consumer`, the
`__post_init__` / `batch_size` / `metrics` functions, the three block-count assignments, the
`if kv_cache is not None` block, and the `for m in ms` commit loop. Replacements that must keep
MAX's code lift it from the file at patch time.

Dropped rather than converted: the per-section timers inside `compute_values`, which needed
line-level anchors. Their results (batch_size ~33 µs, KV metrics ~10 µs at batch 512) are above.

The refactor introduced one bug the in-process tests could not catch: replacing the whole
`async with start_process_consumer(...)` branch instead of just its `yield` meant the telemetry
process never spawned, so `/metrics` served nothing while the worker looked healthy. A GPU run
caught it; the patch now replaces only the yield.


### mmap through mm_mmap instead of hand-rolled syscalls
`MmapFileDestination` originally called `mmap`/`munmap` itself with macOS constants. It now uses
[mm_mmap](https://github.com/Mojo-Mania/mm_mmap) (vendored at `2806959`, Apache-2.0):
`MemoryMap.map_fd` for the mapping, RAII unmapping (growth is an assignment), and `platform_map`
for `AT_FDCWD` (-2 macOS, -100 Linux), so the destination is no longer macOS-only. What stays
local is the file plumbing — create, size, trim — and the growth policy.

Both toolchains compile it unchanged (MAX's Mojo 1.1, flare's 1.0). Verified after the swap:
the mmap log is byte-identical to the buffered one at 4 KB and 64 MB chunks (the 4 KB case
remaps repeatedly), mid-stream reads, the live tailer (3,000 records), the measurement round
trip, and a GPU lifetime matching the baseline series (TG 602, ITL 114,656, input tokens
115,200) at unchanged cost (~15.5 µs build values, ~15.5 µs append).

`flush()` is still a no-op; mm_mmap exposes `flush()` (msync) if durability against an OS crash
is wanted, not just a process crash.
