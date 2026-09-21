# Dagr + Mojo metrics for MAX serve

An experiment: carry [MAX](https://docs.modular.com/max/) serve's metrics as packed
[Dagr](https://github.com/mzaks) records written from Mojo, instead of Python objects and
pickled queue traffic — and then rebuild the whole telemetry process, Prometheus endpoint and
OTLP egress included, in Mojo.

Every published number stays identical to stock MAX; what changes is what it costs.

| Model worker, per scheduler step | MAX today | With Dagr + Mojo |
| --- | --- | --- |
| Batch 32 | ~74 µs | ~28 µs |
| Batch 512 | ~113 µs | ~38 µs |

| Per measurement | MAX today | With Dagr + Mojo |
| --- | --- | --- |
| Producing thread | 0.58 µs + ~0.6 µs pickling | 0.29 µs, no pickling |
| Telemetry-side aggregation | ~3.8 µs (OpenTelemetry) | 0.23 µs |
| `/metrics` scrape | 4.4–4.8 ms | 2.7–3.4 ms |
| OTLP export (36 metrics) | — | encode 0.23 ms, POST 0.74 ms |

A step's metrics are 98.5 bytes on the wire against 606 as msgspec JSONL, and the log scans 18×
faster. Measured on an M4 Max, MAX 26.6.0.dev2026082707, Qwen3-0.6B and Qwen2.5-VL-3B on the
Apple GPU. `docs/measurements.md` is the full lab log: every number here, how it was taken, and
the ideas that measurement killed.

## How it fits together

```
model worker ──step record──┐
                            ├──► Dagr streams (mmap + committed-length sidecar)
API process ──measurements──┘         │
                                      ├──► Python: replay → MAX's own publish functions
                                      └──► Mojo: aggregate → /metrics  and  OTLP push
```

- **In the worker** `BatchMetrics.create()` splits into "compute the 55 values" and "build the
  object"; record mode sends the values to Mojo and publishes nothing locally. Vision and video
  encoder metrics ride along as sub-records.
- **Between processes** every other measurement leaves as `(timestamp, name id, attributes id,
  value)`, with names and attribute sets interned on first use — no `multiprocessing.Queue`, no
  pickling.
- **In telemetry** either Python replays the records into MAX's own publish functions, or a Mojo
  binary reads the same streams and serves `/metrics` and OTLP itself.

## Layout

| Path | What |
| --- | --- |
| `schema.py`, `record_spec.py` | Dagr schemas; the single source for the record's fields |
| `gen_record_code.py`, `gen_prom_table.py` | generate the Mojo conversion code and the instrument table (from MAX's own `SERVE_METRICS`) |
| `metrics_log.mojo`, `measurement_log.mojo`, `pyconv.mojo`, `mmap_destination.mojo` | the Python extension: record/measurement writers, CPython fast paths, the mmap destination |
| `max_patch.py`, `fast_values_patch.py` | patch a locally installed MAX: record mode, the client swap, the exact `compute_values` speedups |
| `dagr_metric_client.py`, `record_publish.py`, `measurement_publish.py` | producer-side client and the telemetry-side readers |
| `prom_core.mojo`, `prom_agg.mojo`, `otlp.mojo` | aggregation, Prometheus rendering, OTLP protobuf |
| `telemetry_server.mojo`, `otlp_push.mojo` | the flare-served endpoint and the OTLP pusher |
| `run_server.sh`, `load_gpu.py`, `load_vlm.py`, `compare_metrics.py` | GPU run harness and loads |
| `tests/` | parity, equivalence and round-trip checks, plus the microbenchmarks |
| `results/`, `docs/` | raw run output and the lab log |

## Reproducing

```sh
uv venv -p 3.12 .venv
uv pip install -p .venv/bin/python --prerelease=allow "max[serve]==26.6.0.dev2026082707" \
    "mojo==1.1.0.dev2026082707" msgspec httpx pillow \
    --extra-index-url https://whl.modular.com/nightly/simple/ --index-strategy unsafe-best-match

dagr build                                   # needs Dagr >= b03fad2
.venv/bin/python gen_record_code.py
.venv/bin/mojo build --emit shared-lib -I gen/mojo -I . metrics_log.mojo -o metrics_log.so
PATCH_FAST_VALUES=1 PATCH_KV_SNAPSHOT=1 .venv/bin/python max_patch.py

MEAS=1 MAX_SERVE_RECORD_METRICS_MMAP=1 sh run_server.sh myrun 1 record
```

The Mojo endpoint builds against flare (Mojo 1.0), the in-process extension against MAX's
Mojo 1.1; the shared sources compile on both:

```sh
.venv/bin/python gen_prom_table.py runs/myrun.metrics    # instrument table from MAX
cd ../flare_check && pixi run mojo build -I . -I ../dagr_max_prototype/gen/mojo \
    -I ../dagr_max_prototype ../dagr_max_prototype/telemetry_server.mojo \
    -o ../dagr_max_prototype/telemetry_server
```

## Checks

| Command | Checks |
| --- | --- |
| `parity_test.py 3000` | 78,816 measurements across 50 instruments identical between the original path and record → file → decode → publish |
| `tests/fast_values_equiv.py`, `tests/kv_snapshot_equiv.py` | the `compute_values` speedups against the pristine MAX classes, 20,000 random inputs each |
| `tests/mmap_check.py`, `tests/mmap_tail_check.py` | the mmap log is byte-identical to the buffered one; a reader sees every record while it is open |
| `tests/measurement_roundtrip.py` | measurements survive name, value, attributes and timestamp |
| `tests/diff_metrics.py` | the Mojo endpoint against MAX's `/metrics`, series by series |
| `tests/live_incremental.py` | incremental aggregation equals one-shot under a live writer |
| `tests/otlp_check.py` | OTLP payloads decoded with opentelemetry-proto and compared to the exposition |

## Licensing — read before publishing

MAX ships under the **Modular MAX Community License**, which grants distribution of derivative
works **in object code form only**. Two consequences:

1. The `*.orig` files are pristine copies of MAX source made by the patch scripts. They are
   git-ignored and must stay that way.
2. `max_patch.py` and `fast_values_patch.py` locate the code they rewrite using **verbatim
   excerpts of MAX source** as anchors (~280 lines across both). Publishing them publicly
   redistributes that source in source form. Before making this repository public, either keep
   it private, get Modular's blessing, or replace the anchors with structural (AST) matching so
   no MAX source is embedded.

`NOTICE` carries the attribution the licence requires. Dagr itself and everything else here is
the author's own work.

## Status

A prototype, not a proposal: it patches a pinned MAX wheel at runtime and nothing has been sent
upstream. Known gaps — OTLP is cumulative-only (no delta, no exponential histograms), vision and
video records are never filled by a model small enough to run here, the endpoint has no TLS or
auth, flare loads its FFI wrappers by relative path, and the two Dagr changes this depends on
(a streaming writer and a buffered destination) are unpushed on Dagr's `main`.
