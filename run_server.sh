#!/bin/sh
# One MAX serve lifetime on the Apple GPU with prototype instrumentation, then the
# batch-512 and batch-32 loads, a cumulative metrics scrape, and a clean stop.
# Usage: sh run_server.sh <label> <stage_timing 0|1> <mode: base | dagr_log | record>
#   base      MAX as patched, original BatchMetrics path (no Dagr)
#   dagr_log  original path + 20-field Dagr log (batch_log.so)
#   record    design 1: record instead of object + publish; telemetry publishes from the log
# Outputs: runs/<label>.serve.log, runs/<label>.metrics, runs/<label>.dagr (dagr_log / record)
P="$(cd "$(dirname "$0")" && pwd)"
label=$1
stage=$2
mode=$3
MODEL=${MODEL:-Qwen/Qwen3-0.6B}
BATCH=${BATCH:-512}
MAXLEN=${MAXLEN:-512}
mkdir -p "$P/runs"
cd "$P" || exit 1
rm -f "runs/$label.dagr" "runs/$label.dagr.len"
export MAX_SERVE_STAGE_TIMING="$stage"
export MAX_SERVE_TELEMETRY_TIMING_PATH="$P/runs/$label.telemetry.txt"
rm -f "$P/runs/$label.telemetry.txt"
export MAX_SERVE_RECORD_METRICS_MODULE_DIR="$P"
if [ -n "$MEAS" ]; then                       # generic measurements -> Dagr stream
  export MAX_SERVE_MEASUREMENT_LOG="$P/runs/$label.meas"
  rm -rf "$MAX_SERVE_MEASUREMENT_LOG"
fi
if [ "$mode" = "record" ]; then
  export MAX_SERVE_RECORD_METRICS="$P/runs/$label.dagr"
  export MAX_SERVE_RECORD_METRICS_MODULE_DIR="$P"
  export MAX_SERVE_RECORD_METRICS_MODEL="$MODEL"
fi
if [ "$mode" = "dagr_log" ]; then
  export MAX_SERVE_BATCH_LOG="$P/runs/$label.dagr"
  export MAX_SERVE_BATCH_LOG_MODULE_DIR="$P"
  export MAX_SERVE_BATCH_LOG_MODEL=Qwen/Qwen3-0.6B
  export MAX_SERVE_BATCH_LOG_BUFFER_BYTES=65536
  export MAX_SERVE_BATCH_LOG_FLUSH_STEPS=256
fi
if lsof -nP -iTCP:8200 -sTCP:LISTEN >/dev/null 2>&1; then echo "port 8200 busy"; exit 1; fi
MAX_SERVE_METRICS_ENDPOINT_PORT=8201 HF_HOME="$P/hf" HF_HUB_OFFLINE=1 TRANSFORMERS_VERBOSITY=error \
  .venv/bin/max serve --model "$MODEL" --devices gpu --max-batch-size "$BATCH" --max-length "$MAXLEN" --port 8200 \
  > "runs/$label.serve.log" 2>&1 &
i=0
until grep -q "Server ready" "runs/$label.serve.log"; do
  sleep 5; i=$((i + 1)); [ $i -gt 120 ] && { echo "label=$label server did not start"; exit 1; }
done
if [ -n "$VLM_LOAD" ]; then
  .venv/bin/python -I load_vlm.py $VLM_LOAD > "runs/$label.loadvlm.txt" 2>&1
else
  .venv/bin/python -I load_gpu.py 512 200 8 20 > "runs/$label.load512.txt" 2>&1
  .venv/bin/python -I load_gpu.py 32 400 4 10 > "runs/$label.load32.txt" 2>&1
fi
sleep 3
.venv/bin/python -c "import time;print(time.time_ns())" > "runs/$label.scrape_ns"
# The first scrape is the one kept: OTel synchronous Gauges report only if recorded since the
# previous collection, so a later scrape silently drops them.
curl -s -o "runs/$label.metrics" -w "scrape %{time_total}s %{size_download} bytes\n" \
  http://127.0.0.1:8201/metrics > "runs/$label.scrape.txt"
for i in 2 3 4 5; do
  curl -s -o /dev/null -w "scrape %{time_total}s %{size_download} bytes\n" \
    http://127.0.0.1:8201/metrics >> "runs/$label.scrape.txt"
done
PID=$(lsof -nP -iTCP:8200 -sTCP:LISTEN -t)
[ -n "$PID" ] && kill -INT $PID
sleep 12
echo "label=$label stage=$stage mode=$mode"
grep "PROTOTYPE stages\|PROTOTYPE telemetry\|Traceback" "runs/$label.serve.log"
