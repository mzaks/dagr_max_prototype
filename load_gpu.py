"""Drive MAX serve with N concurrent streaming chat requests and read its own metrics.

Usage: .venv/bin/python -I load_gpu.py <total_streams> <max_tokens> [procs] [window_s]

Streams are split across client processes so client-side parsing stays cheap. Every
request sets ignore_eos so the decode batch stays full. Prometheus histograms are scraped
at the start and end of a steady-state window and differenced.
"""

import asyncio
import multiprocessing as mp
import re
import sys
import time

import httpx

BASE = "http://127.0.0.1:8200"
METRICS = "http://127.0.0.1:8201/metrics"
MODEL = "Qwen/Qwen3-0.6B"


def client_proc(n, max_tokens, first_q, done_q, start_evt):
    async def one(client, idx):
        body = {
            "model": MODEL,
            "messages": [{"role": "user", "content": f"Write a long story about river number {idx}."}],
            "max_tokens": max_tokens,
            "ignore_eos": True,
            "stream": True,
        }
        stamps = []
        async with client.stream("POST", f"{BASE}/v1/chat/completions", json=body) as r:
            r.raise_for_status()
            async for line in r.aiter_lines():
                if line.startswith("data:") and "[DONE]" not in line:
                    stamps.append(time.time())
                    if len(stamps) == 1:
                        first_q.put(stamps[0])
        done_q.put((stamps[0], stamps[-1], len(stamps)))

    async def main():
        start_evt.wait()
        limits = httpx.Limits(max_connections=n + 4, max_keepalive_connections=n + 4)
        async with httpx.AsyncClient(timeout=None, limits=limits) as client:
            await asyncio.gather(*(one(client, i) for i in range(n)))

    asyncio.run(main())


def scrape():
    text = httpx.get(METRICS, timeout=30).text
    out = {}
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        m = re.match(r"^(\S+?)(\{.*\})?\s+([0-9.eE+-]+|NaN)$", line)
        if m:
            out[(m.group(1), m.group(2) or "")] = float(m.group(3))
    return out


def hist_by_labels(a, b, prefix):
    out = {}
    for (k, labels), v in b.items():
        if not k.startswith(prefix) or not k.endswith("_count"):
            continue
        base = k[: -len("_count")]
        lab = re.sub(r',?le="[^"]*"', "", labels)
        dc = v - a.get((k, labels), 0.0)
        ds = b.get((base + "_sum", labels), 0.0) - a.get((base + "_sum", labels), 0.0)
        if dc > 0:
            out[(base, lab)] = (ds / dc, dc)
    return out


if __name__ == "__main__":
    total = int(sys.argv[1])
    max_tokens = int(sys.argv[2])
    procs = int(sys.argv[3]) if len(sys.argv) > 3 else 8
    window_s = float(sys.argv[4]) if len(sys.argv) > 4 else 8.0
    per = total // procs

    first_q, done_q = mp.Queue(), mp.Queue()
    start_evt = mp.Event()
    ps = [mp.Process(target=client_proc, args=(per, max_tokens, first_q, done_q, start_evt)) for _ in range(procs)]
    for p in ps:
        p.start()
    t0 = time.time()
    start_evt.set()
    firsts = [first_q.get() for _ in range(per * procs)]
    print(f"all {per * procs} streams streaming after {max(firsts) - t0:.1f}s")
    m0 = scrape()
    time.sleep(window_s)
    m1 = scrape()
    results = [done_q.get() for _ in range(per * procs)]
    for p in ps:
        p.join()
    for (base, lab), (mean, cnt) in sorted(hist_by_labels(m0, m1, "maxserve").items()):
        if "batch_execution" in base or "batch_creation" in base:
            print(f"  {base}{lab}: mean={mean:.3f} count={cnt:.0f}")
    itl = sorted((last - first) / (n - 1) * 1000 for first, last, n in results if n > 1)
    print(f"client: {len(results)} streams, per-stream ITL mean {sum(itl) / len(itl):.2f} ms")
