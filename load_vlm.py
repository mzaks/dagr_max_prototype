"""Drive MAX serve with N concurrent streaming chat requests carrying an image.

Usage: .venv/bin/python -I load_vlm.py <streams> <max_tokens> [procs] [distinct_images]

Each request sends one PNG as a data URI plus a short prompt. `distinct_images` controls how
many different images are used: fewer images means more vision-encoder cache hits, so a VLM
with an encoder cache reports both encoded and cached images.
"""

import asyncio
import base64
import io
import multiprocessing as mp
import sys
import time

import httpx
from PIL import Image

BASE = "http://127.0.0.1:8200"
MODEL = "Qwen/Qwen2.5-VL-3B-Instruct"


def make_image(seed: int, size: int = 224) -> str:
    img = Image.new("RGB", (size, size))
    px = img.load()
    for y in range(size):
        for x in range(size):
            px[x, y] = ((x * 7 + seed * 31) % 256, (y * 5 + seed * 17) % 256, (x + y + seed) % 256)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode()


def client_proc(n, max_tokens, images, done_q, start_evt):
    async def one(client, idx):
        body = {
            "model": MODEL,
            "messages": [{"role": "user", "content": [
                {"type": "image_url", "image_url": {"url": images[idx % len(images)]}},
                {"type": "text", "text": "Describe this image in detail."},
            ]}],
            "max_tokens": max_tokens,
            "ignore_eos": True,
            "stream": True,
        }
        first = None
        t0 = time.perf_counter()
        n_chunks = 0
        try:
            async with client.stream("POST", f"{BASE}/v1/chat/completions", json=body) as r:
                if r.status_code != 200:
                    return ("error", r.status_code, (await r.aread())[:400].decode("utf8", "replace"))
                async for line in r.aiter_lines():
                    if line.startswith("data: ") and not line.endswith("[DONE]"):
                        n_chunks += 1
                        if first is None:
                            first = time.perf_counter() - t0
        except Exception as e:  # noqa: BLE001
            return ("error", 0, repr(e)[:200])
        return ("ok", first, time.perf_counter() - t0, n_chunks)

    async def main():
        start_evt.wait()
        async with httpx.AsyncClient(timeout=1200) as client:
            res = await asyncio.gather(*[one(client, i) for i in range(n)])
        done_q.put(res)

    asyncio.run(main())


def main():
    streams = int(sys.argv[1])
    max_tokens = int(sys.argv[2])
    procs = int(sys.argv[3]) if len(sys.argv) > 3 else 4
    distinct = int(sys.argv[4]) if len(sys.argv) > 4 else 8
    images = [make_image(i) for i in range(distinct)]
    ctx = mp.get_context("spawn")
    done_q = ctx.Queue()
    start = ctx.Event()
    per = [streams // procs + (1 if i < streams % procs else 0) for i in range(procs)]
    ps = [ctx.Process(target=client_proc, args=(k, max_tokens, images, done_q, start))
          for k in per if k]
    for p in ps:
        p.start()
    time.sleep(2)
    t0 = time.perf_counter()
    start.set()
    res = [r for _ in ps for r in done_q.get()]
    for p in ps:
        p.join()
    ok = [r for r in res if r[0] == "ok"]
    err = [r for r in res if r[0] != "ok"]
    print(f"{len(ok)}/{len(res)} streams ok in {time.perf_counter() - t0:.1f}s "
          f"({distinct} distinct images, {max_tokens} tokens each)")
    if ok:
        ttft = sorted(r[1] for r in ok if r[1] is not None)
        print(f"  TTFT median {ttft[len(ttft) // 2]:.2f}s, total median "
              f"{sorted(r[2] for r in ok)[len(ok) // 2]:.2f}s")
    for e in err[:3]:
        print("  error:", e[1], e[2])


if __name__ == "__main__":
    main()
