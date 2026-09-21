"""Compare Prometheus output of base vs record (design 1) server runs, and record files vs metrics.

Usage: .venv/bin/python -I compare_metrics.py base_1 record_1 [base_2 record_2 ...]
Reads runs/<label>.metrics (and runs/<label>.dagr for record runs).
"""

import os
import re
import sys

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

# Instruments published from BatchMetrics (publish_metrics + publish_completed_batch_metrics).
BATCH_PREFIXES = (
    "maxserve_batch_", "maxserve_cache_", "maxserve_reqs_queued", "maxserve_dp_",
    "maxserve_spec_decode_", "maxserve_dkv_",
)


def parse(label):
    out = {}
    for line in open(os.path.join(HERE, "runs", f"{label}.metrics")):
        if line.startswith("#"):
            continue
        m = re.match(r"^(\S+?)(\{.*\})?\s+([0-9.eE+-]+|NaN)$", line.strip())
        if not m or not m.group(1).startswith(BATCH_PREFIXES) or "_bucket" in m.group(1):
            continue
        out[(m.group(1), m.group(2) or "")] = float(m.group(3))
    return out


def record_counts(label):
    import max._core_mojo  # noqa: F401
    import metrics_log
    from record_spec import NAMES

    path = os.path.join(HERE, "runs", f"{label}.dagr")
    data = open(path, "rb").read()
    if os.path.exists(path + ".len"):   # mmap log: pre-sized unless closed; trust the committed length
        committed = int.from_bytes(open(path + ".len", "rb").read(8), "little")
        rc_trimmed = len(data) == committed
        data = data[:committed]
    arr = np.frombuffer(data, dtype=np.uint8)
    start = metrics_log.records_start(arr.ctypes.data, len(data))
    records, end = metrics_log.decode_records(arr.ctypes.data, len(data), start)
    bt = NAMES.index("batch_type")
    return {
        "records": len(records),
        "CE": sum(1 for r in records if r[bt] == 0),
        "TG": sum(1 for r in records if r[bt] == 1),
        "complete_file": end == len(data),
        "bytes": len(data),
        "mmap_closed": rc_trimmed if os.path.exists(path + ".len") else None,
    }


def main():
    labels = sys.argv[1:]
    parsed = {lab: parse(lab) for lab in labels}
    keys = sorted(set().union(*[set(p) for p in parsed.values()]))
    base_keys = [set(parsed[lab]) for lab in labels if lab.startswith("base")]
    rec_keys = [set(parsed[lab]) for lab in labels if lab.startswith("record")]
    if base_keys and rec_keys:
        only_base = set.union(*base_keys) - set.union(*rec_keys)
        only_rec = set.union(*rec_keys) - set.union(*base_keys)
        print(f"series in base runs only: {sorted(only_base) or 'none'}")
        print(f"series in record runs only: {sorted(only_rec) or 'none'}")
    print()
    head = f"{'series':<78}" + "".join(f"{lab:>14}" for lab in labels)
    print(head)
    for k in keys:
        name, lab = k
        if not (name.endswith("_count") or name.endswith("_sum") or not name.endswith(("_created",))):
            continue
        if name.endswith("_created"):
            continue
        row = f"{(name + lab)[:78]:<78}"
        for label in labels:
            v = parsed[label].get(k)
            row += f"{'-' if v is None else f'{v:.6g}':>14}"
        print(row)
    print()
    for label in labels:
        if label.startswith("record"):
            rc = record_counts(label)
            p = parsed[label]
            ce = p.get(("maxserve_batch_size_count", '{batch_type="CE"}'), 0)
            tg = p.get(("maxserve_batch_size_count", '{batch_type="TG"}'), 0)
            print(f"{label}: record file {rc['records']} records (CE {rc['CE']}, TG {rc['TG']}), "
                  f"{rc['bytes']} bytes, ends on a record boundary: {rc['complete_file']}, "
                  f"mmap file trimmed at close: {rc['mmap_closed']} | "
                  f"Prometheus batch_size counts CE {ce:.0f}, TG {tg:.0f}")


if __name__ == "__main__":
    main()
