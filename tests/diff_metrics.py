"""Diff two Prometheus exposition files by series, ignoring formatting and line order.

Usage: .venv/bin/python -I tests/diff_metrics.py <max.metrics> <mojo.metrics>
Skips python_* (prometheus_client's own collectors) and *_created (process start timestamps).
"""
import re, sys

def parse(path):
    out, types = {}, {}
    for line in open(path):
        line = line.strip()
        m = re.match(r"^# TYPE (\S+) (\S+)$", line)
        if m:
            types[m.group(1)] = m.group(2)
            continue
        if line.startswith("#") or not line:
            continue
        m = re.match(r"^(\S+?)(\{.*\})?\s+([0-9.eE+-]+|NaN|\+Inf)$", line)
        if not m:
            print("unparsed:", line[:120])
            continue
        name = m.group(1)
        if name.startswith("python_") or name.endswith("_created"):
            continue
        labels = m.group(2) or ""
        labels = "{" + ",".join(sorted(labels.strip("{}").split(","))) + "}" if labels else ""
        out[(name, labels)] = float(m.group(3))
    return out, types

a, ta = parse(sys.argv[1])
b, tb = parse(sys.argv[2])
only_a = sorted(set(a) - set(b))
only_b = sorted(set(b) - set(a))
def close(x, y):   # float summation order differs; compare with a relative tolerance
    return x == y or abs(x - y) <= 1e-9 * max(abs(x), abs(y))

diff = [(k, a[k], b[k]) for k in sorted(set(a) & set(b)) if not close(a[k], b[k])]
near = sum(1 for k in set(a) & set(b) if a[k] != b[k] and close(a[k], b[k]))
fam_a = {k[0].rsplit("_bucket", 1)[0].rsplit("_sum", 1)[0].rsplit("_count", 1)[0] for k in a}
fam_b = {k[0].rsplit("_bucket", 1)[0].rsplit("_sum", 1)[0].rsplit("_count", 1)[0] for k in b}
print(f"{sys.argv[1]}: {len(a)} series / {len(fam_a)} families")
print(f"{sys.argv[2]}: {len(b)} series / {len(fam_b)} families")
print(f"equal: {len(set(a) & set(b)) - len(diff) - near}   within 1e-9: {near}   differing: {len(diff)}   "
      f"only in first: {len(only_a)}   only in second: {len(only_b)}")
for k in only_a[:12]:
    print("  only in first :", k)
for k in only_b[:12]:
    print("  only in second:", k)
for k, va, vb in diff[:15]:
    print(f"  {k[0]}{k[1]}: {va} vs {vb}")
