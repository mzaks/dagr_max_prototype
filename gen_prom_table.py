"""Generate prom_table.mojo from MAX's own instrument definitions.

For every instrument in SERVE_METRICS: the Prometheus family name the OTel exporter derives
(dots to underscores, unit suffix, `_total` for counters), its kind, and — for histograms — the
explicit bucket boundaries from HISTOGRAM_BUCKETS_BY_METRIC. Every generated family name is
verified against a real scrape, so a naming rule that drifts is caught here rather than in a diff.

Run: .venv/bin/python -I gen_prom_table.py runs/meas_base_2.metrics
"""

import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import max._core_mojo  # noqa: F401,E402
from opentelemetry.metrics import (  # noqa: E402
    Counter,
    Histogram,
    ObservableGauge,
    UpDownCounter,
)

from max.serve.telemetry.common import HISTOGRAM_BUCKETS_BY_METRIC  # noqa: E402
from max.serve.telemetry.metrics import HISTOGRAM_SHADOW_SUFFIX, SERVE_METRICS  # noqa: E402

# The OTel Prometheus exporter appends a suffix derived from the instrument's unit
# ("ms" -> "_milliseconds"). Rather than hard-code that table, learn it from a real scrape:
# every instrument that appears there reveals its unit's suffix, and the rest reuse it.
SEED_SUFFIX = {"": "", "1": ""}


def kind_of(inst) -> str:
    # SERVE_METRICS holds proxy instruments (created before the meter provider is set), so
    # match on the class name rather than the concrete SDK types.
    cls = type(inst).__name__.lstrip("_").replace("Proxy", "")
    if "Histogram" in cls:
        return "histogram"
    if "UpDownCounter" in cls:
        return "sum_gauge"          # deltas summed, exported as a gauge
    if "Gauge" in cls:
        return "gauge"              # last value wins
    if "Counter" in cls:
        return "counter"
    raise TypeError(f"unhandled instrument type {type(inst)}")


def learn_suffixes(rows, scrape: dict[str, str]) -> dict[str, str]:
    """unit -> the word the exporter appends, learned from the instruments in the scrape.

    The exporter does not append the unit when the name already ends with it
    (`maxserve.batch_input_tokens` with unit `tokens` stays as it is), so an empty match is
    only evidence when the base already ends in that word.
    """
    learned = dict(SEED_SUFFIX)
    for name, kind, unit, _, _ in rows:
        base = name.replace(".", "_")
        tail = "_total" if kind == "counter" else ""
        for fam, fam_kind in scrape.items():
            if fam_kind != ("gauge" if kind == "sum_gauge" else kind) \
                    or not fam.startswith(base) or not fam.endswith(tail):
                continue
            word = fam[len(base):len(fam) - len(tail) if tail else len(fam)].lstrip("_")
            if not word:
                # The name already carries the unit; take the word from the name itself so
                # instruments with the same unit that are NOT in this scrape still resolve.
                guess = unit.replace("/", "_per_").replace("%", "percent")
                if guess and base.endswith("_" + guess):
                    learned.setdefault(unit, guess)
                continue
            if unit in learned and learned[unit] and learned[unit] != word:
                raise AssertionError(f"unit {unit!r}: {learned[unit]!r} vs {word!r} ({name})")
            learned[unit] = word
    return learned


GUESSED: set[str] = set()


def family_name(name: str, unit: str, kind: str, suffixes: dict[str, str]) -> str:
    base = name.replace(".", "_")
    if unit in suffixes:
        word = suffixes[unit]
    else:
        # No instrument with this unit was recorded in the scrape (vision / video / dKV in a
        # text-only run), so fall back to the exporter's own transformation of the unit.
        word = unit.replace("/", "_per_").replace("%", "percent")
        GUESSED.add(unit)
    if word and not base.endswith("_" + word):
        base += "_" + word
    return base + "_total" if kind == "counter" else base


def scrape_families(path: str) -> dict[str, str]:
    out = {}
    for line in open(path):
        m = re.match(r"^# TYPE (\S+) (\S+)$", line.strip())
        if m:
            out[m.group(1)] = m.group(2)
    return out


def main() -> None:
    scrape = scrape_families(sys.argv[1])
    raw = []
    for name, inst in sorted(SERVE_METRICS.items()):
        if name.endswith(HISTOGRAM_SHADOW_SUFFIX):
            continue                       # OTLP-only shadow histograms
        kind = kind_of(inst)
        unit = getattr(inst, "unit", None) or getattr(inst, "_unit", None) or ""
        buckets = list(HISTOGRAM_BUCKETS_BY_METRIC.get(name, ())) if kind == "histogram" else []
        if kind == "histogram" and not buckets:
            raise AssertionError(f"histogram without explicit buckets: {name}")
        desc = (getattr(inst, "description", None) or getattr(inst, "_description", None) or "")
        desc = desc.replace("\\", "\\\\").replace('"', '\\"')
        raw.append((name, kind, unit, buckets, desc))

    suffixes = learn_suffixes(raw, scrape)
    rows = [(n, family_name(n, u, k, suffixes), k, b, d, u) for n, k, u, b, d in raw]
    in_scrape = [f for _, f, _, _, _, _ in rows if f in scrape]
    wrong = [(f, k, scrape[f]) for _, f, k, _, _, _ in rows
             if f in scrape and scrape[f] != ("gauge" if k == "sum_gauge" else k)]
    print(f"{len(rows)} instruments; {len(in_scrape)} of the scrape's {len(scrape)} families "
          f"matched; units learned: {sorted(suffixes)}")
    assert not wrong, f"kind mismatch vs the scrape: {wrong}"
    if GUESSED:
        print(f"  units not present in the scrape, suffix guessed: {sorted(GUESSED)}")
    unmatched = [f for f in scrape if f not in {r[1] for r in rows} and not f.startswith("python_")]
    if unmatched:
        print("  scrape families with no instrument (check the naming rule):", unmatched)

    L = [
        "# GENERATED by gen_prom_table.py from MAX's SERVE_METRICS — do not edit.",
        '"""The Prometheus family, kind and bucket layout of every MAX serve instrument."""',
        "",
        "",
        "@fieldwise_init",
        "struct Instrument(Copyable, Movable):",
        "    var otel_name: String",
        "    var family: String",
        "    var unit: String       # OTel unit, carried through to OTLP",
        "    var kind: Int          # 0 counter, 1 gauge, 2 histogram",
        "    var buckets: List[Float64]",
        "    var help: String",
        "",
        "",
        "comptime KIND_COUNTER = 0",
        "comptime KIND_GAUGE = 1",
        "comptime KIND_HISTOGRAM = 2",
        "comptime KIND_SUM_GAUGE = 3",   # UpDownCounter: deltas summed, TYPE gauge",
        "",
        "",
        "def instruments() -> List[Instrument]:",
        "    var out = List[Instrument]()",
    ]
    kinds = {"counter": "KIND_COUNTER", "gauge": "KIND_GAUGE", "histogram": "KIND_HISTOGRAM",
             "sum_gauge": "KIND_SUM_GAUGE"}
    for name, fam, kind, buckets, desc, unit in rows:
        blist = "[" + ", ".join(f"{float(b)!r}" for b in buckets) + "]" if buckets \
            else "List[Float64]()"
        L.append(f'    out.append(Instrument("{name}", "{fam}", "{unit}", {kinds[kind]}, {blist}, "{desc}"))')
    L += ["    return out^", ""]
    path = os.path.join(HERE, "prom_table.mojo")
    with open(path, "w") as fh:
        fh.write("\n".join(L))
    print(f"wrote {path} ({len(rows)} instruments)")


if __name__ == "__main__":
    main()
