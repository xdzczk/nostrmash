#!/usr/bin/env bash

# Advisory gate-recalibration reducer. Reads the append-only NDJSON histories
# produced by scripts/perf_history_append.sh (accumulated by the perf.yml
# baseline jobs) and prints suggested gate thresholds versus the currently
# configured ones. It NEVER edits any config — a human reads the recommendation
# and commits a new threshold if they agree.
#
# What it recommends:
#   - Load harness: an error-rate floor and factor derived from the observed
#     per-channel error-rate distribution, plus advisory p95/p99 run-to-run
#     volatility so operators can see whether the (deliberately advisory)
#     latency signal is stable enough to ever gate.
#   - Protected benchmarks: a per-benchmark regression percent at mean + 3σ of
#     the run-to-run ns/op deltas, and the max across benchmarks as the single
#     suggested PROTECTED_THRESHOLD_PCT.
#
# Statistics use only run-to-run *deltas* (how much a metric moves between
# consecutive main pushes), because the gates fire on movement, not on absolute
# level. mean + 3σ targets a ~0.1% false-positive rate on a normal-ish
# distribution; the script prints the sample size so a reader can discount
# recommendations built on too few runs.
#
# Usage:
#   scripts/gate_recalibrate.sh [loadtest_history.ndjson] [benchmark_history.ndjson]
#
# Env knobs (current gates, for the "vs configured" column):
#   LOADTEST_ERROR_RATE_FLOOR   default 0.01
#   LOADTEST_ERROR_RATE_FACTOR  default 2.0
#   PROTECTED_THRESHOLD_PCT     default 15
#   RECALIBRATE_MIN_SAMPLES     minimum deltas before a recommendation is firm
#                               (default 8; below this it is marked LOW-DATA)

set -euo pipefail

LOADTEST_HISTORY="${1:-loadtest/baseline/history.ndjson}"
BENCH_HISTORY="${2:-benchmarks/history/protection/history.ndjson}"

CUR_FLOOR="${LOADTEST_ERROR_RATE_FLOOR:-0.01}"
CUR_FACTOR="${LOADTEST_ERROR_RATE_FACTOR:-2.0}"
CUR_PCT="${PROTECTED_THRESHOLD_PCT:-15}"
MIN_SAMPLES="${RECALIBRATE_MIN_SAMPLES:-8}"

python3 - "${LOADTEST_HISTORY}" "${BENCH_HISTORY}" "${CUR_FLOOR}" "${CUR_FACTOR}" "${CUR_PCT}" "${MIN_SAMPLES}" <<'PY'
import json
import pathlib
import statistics
import sys

loadtest_path = pathlib.Path(sys.argv[1])
bench_path = pathlib.Path(sys.argv[2])
cur_floor = float(sys.argv[3])
cur_factor = float(sys.argv[4])
cur_pct = float(sys.argv[5])
min_samples = int(sys.argv[6])


def read_ndjson(path):
    rows = []
    if not path.is_file():
        return rows
    for raw in path.read_text().splitlines():
        raw = raw.strip()
        if not raw:
            continue
        try:
            rows.append(json.loads(raw))
        except ValueError:
            continue
    return rows


def mean_plus_3sigma(values):
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    mu = statistics.mean(values)
    sigma = statistics.pstdev(values)
    return mu + 3.0 * sigma


def low_data(n):
    return " (LOW-DATA)" if n < min_samples else ""


lines = []
lines.append("# Gate Recalibration (advisory)")
lines.append("")
lines.append(
    "Recommendations derived from run-to-run deltas in the committed perf/load "
    "histories. Nothing is changed automatically; commit a new threshold only "
    "if you agree. `mean + 3σ` targets ~0.1% false positives; sample sizes "
    f"below {min_samples} are flagged LOW-DATA."
)
lines.append("")

# ---- Load harness -------------------------------------------------------
load_rows = read_ndjson(loadtest_path)
lines.append(f"## Load harness — {len(load_rows)} runs in `{loadtest_path}`")
lines.append("")
if len(load_rows) < 2:
    lines.append("_Not enough runs yet to compute a distribution (need ≥2)._")
    lines.append("")
else:
    # Peak per-run error rate across channels; the gate is per-channel, but the
    # floor/factor are global, so the worst channel per run bounds them.
    peak_err = []
    for row in load_rows:
        channels = row.get("channels", {}) or {}
        rates = [float(v.get("error_rate", 0.0) or 0.0) for v in channels.values()]
        peak_err.append(max(rates) if rates else 0.0)

    observed_max = max(peak_err)
    observed_mean = statistics.mean(peak_err)
    # A floor a little above the observed worst run avoids gating on noise;
    # round up to a clean fraction.
    suggested_floor = max(0.005, round(observed_max * 1.5, 4))

    # Latency volatility (advisory): relative run-to-run p95/p99 deltas.
    def rel_deltas(metric):
        series = {}
        for row in load_rows:
            for name, ch in (row.get("channels", {}) or {}).items():
                series.setdefault(name, []).append(float(ch.get(metric, 0.0) or 0.0))
        out = []
        for values in series.values():
            for a, b in zip(values, values[1:]):
                if a > 0:
                    out.append(abs(b - a) / a * 100.0)
        return out

    p95_deltas = rel_deltas("p95_ms")
    p99_deltas = rel_deltas("p99_ms")

    lines.append(f"- Observed peak error rate: max **{observed_max*100:.3f}%**, mean {observed_mean*100:.3f}%.")
    lines.append(
        f"- Suggested error-rate floor: **{suggested_floor:.4f}** "
        f"(configured {cur_floor:.4f}); keep factor **{cur_factor:g}×** unless the "
        "floor is raised."
    )
    if p95_deltas:
        p95_band = mean_plus_3sigma(p95_deltas)
        lines.append(
            f"- p95 run-to-run volatility (advisory): mean+3σ ≈ **{p95_band:.1f}%**{low_data(len(p95_deltas))} "
            f"over {len(p95_deltas)} deltas — latency stays advisory until this is small and stable."
        )
    if p99_deltas:
        p99_band = mean_plus_3sigma(p99_deltas)
        lines.append(
            f"- p99 run-to-run volatility (advisory): mean+3σ ≈ **{p99_band:.1f}%**{low_data(len(p99_deltas))} "
            f"over {len(p99_deltas)} deltas."
        )
    lines.append("")

# ---- Protected benchmarks ----------------------------------------------
bench_rows = read_ndjson(bench_path)
lines.append(f"## Protected benchmarks — {len(bench_rows)} runs in `{bench_path}`")
lines.append("")
if len(bench_rows) < 2:
    lines.append("_Not enough runs yet to compute a distribution (need ≥2)._")
    lines.append("")
else:
    series = {}
    for row in bench_rows:
        for name, entry in (row.get("benchmarks", {}) or {}).items():
            ns = entry.get("ns_per_op")
            if ns is None:
                continue
            series.setdefault(name, []).append(float(ns))

    lines.append("| Benchmark | Runs | Δ mean+3σ | Suggested pct |")
    lines.append("| --- | ---: | ---: | ---: |")
    worst = 0.0
    for name in sorted(series):
        values = series[name]
        deltas = []
        for a, b in zip(values, values[1:]):
            if a > 0:
                deltas.append(abs(b - a) / a * 100.0)
        band = mean_plus_3sigma(deltas) if deltas else None
        if band is None:
            lines.append(f"| {name} | {len(values)} | n/a | n/a |")
            continue
        worst = max(worst, band)
        lines.append(f"| {name} | {len(values)} | {band:.1f}% | {band:.0f}%{low_data(len(deltas))} |")
    lines.append("")
    if worst > 0:
        suggested_pct = max(5.0, round(worst))
        verdict = "tighten" if suggested_pct < cur_pct else "hold/loosen"
        lines.append(
            f"- Suggested `PROTECTED_THRESHOLD_PCT`: **{suggested_pct:.0f}%** "
            f"(configured {cur_pct:.0f}%) → **{verdict}**. This is the max per-benchmark "
            "mean+3σ so no single benchmark trips the gate on normal noise."
        )
    lines.append("")

print("\n".join(lines))
PY
