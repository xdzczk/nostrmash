#!/usr/bin/env bash

# Compare a current WS+API load-harness summary against a committed baseline.
#
# Gating policy (mirrors the Tier 1 Remediation Plan): fail ONLY on error-rate
# regression; latency (p95/p99) comparison is advisory to avoid runner-variance
# flakes. A channel regresses when, versus the baseline channel of the same
# name, its error rate exceeds the absolute floor (default 1%) AND more than
# doubles (default 2x). Both conditions must hold so that tiny absolute moves
# near zero and large-but-still-negligible rates do not trip the gate.
#
# Usage:
#   scripts/loadtest_compare.sh <baseline_summary.json> <current_summary.json> [output_dir]
#
# Env knobs:
#   LOADTEST_ERROR_RATE_FLOOR   absolute error-rate floor (fraction), default 0.01
#   LOADTEST_ERROR_RATE_FACTOR  multiplicative regression factor, default 2.0
#   FAIL_ON_REGRESSION          "1" to exit non-zero on regression, default 1

set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
	echo "usage: $0 <baseline_summary.json> <current_summary.json> [output_dir]" >&2
	exit 1
fi

BASELINE_JSON="$1"
CURRENT_JSON="$2"
OUTPUT_DIR="${3:-loadtest/compare}"

if [[ ! -f "${CURRENT_JSON}" ]]; then
	echo "current summary not found: ${CURRENT_JSON}" >&2
	exit 1
fi

mkdir -p "${OUTPUT_DIR}"

ERROR_RATE_FLOOR="${LOADTEST_ERROR_RATE_FLOOR:-0.01}"
ERROR_RATE_FACTOR="${LOADTEST_ERROR_RATE_FACTOR:-2.0}"
FAIL_ON_REGRESSION="${FAIL_ON_REGRESSION:-1}"

SUMMARY_MD="${OUTPUT_DIR}/loadtest-summary.md"
SUMMARY_JSON="${OUTPUT_DIR}/loadtest-summary.json"

set +e
python3 - "${BASELINE_JSON}" "${CURRENT_JSON}" "${SUMMARY_MD}" "${SUMMARY_JSON}" "${ERROR_RATE_FLOOR}" "${ERROR_RATE_FACTOR}" <<'PY'
import json
import pathlib
import sys

baseline_path = pathlib.Path(sys.argv[1])
current_path = pathlib.Path(sys.argv[2])
summary_md = pathlib.Path(sys.argv[3])
summary_json = pathlib.Path(sys.argv[4])
error_floor = float(sys.argv[5])
error_factor = float(sys.argv[6])


def load(path):
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def channels_by_name(report):
    out = {}
    if not report:
        return out
    for ch in report.get("channels", []) or []:
        name = ch.get("name")
        if name:
            out[name] = ch
    return out


def ns_to_ms(ns):
    try:
        return float(ns) / 1_000_000.0
    except (TypeError, ValueError):
        return 0.0


def pct_delta(base, cur):
    if base <= 0:
        return None
    return (cur - base) / base * 100.0


current = load(current_path)
baseline = load(baseline_path)

cur_channels = channels_by_name(current)
base_channels = channels_by_name(baseline)

rows = []
regressions = []
advisories = []

for name in sorted(cur_channels):
    cur = cur_channels[name]
    base = base_channels.get(name)

    cur_err = float(cur.get("error_rate", 0.0) or 0.0)
    cur_p95 = ns_to_ms(cur.get("latency", {}).get("p95_ns", 0))
    cur_p99 = ns_to_ms(cur.get("latency", {}).get("p99_ns", 0))

    row = {
        "channel": name,
        "current_error_rate": cur_err,
        "current_p95_ms": round(cur_p95, 3),
        "current_p99_ms": round(cur_p99, 3),
        "total": cur.get("total", 0),
        "regressed": False,
    }

    if base is None:
        row["note"] = "no baseline channel; advisory only"
        rows.append(row)
        continue

    base_err = float(base.get("error_rate", 0.0) or 0.0)
    base_p95 = ns_to_ms(base.get("latency", {}).get("p95_ns", 0))
    base_p99 = ns_to_ms(base.get("latency", {}).get("p99_ns", 0))

    row["baseline_error_rate"] = base_err
    row["baseline_p95_ms"] = round(base_p95, 3)
    row["baseline_p99_ms"] = round(base_p99, 3)
    row["p95_delta_pct"] = pct_delta(base_p95, cur_p95)
    row["p99_delta_pct"] = pct_delta(base_p99, cur_p99)

    # Gating condition: error rate is above the absolute floor AND more than
    # `error_factor` times the baseline (baseline of 0 => any rate above the
    # floor counts as a doubling).
    over_floor = cur_err > error_floor
    threshold = base_err * error_factor
    over_factor = cur_err > threshold if base_err > 0 else over_floor
    if over_floor and over_factor:
        row["regressed"] = True
        regressions.append(
            f"{name}: error_rate {cur_err*100:.2f}% (baseline {base_err*100:.2f}%, "
            f"floor {error_floor*100:.2f}%, factor {error_factor:g}x)"
        )

    for label, delta in (("p95", row["p95_delta_pct"]), ("p99", row["p99_delta_pct"])):
        if delta is not None and delta > 25.0:
            advisories.append(f"{name}: {label} +{delta:.1f}% ({label} latency, advisory)")

    rows.append(row)

result = {
    "gating": "error_rate_only",
    "error_rate_floor": error_floor,
    "error_rate_factor": error_factor,
    "has_baseline": baseline is not None,
    "regression_count": len(regressions),
    "regressions": regressions,
    "advisories": advisories,
    "channels": rows,
}
summary_json.write_text(json.dumps(result, indent=2) + "\n")

lines = []
lines.append("# WS + API Load Harness Comparison")
lines.append("")
if baseline is None:
    lines.append("> No committed baseline found; error-rate gating is advisory only for this run.")
    lines.append("")
lines.append(f"Gating: **error-rate only** (floor {error_floor*100:.2f}%, factor {error_factor:g}x). Latency is advisory.")
lines.append("")
lines.append("| Channel | Total | Error rate | Baseline err | p95 (ms) | p95 Δ | p99 (ms) | p99 Δ | Status |")
lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :---: |")
for row in rows:
    def fmt_pct(v):
        return "n/a" if v is None else f"{v:+.1f}%"
    base_err = row.get("baseline_error_rate")
    lines.append(
        "| {ch} | {total} | {err:.2f}% | {berr} | {p95:.2f} | {p95d} | {p99:.2f} | {p99d} | {status} |".format(
            ch=row["channel"],
            total=row["total"],
            err=row["current_error_rate"] * 100,
            berr=("n/a" if base_err is None else f"{base_err*100:.2f}%"),
            p95=row["current_p95_ms"],
            p95d=fmt_pct(row.get("p95_delta_pct")),
            p99=row["current_p99_ms"],
            p99d=fmt_pct(row.get("p99_delta_pct")),
            status=("REGRESSED" if row["regressed"] else "ok"),
        )
    )
lines.append("")
if regressions:
    lines.append("## Error-rate regressions (gating)")
    for r in regressions:
        lines.append(f"- {r}")
    lines.append("")
if advisories:
    lines.append("## Latency advisories (non-gating)")
    for a in advisories:
        lines.append(f"- {a}")
    lines.append("")
summary_md.write_text("\n".join(lines) + "\n")

print("\n".join(lines))
sys.exit(1 if regressions else 0)
PY
compare_status=$?
set -e

echo "loadtest-compare: wrote ${SUMMARY_MD} and ${SUMMARY_JSON}"

if [[ "${compare_status}" -ne 0 ]]; then
	if [[ "${FAIL_ON_REGRESSION}" == "1" ]]; then
		echo "loadtest-compare: error-rate regression detected; failing." >&2
		exit 1
	fi
	echo "loadtest-compare: error-rate regression detected but FAIL_ON_REGRESSION!=1; advisory only." >&2
fi
exit 0
