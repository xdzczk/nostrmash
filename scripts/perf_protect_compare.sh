#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
	echo "usage: $0 <baseline_dir> <current_dir> [output_dir]" >&2
	exit 1
fi

BASELINE_DIR="$1"
CURRENT_DIR="$2"
OUTPUT_DIR="${3:-benchmarks/compare}"

if [[ ! -d "${BASELINE_DIR}/benchmarks" ]]; then
	echo "baseline benchmarks directory not found: ${BASELINE_DIR}/benchmarks" >&2
	exit 1
fi
if [[ ! -d "${CURRENT_DIR}/benchmarks" ]]; then
	echo "current benchmarks directory not found: ${CURRENT_DIR}/benchmarks" >&2
	exit 1
fi

mkdir -p "${OUTPUT_DIR}"

THRESHOLD_PCT="${REGRESSION_THRESHOLD_PCT:-15}"
MODE="${ENFORCEMENT_MODE:-advisory}"
FAIL_ON_REGRESSION="${FAIL_ON_REGRESSION:-0}"

SUMMARY_MD="${OUTPUT_DIR}/protected-summary.md"
SUMMARY_JSON="${OUTPUT_DIR}/protected-summary.json"

python3 - "${BASELINE_DIR}" "${CURRENT_DIR}" "${SUMMARY_MD}" "${SUMMARY_JSON}" "${THRESHOLD_PCT}" "${MODE}" <<'PY'
import json
import pathlib
import re
import statistics
import sys

baseline_dir = pathlib.Path(sys.argv[1])
current_dir = pathlib.Path(sys.argv[2])
summary_md = pathlib.Path(sys.argv[3])
summary_json = pathlib.Path(sys.argv[4])
threshold = float(sys.argv[5])
mode = sys.argv[6]

targets = [
    "BenchmarkServiceGetThreadWindow",
    "BenchmarkServiceGetUserInfos",
    "BenchmarkServiceGetEventBatch",
    "BenchmarkWSGatewayDispatchCacheCallThreadView",
    "BenchmarkWSGatewayDispatchCacheCallUserInfos",
    "BenchmarkLoadFixtureFile",
    "BenchmarkDeriveEventReferences",
]

pattern = re.compile(r'^(Benchmark\S+?)(?:-\d+)?\s+\d+\s+([\d.]+)\s+ns/op')

def load_metrics(snapshot_dir: pathlib.Path):
    rows = {}
    for path in sorted((snapshot_dir / "benchmarks").glob("*.txt")):
        for line in path.read_text().splitlines():
            match = pattern.match(line.strip())
            if not match:
                continue
            rows.setdefault(match.group(1), []).append(float(match.group(2)))
    return rows

old = load_metrics(baseline_dir)
new = load_metrics(current_dir)
rows = []
regressions = []

for name in targets:
    if name not in old or name not in new:
        rows.append({"benchmark": name, "status": "missing"})
        continue
    old_median = statistics.median(old[name])
    new_median = statistics.median(new[name])
    delta_pct = ((new_median - old_median) / old_median) * 100 if old_median else 0.0
    row = {
        "benchmark": name,
        "status": "ok",
        "baseline_ns_op": old_median,
        "current_ns_op": new_median,
        "delta_pct": delta_pct,
    }
    rows.append(row)
    if delta_pct >= threshold:
        regressions.append(row)

md_lines = [
    "# Protected Benchmark Comparison",
    "",
    f"- Mode: `{mode}`",
    f"- Regression threshold: `{threshold:.1f}%` on median `ns/op`",
    f"- Baseline snapshot: `{baseline_dir}`",
    f"- Current snapshot: `{current_dir}`",
    "",
    "| Benchmark | Baseline ns/op | Current ns/op | Delta |",
    "| --- | ---: | ---: | ---: |",
]

for row in rows:
    if row["status"] == "missing":
        md_lines.append(f"| `{row['benchmark']}` | n/a | n/a | missing |")
        continue
    md_lines.append(
        f"| `{row['benchmark']}` | {row['baseline_ns_op']:.2f} | {row['current_ns_op']:.2f} | {row['delta_pct']:+.2f}% |"
    )

if regressions:
    md_lines.append("")
    md_lines.append("## Regressions Requiring Investigation")
    for row in regressions:
        md_lines.append(f"- `{row['benchmark']}` delta `{row['delta_pct']:+.2f}%`")
else:
    md_lines.append("")
    md_lines.append("No protected benchmark crossed the configured regression threshold.")

summary_md.write_text("\n".join(md_lines) + "\n")
summary_json.write_text(json.dumps({"rows": rows, "regressions": regressions}, indent=2))

print(f"regression_count={len(regressions)}")
print("comparison_ready=true")
PY

REGRESSION_COUNT="$(python3 - "${SUMMARY_JSON}" <<'PY'
import json
import sys
path = sys.argv[1]
data = json.load(open(path))
print(len(data.get("regressions", [])))
PY
)"

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
	{
		echo "regression_count=${REGRESSION_COUNT}"
		echo "comparison_ready=true"
	} >>"${GITHUB_OUTPUT}"
fi

echo "wrote protected benchmark comparison:"
echo "  ${SUMMARY_MD}"
echo "  ${SUMMARY_JSON}"

if [[ "${FAIL_ON_REGRESSION}" == "1" && "${REGRESSION_COUNT}" != "0" ]]; then
	echo "protected benchmark regressions exceeded threshold (${THRESHOLD_PCT}%)." >&2
	exit 2
fi
