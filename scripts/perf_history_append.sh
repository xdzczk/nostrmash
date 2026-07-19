#!/usr/bin/env bash

# Append one compact NDJSON history line to a long-lived perf/load history file,
# pruned to the last HISTORY_MAX entries. This is the append-only companion to
# the overwritten `main-latest` baselines in .github/workflows/perf.yml: those
# capture only the newest run, while these accumulate the run-to-run
# distribution that scripts/gate_recalibrate.sh reduces into threshold
# recommendations.
#
# Subcommands:
#   loadtest   <summary.json> <history.ndjson>
#       Extract per-channel error_rate / p50 / p95 / p99 / throughput / total
#       from a WS+API harness summary.
#   benchmarks <baseline_dir>  <history.ndjson>
#       Extract per-benchmark median ns/op and allocs/op from the Go benchmark
#       *.txt files under <baseline_dir>/benchmarks.
#
# Env knobs:
#   HISTORY_MAX  entries to retain (default 90)
#   HISTORY_SHA  commit sha to stamp (default: $GITHUB_SHA or `git rev-parse`)
#   HISTORY_TS   ISO-8601 UTC timestamp (default: now)

set -euo pipefail

usage() {
	echo "usage: $0 {loadtest <summary.json> <history.ndjson> | benchmarks <baseline_dir> <history.ndjson>}" >&2
	exit 2
}

if [[ $# -ne 3 ]]; then
	usage
fi

MODE="$1"
SOURCE="$2"
HISTORY_FILE="$3"

HISTORY_MAX="${HISTORY_MAX:-90}"
if [[ -z "${HISTORY_SHA:-}" ]]; then
	HISTORY_SHA="${GITHUB_SHA:-$(git rev-parse HEAD 2>/dev/null || echo unknown)}"
fi
HISTORY_TS="${HISTORY_TS:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"

mkdir -p "$(dirname "${HISTORY_FILE}")"

python3 - "${MODE}" "${SOURCE}" "${HISTORY_FILE}" "${HISTORY_MAX}" "${HISTORY_SHA}" "${HISTORY_TS}" <<'PY'
import json
import pathlib
import re
import statistics
import sys

mode = sys.argv[1]
source = pathlib.Path(sys.argv[2])
history_file = pathlib.Path(sys.argv[3])
history_max = int(sys.argv[4])
sha = sys.argv[5]
ts = sys.argv[6]


def ns_to_ms(ns):
    try:
        return round(float(ns) / 1_000_000.0, 4)
    except (TypeError, ValueError):
        return 0.0


def build_loadtest_line():
    report = json.loads(source.read_text())
    channels = {}
    for ch in report.get("channels", []) or []:
        name = ch.get("name")
        if not name:
            continue
        lat = ch.get("latency", {}) or {}
        channels[name] = {
            "error_rate": float(ch.get("error_rate", 0.0) or 0.0),
            "p50_ms": ns_to_ms(lat.get("p50_ns", 0)),
            "p95_ms": ns_to_ms(lat.get("p95_ns", 0)),
            "p99_ms": ns_to_ms(lat.get("p99_ns", 0)),
            "throughput_rps": float(ch.get("throughput_rps", 0.0) or 0.0),
            "total": int(ch.get("total", 0) or 0),
        }
    return {"ts": ts, "sha": sha, "channels": channels}


# Matches "BenchmarkName-8   1234   567.8 ns/op   0 B/op   0 allocs/op".
NS_RE = re.compile(r'^(Benchmark\S+?)(?:-\d+)?\s+\d+\s+([\d.]+)\s+ns/op')
ALLOCS_RE = re.compile(r'([\d.]+)\s+allocs/op')


def build_benchmarks_line():
    bench_dir = source / "benchmarks"
    ns_samples = {}
    alloc_samples = {}
    if bench_dir.is_dir():
        for txt in sorted(bench_dir.glob("*.txt")):
            for raw in txt.read_text().splitlines():
                m = NS_RE.match(raw)
                if not m:
                    continue
                name = m.group(1)
                ns_samples.setdefault(name, []).append(float(m.group(2)))
                am = ALLOCS_RE.search(raw)
                if am:
                    alloc_samples.setdefault(name, []).append(float(am.group(1)))
    benchmarks = {}
    for name, samples in ns_samples.items():
        entry = {"ns_per_op": round(statistics.median(samples), 3)}
        allocs = alloc_samples.get(name)
        if allocs:
            entry["allocs_per_op"] = round(statistics.median(allocs), 3)
        benchmarks[name] = entry
    return {"ts": ts, "sha": sha, "benchmarks": benchmarks}


if mode == "loadtest":
    line = build_loadtest_line()
    payload_key = "channels"
elif mode == "benchmarks":
    line = build_benchmarks_line()
    payload_key = "benchmarks"
else:
    print(f"unknown mode: {mode}", file=sys.stderr)
    sys.exit(2)

if not line.get(payload_key):
    print(f"perf_history_append: no {payload_key} extracted from {source}; skipping", file=sys.stderr)
    sys.exit(0)

existing = []
if history_file.is_file():
    for raw in history_file.read_text().splitlines():
        raw = raw.strip()
        if not raw:
            continue
        try:
            existing.append(json.loads(raw))
        except ValueError:
            # Drop unparsable legacy lines rather than fail the whole append.
            continue

existing.append(line)
existing = existing[-history_max:]

with history_file.open("w") as out:
    for entry in existing:
        out.write(json.dumps(entry, separators=(",", ":")) + "\n")

print(f"perf_history_append: {mode} -> {history_file} ({len(existing)} entries retained)")
PY
