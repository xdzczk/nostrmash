#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

RUN_TS="$(date -u +"%Y%m%dT%H%M%SZ")"
GIT_SHA="$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")"
REF_NAME="${GITHUB_REF_NAME:-$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "local")}"
SAFE_REF_NAME="$(echo "${REF_NAME}" | tr '/' '-')"

OUTPUT_BASE="${PERF_OUTPUT_BASE:-benchmarks/history}"
RUN_ID="${PERF_RUN_ID:-${RUN_TS}-${SAFE_REF_NAME}-${GIT_SHA}}"
OUT_DIR="${OUTPUT_BASE}/${RUN_ID}"

BENCH_COUNT="${PERF_BENCH_COUNT:-3}"
SKIP_BENCHMARKS="${PERF_SKIP_BENCHMARKS:-0}"
INCLUDE_LOADTEST="${PERF_COLLECT_INCLUDE_LOADTEST:-0}"
LOADTEST_RESULTS_DIR="${PERF_COLLECT_LOADTEST_DIR:-loadtest/results}"
COLLECT_SCOPE="${PERF_COLLECT_SCOPE:-full}"

mkdir -p "${OUT_DIR}/benchmarks"

cat >"${OUT_DIR}/metadata.json" <<EOF
{
  "run_id": "${RUN_ID}",
  "collected_at_utc": "${RUN_TS}",
  "git_sha": "${GIT_SHA}",
  "ref_name": "${REF_NAME}",
  "bench_count": ${BENCH_COUNT},
  "collect_scope": "${COLLECT_SCOPE}",
  "skip_benchmarks": ${SKIP_BENCHMARKS},
  "include_loadtest": ${INCLUDE_LOADTEST}
}
EOF

if [[ "${SKIP_BENCHMARKS}" != "1" ]]; then
	if [[ "${COLLECT_SCOPE}" == "protected" ]]; then
		go test -run=^$ -bench='BenchmarkService(GetThreadWindow|GetUserInfos|GetEventBatch)$' -benchmem -count="${BENCH_COUNT}" ./internal/query | tee "${OUT_DIR}/benchmarks/benchmark-query-protected.txt"
		go test -run=^$ -bench='BenchmarkWSGatewayDispatchCacheCall(ThreadView|UserInfos)$' -benchmem -count="${BENCH_COUNT}" ./internal/api_primal | tee "${OUT_DIR}/benchmarks/benchmark-ws-protected.txt"
		go test -run=^$ -bench='BenchmarkLoadFixtureFile$' -benchmem -count="${BENCH_COUNT}" ./internal/replay | tee "${OUT_DIR}/benchmarks/benchmark-replay-protected.txt"
		go test -run=^$ -bench='BenchmarkDeriveEventReferences$' -benchmem -count="${BENCH_COUNT}" ./internal/derivation | tee "${OUT_DIR}/benchmarks/benchmark-derivation-protected.txt"
	else
		go test -run=^$ -bench=. -benchmem -count="${BENCH_COUNT}" ./internal/query ./internal/store ./internal/replay ./internal/derivation ./internal/api_primal | tee "${OUT_DIR}/benchmarks/benchmark-hot.txt"
		go test -run=^$ -bench=BenchmarkService -benchmem -count="${BENCH_COUNT}" ./internal/query | tee "${OUT_DIR}/benchmarks/benchmark-query.txt"
		go test -run=^$ -bench=BenchmarkWSGateway -benchmem -count="${BENCH_COUNT}" ./internal/api_primal | tee "${OUT_DIR}/benchmarks/benchmark-ws.txt"
		go test -run=^$ -bench=BenchmarkLoadFixtureFile -benchmem -count="${BENCH_COUNT}" ./internal/replay | tee "${OUT_DIR}/benchmarks/benchmark-replay.txt"
		go test -run=^$ -bench=BenchmarkDerive -benchmem -count="${BENCH_COUNT}" ./internal/derivation | tee "${OUT_DIR}/benchmarks/benchmark-derivation.txt"
	fi
fi

if [[ "${INCLUDE_LOADTEST}" == "1" ]]; then
	if [[ -d "${LOADTEST_RESULTS_DIR}" ]]; then
		mkdir -p "${OUT_DIR}/loadtest"
		cp -R "${LOADTEST_RESULTS_DIR}" "${OUT_DIR}/loadtest/results"
	else
		echo "requested load-test collection, but directory not found: ${LOADTEST_RESULTS_DIR}" >"${OUT_DIR}/loadtest-collection-warning.txt"
	fi
fi

cat >"${OUT_DIR}/README.txt" <<EOF
Performance snapshot: ${RUN_ID}

Collected:
- benchmark outputs under benchmarks/*.txt (unless PERF_SKIP_BENCHMARKS=1)
- benchmark scope: ${COLLECT_SCOPE}
- optional load-test results under loadtest/results (when PERF_COLLECT_INCLUDE_LOADTEST=1)

Comparison examples:
- benchstat ${OUTPUT_BASE}/<older-run>/benchmarks/benchmark-hot.txt ${OUT_DIR}/benchmarks/benchmark-hot.txt
- benchstat ${OUTPUT_BASE}/<older-run>/benchmarks/benchmark-query.txt ${OUT_DIR}/benchmarks/benchmark-query.txt

If benchstat is missing:
go install golang.org/x/perf/cmd/benchstat@latest
EOF

echo "perf collection complete: ${OUT_DIR}"
