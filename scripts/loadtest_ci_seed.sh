#!/usr/bin/env bash

# CI helper for the WS+API load-harness jobs in .github/workflows/perf.yml.
#
# Splits the shared lifecycle into subcommands so the baseline (main) and
# compare (PR) jobs seed and drive identical backends:
#
#   wait       block until Postgres is healthy
#   seed       amplify the replay fixture and ingest it (INGESTOR_MODE=replay),
#              which also applies migrations so the API has a populated schema
#   start-api  launch cmd/api in the background and wait for /health
#   stop-api   terminate the background API server (best-effort)
#
# Env inputs (with CI defaults):
#   DATABASE_URL              Postgres DSN (required for seed/start-api)
#   LOADTEST_FIXTURE_SOURCE   ndjson relay payload to amplify
#   LOADTEST_FIXTURE_REPEAT   how many times to repeat the fixture
#   GITHUB_ENV                when set, start-api records API_PID there

set -euo pipefail

FIXTURE_SOURCE="${LOADTEST_FIXTURE_SOURCE:-internal/replay/testdata/relay_payloads/basic_flow.ndjson}"
FIXTURE_REPEAT="${LOADTEST_FIXTURE_REPEAT:-300}"
PG_HOST="${LOADTEST_PG_HOST:-localhost}"
PG_PORT="${LOADTEST_PG_PORT:-5432}"
PG_USER="${LOADTEST_PG_USER:-nostrmash}"
PG_DB="${LOADTEST_PG_DB:-nostrmash}"
API_HEALTH_URL="${LOADTEST_API_HEALTH_URL:-http://localhost:8080/health}"

cmd_wait() {
	for _ in $(seq 1 30); do
		if pg_isready -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d "${PG_DB}"; then
			echo "Postgres is healthy"
			return 0
		fi
		sleep 1
	done
	echo "Postgres did not become healthy in time" >&2
	return 1
}

cmd_seed() {
	if [[ ! -f "${FIXTURE_SOURCE}" ]]; then
		echo "fixture not found: ${FIXTURE_SOURCE}" >&2
		return 1
	fi
	local amplified
	amplified="$(mktemp -t loadtest-fixture.XXXXXX.ndjson)"
	python3 - "${FIXTURE_SOURCE}" "${FIXTURE_REPEAT}" "${amplified}" <<'PY'
import pathlib
import sys

src = pathlib.Path(sys.argv[1])
repeat = int(sys.argv[2])
dst = pathlib.Path(sys.argv[3])
payload = src.read_text()
with dst.open("w") as out:
    for _ in range(repeat):
        out.write(payload)
PY
	echo "seeding Postgres via replay (${FIXTURE_SOURCE} x${FIXTURE_REPEAT})"
	# The ingestor applies migrations on startup, so replay both builds the
	# schema and populates read paths the harness exercises.
	INGESTOR_MODE=replay \
	INGESTOR_REPLAY_FIXTURE_PATH="${amplified}" \
	go run ./cmd/ingestor
	rm -f "${amplified}"
}

cmd_start_api() {
	go run ./cmd/api > api-server.log 2>&1 &
	local pid=$!
	if [[ -n "${GITHUB_ENV:-}" ]]; then
		echo "API_PID=${pid}" >> "${GITHUB_ENV}"
	fi
	for _ in $(seq 1 60); do
		if curl -sf "${API_HEALTH_URL}" >/dev/null; then
			echo "API is healthy (pid ${pid})"
			return 0
		fi
		sleep 1
	done
	echo "API did not become healthy in time" >&2
	cat api-server.log || true
	return 1
}

cmd_stop_api() {
	if [[ -n "${API_PID:-}" ]]; then
		kill "${API_PID}" 2>/dev/null || true
	fi
	# go run spawns the compiled server as a child; sweep any stragglers so the
	# runner does not leave the port bound for later jobs.
	pkill -f 'cmd/api' 2>/dev/null || true
	return 0
}

main() {
	local sub="${1:-}"
	case "${sub}" in
	wait) cmd_wait ;;
	seed) cmd_seed ;;
	start-api) cmd_start_api ;;
	stop-api) cmd_stop_api ;;
	*)
		echo "usage: $0 {wait|seed|start-api|stop-api}" >&2
		return 2
		;;
	esac
}

main "$@"
