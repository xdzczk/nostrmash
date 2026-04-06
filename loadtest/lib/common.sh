#!/usr/bin/env bash

set -euo pipefail

LOADTEST_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS_ROOT="${LOADTEST_RESULTS_ROOT:-${LOADTEST_ROOT}/results}"

require_cmd() {
	local cmd="$1"
	if ! command -v "${cmd}" >/dev/null 2>&1; then
		echo "missing required command: ${cmd}" >&2
		exit 1
	fi
}

timestamp_utc() {
	date -u +"%Y%m%dT%H%M%SZ"
}

create_result_dir() {
	local scenario="$1"
	local out_dir="${RESULTS_ROOT}/${scenario}-$(timestamp_utc)"
	mkdir -p "${out_dir}"
	echo "${out_dir}"
}

now_epoch_ms() {
	python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
}
