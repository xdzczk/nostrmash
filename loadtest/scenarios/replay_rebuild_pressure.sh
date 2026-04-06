#!/usr/bin/env bash

set -euo pipefail

SCENARIO_NAME="replay-rebuild-pressure"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${SCRIPT_DIR}/../lib/common.sh"

require_cmd curl
require_cmd go
require_cmd python3

API_BASE_URL="${API_BASE_URL:-http://localhost:8080}"
WORKER_METRICS_URL="${WORKER_METRICS_URL:-http://localhost:9091/metrics}"
ADMIN_BEARER_TOKEN="${ADMIN_BEARER_TOKEN:-}"
DERIVATION_NAME="${DERIVATION_NAME:-}"
FIXTURE_SOURCE_PATH="${FIXTURE_SOURCE_PATH:-internal/replay/testdata/relay_payloads/basic_flow.ndjson}"
FIXTURE_REPEAT_COUNT="${FIXTURE_REPEAT_COUNT:-300}"
POLL_SECONDS="${POLL_SECONDS:-90}"
POLL_EVERY_SECONDS="${POLL_EVERY_SECONDS:-5}"

OUT_DIR="$(create_result_dir "${SCENARIO_NAME}")"
SUMMARY_TXT="${OUT_DIR}/summary.txt"
REPLAY_LOG="${OUT_DIR}/replay.log"
AMPLIFIED_FIXTURE_PATH="${OUT_DIR}/amplified_fixture.ndjson"
REBUILD_RESPONSE_JSON="${OUT_DIR}/rebuild_response.json"
REBUILDS_SNAPSHOTS="${OUT_DIR}/rebuilds_snapshots.ndjson"
WORKER_SAMPLES_CSV="${OUT_DIR}/worker_jobs_samples.csv"

echo "scenario=${SCENARIO_NAME}" | tee "${SUMMARY_TXT}"
echo "api_base_url=${API_BASE_URL}" | tee -a "${SUMMARY_TXT}"
echo "worker_metrics_url=${WORKER_METRICS_URL}" | tee -a "${SUMMARY_TXT}"
echo "fixture_source_path=${FIXTURE_SOURCE_PATH}" | tee -a "${SUMMARY_TXT}"
echo "fixture_repeat_count=${FIXTURE_REPEAT_COUNT}" | tee -a "${SUMMARY_TXT}"
echo "poll_seconds=${POLL_SECONDS}" | tee -a "${SUMMARY_TXT}"
echo "poll_every_seconds=${POLL_EVERY_SECONDS}" | tee -a "${SUMMARY_TXT}"
echo "output_dir=${OUT_DIR}" | tee -a "${SUMMARY_TXT}"

if [[ -z "${ADMIN_BEARER_TOKEN}" || -z "${DERIVATION_NAME}" ]]; then
	echo "ADMIN_BEARER_TOKEN and DERIVATION_NAME are required for replay/rebuild pressure scenario." >&2
	exit 1
fi
if [[ ! -f "${FIXTURE_SOURCE_PATH}" ]]; then
	echo "fixture not found: ${FIXTURE_SOURCE_PATH}" >&2
	exit 1
fi

python3 - "${FIXTURE_SOURCE_PATH}" "${FIXTURE_REPEAT_COUNT}" "${AMPLIFIED_FIXTURE_PATH}" <<'PY'
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

echo "phase=replay" | tee -a "${SUMMARY_TXT}"
replay_start_ms="$(now_epoch_ms)"
set +e
INGESTOR_MODE=replay \
INGESTOR_REPLAY_FIXTURE_PATH="${AMPLIFIED_FIXTURE_PATH}" \
go run ./cmd/ingestor >"${REPLAY_LOG}" 2>&1
replay_exit_code=$?
set -e
replay_end_ms="$(now_epoch_ms)"

echo "phase=rebuild_trigger" | tee -a "${SUMMARY_TXT}"
curl -sS -X POST "${API_BASE_URL}/admin/v1/rebuilds" \
	-H "Authorization: Bearer ${ADMIN_BEARER_TOKEN}" \
	-H "Content-Type: application/json" \
	-d "{\"derivation_name\":\"${DERIVATION_NAME}\",\"scope\":{\"type\":\"full\"}}" \
	>"${REBUILD_RESPONSE_JSON}" || true

echo "timestamp_epoch,job_outcome,total_count" >"${WORKER_SAMPLES_CSV}"
sample_count=$((POLL_SECONDS / POLL_EVERY_SECONDS))
if ((sample_count < 1)); then
	sample_count=1
fi

for _ in $(seq 1 "${sample_count}"); do
	ts="$(date +%s)"

	curl -sS -H "Authorization: Bearer ${ADMIN_BEARER_TOKEN}" "${API_BASE_URL}/admin/v1/rebuilds?limit=20" >"${OUT_DIR}/rebuilds-${ts}.json" || true
	printf '{"timestamp_epoch":%s,"snapshot_file":"rebuilds-%s.json"}\n' "${ts}" "${ts}" >>"${REBUILDS_SNAPSHOTS}"

	metrics_body="$(curl -sS "${WORKER_METRICS_URL}" || true)"
	if [[ -z "${metrics_body}" ]]; then
		echo "${ts},metrics_unreachable,0" >>"${WORKER_SAMPLES_CSV}"
	else
		printf "%s\n" "${metrics_body}" | python3 - "${ts}" >>"${WORKER_SAMPLES_CSV}" <<'PY'
import re
import sys

ts = sys.argv[1]
outcome_totals = {}
pattern = re.compile(r'^nostrmash_worker_jobs_total\{[^}]*outcome="([^"]+)"[^}]*\}\s+([0-9.eE+-]+)$')

for raw in sys.stdin:
    line = raw.strip()
    m = pattern.match(line)
    if not m:
        continue
    outcome = m.group(1)
    value = float(m.group(2))
    outcome_totals[outcome] = outcome_totals.get(outcome, 0.0) + value

if not outcome_totals:
    print(f"{ts},no_worker_counters,0")
else:
    for outcome, total in sorted(outcome_totals.items()):
        print(f"{ts},{outcome},{int(total)}")
PY
	fi

	sleep "${POLL_EVERY_SECONDS}"
done

python3 - "${SUMMARY_TXT}" "${replay_start_ms}" "${replay_end_ms}" "${replay_exit_code}" "${WORKER_SAMPLES_CSV}" <<'PY'
import csv
import sys
from collections import defaultdict

summary_path, replay_start_ms, replay_end_ms, replay_exit_code, samples_csv = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), sys.argv[5]
elapsed = max((replay_end_ms - replay_start_ms) / 1000.0, 0.001)
series = defaultdict(list)
with open(samples_csv, newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        series[row["job_outcome"]].append(int(row["total_count"]))

with open(summary_path, "a") as out:
    out.write(f"replay_elapsed_seconds={elapsed:.3f}\n")
    out.write(f"replay_exit_code={replay_exit_code}\n")
    for outcome in sorted(series.keys()):
        vals = series[outcome]
        delta = vals[-1] - vals[0] if len(vals) > 1 else 0
        out.write(f"worker_outcome={outcome} first={vals[0]} last={vals[-1]} delta={delta}\n")
PY

echo "scenario completed: ${SCENARIO_NAME}"
echo "results: ${OUT_DIR}"
