#!/usr/bin/env bash

set -euo pipefail

SCENARIO_NAME="worker-throughput-pressure"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${SCRIPT_DIR}/../lib/common.sh"

require_cmd curl
require_cmd python3

API_BASE_URL="${API_BASE_URL:-http://localhost:8080}"
WORKER_METRICS_URL="${WORKER_METRICS_URL:-http://localhost:9091/metrics}"
SAMPLE_SECONDS="${SAMPLE_SECONDS:-120}"
SAMPLE_EVERY_SECONDS="${SAMPLE_EVERY_SECONDS:-5}"
ADMIN_BEARER_TOKEN="${ADMIN_BEARER_TOKEN:-}"
DERIVATION_NAME="${DERIVATION_NAME:-}"
TRIGGER_REBUILD="${TRIGGER_REBUILD:-0}"

OUT_DIR="$(create_result_dir "${SCENARIO_NAME}")"
SAMPLES_CSV="${OUT_DIR}/worker_jobs_samples.csv"
SUMMARY_TXT="${OUT_DIR}/summary.txt"
REBUILD_RESPONSE_JSON="${OUT_DIR}/rebuild_trigger_response.json"

echo "scenario=${SCENARIO_NAME}" | tee "${SUMMARY_TXT}"
echo "api_base_url=${API_BASE_URL}" | tee -a "${SUMMARY_TXT}"
echo "worker_metrics_url=${WORKER_METRICS_URL}" | tee -a "${SUMMARY_TXT}"
echo "sample_seconds=${SAMPLE_SECONDS}" | tee -a "${SUMMARY_TXT}"
echo "sample_every_seconds=${SAMPLE_EVERY_SECONDS}" | tee -a "${SUMMARY_TXT}"
echo "output_dir=${OUT_DIR}" | tee -a "${SUMMARY_TXT}"

if [[ "${TRIGGER_REBUILD}" == "1" ]]; then
	if [[ -z "${ADMIN_BEARER_TOKEN}" || -z "${DERIVATION_NAME}" ]]; then
		echo "TRIGGER_REBUILD=1 requires ADMIN_BEARER_TOKEN and DERIVATION_NAME" | tee -a "${SUMMARY_TXT}"
		exit 1
	fi
fi

if [[ "${TRIGGER_REBUILD}" == "1" ]]; then
	echo "triggering rebuild for derivation=${DERIVATION_NAME}" | tee -a "${SUMMARY_TXT}"
	curl -sS -X POST "${API_BASE_URL}/admin/v1/rebuilds" \
		-H "Authorization: Bearer ${ADMIN_BEARER_TOKEN}" \
		-H "Content-Type: application/json" \
		-d "{\"derivation_name\":\"${DERIVATION_NAME}\",\"scope\":{\"type\":\"full\"}}" \
		>"${REBUILD_RESPONSE_JSON}" || true
fi

echo "timestamp_epoch,job_outcome,total_count" >"${SAMPLES_CSV}"
samples=$((SAMPLE_SECONDS / SAMPLE_EVERY_SECONDS))
if ((samples < 1)); then
	samples=1
fi

for _ in $(seq 1 "${samples}"); do
	ts="$(date +%s)"
	metrics_body="$(curl -sS "${WORKER_METRICS_URL}" || true)"
	if [[ -z "${metrics_body}" ]]; then
		echo "${ts},metrics_unreachable,0" >>"${SAMPLES_CSV}"
	else
		printf "%s\n" "${metrics_body}" | python3 - "${ts}" >>"${SAMPLES_CSV}" <<'PY'
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
	sleep "${SAMPLE_EVERY_SECONDS}"
done

python3 - "${SAMPLES_CSV}" "${SUMMARY_TXT}" <<'PY'
import csv
import sys
from collections import defaultdict

csv_path, summary_path = sys.argv[1], sys.argv[2]
series = defaultdict(list)
with open(csv_path, newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        series[row["job_outcome"]].append(int(row["total_count"]))

with open(summary_path, "a") as out:
    if not series:
        out.write("no_samples_recorded=true\n")
        sys.exit(0)
    for outcome in sorted(series.keys()):
        values = series[outcome]
        delta = values[-1] - values[0] if len(values) > 1 else 0
        out.write(f"outcome={outcome} first={values[0]} last={values[-1]} delta={delta}\n")
PY

echo "scenario completed: ${SCENARIO_NAME}"
echo "results: ${OUT_DIR}"
