#!/usr/bin/env bash

set -euo pipefail

SCENARIO_NAME="api-read-pressure"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${SCRIPT_DIR}/../lib/common.sh"

require_cmd curl
require_cmd python3

BASE_URL="${BASE_URL:-http://localhost:8080}"
CONCURRENCY="${CONCURRENCY:-8}"
REQUESTS_TOTAL="${REQUESTS_TOTAL:-240}"
REQUEST_TIMEOUT_SECONDS="${REQUEST_TIMEOUT_SECONDS:-5}"

# Defaults point at the checked-in replay fixture identifiers.
EVENT_ID="${EVENT_ID:-c108c0bfe77ffc3c0e07f1056d0b5d008e2b4e2a8c4197af5b8c7e3582d41f74}"
PUBKEY="${PUBKEY:-37ce94259421d17a13e04382205c6061323ebc6bbfa46aab1f73e6f93c774a5e}"

EVENT_BATCH_IDS="${EVENT_BATCH_IDS:-${EVENT_ID}}"
PROFILE_BATCH_PUBKEYS="${PROFILE_BATCH_PUBKEYS:-${PUBKEY}}"

OUT_DIR="$(create_result_dir "${SCENARIO_NAME}")"
RAW_CSV="${OUT_DIR}/requests.csv"
SUMMARY_TXT="${OUT_DIR}/summary.txt"

echo "scenario=${SCENARIO_NAME}" | tee "${SUMMARY_TXT}"
echo "base_url=${BASE_URL}" | tee -a "${SUMMARY_TXT}"
echo "concurrency=${CONCURRENCY}" | tee -a "${SUMMARY_TXT}"
echo "requests_total=${REQUESTS_TOTAL}" | tee -a "${SUMMARY_TXT}"
echo "request_timeout_seconds=${REQUEST_TIMEOUT_SECONDS}" | tee -a "${SUMMARY_TXT}"
echo "output_dir=${OUT_DIR}" | tee -a "${SUMMARY_TXT}"
echo "endpoint,status,time_seconds" >"${RAW_CSV}"

run_one_request() {
	local idx="$1"
	local endpoint=""
	local method=""
	local payload=""
	local url=""
	local curl_out=""
	local time_and_status=""

	case $((idx % 5)) in
	0)
		endpoint="/api/v1/events/${EVENT_ID}"
		method="GET"
		;;
	1)
		endpoint="/api/v1/profiles/${PUBKEY}"
		method="GET"
		;;
	2)
		endpoint="/api/v1/threads/${EVENT_ID}"
		method="GET"
		;;
	3)
		endpoint="/api/v1/events/batch"
		method="POST"
		payload="$(python3 - <<PY
import json
ids = [x.strip() for x in "${EVENT_BATCH_IDS}".split(",") if x.strip()]
print(json.dumps({"ids": ids}))
PY
)"
		;;
	4)
		endpoint="/api/v1/profiles/batch"
		method="POST"
		payload="$(python3 - <<PY
import json
pubkeys = [x.strip() for x in "${PROFILE_BATCH_PUBKEYS}".split(",") if x.strip()]
print(json.dumps({"pubkeys": pubkeys}))
PY
)"
		;;
	esac

	url="${BASE_URL}${endpoint}"
	if [[ "${method}" == "GET" ]]; then
		time_and_status="$(curl -sS -o /dev/null -m "${REQUEST_TIMEOUT_SECONDS}" -w "%{time_total},%{http_code}" "${url}" || true)"
	else
		time_and_status="$(curl -sS -o /dev/null -m "${REQUEST_TIMEOUT_SECONDS}" -w "%{time_total},%{http_code}" -H "Content-Type: application/json" -X POST "${url}" -d "${payload}" || true)"
	fi

	if [[ -z "${time_and_status}" ]]; then
		echo "${endpoint},000,${REQUEST_TIMEOUT_SECONDS}" >>"${RAW_CSV}"
		return
	fi

	curl_out="${time_and_status}"
	echo "${endpoint},${curl_out#*,},${curl_out%%,*}" >>"${RAW_CSV}"
}

export -f run_one_request
export BASE_URL REQUEST_TIMEOUT_SECONDS EVENT_ID PUBKEY EVENT_BATCH_IDS PROFILE_BATCH_PUBKEYS RAW_CSV

start_ms="$(now_epoch_ms)"
seq 1 "${REQUESTS_TOTAL}" | xargs -P "${CONCURRENCY}" -n 1 bash -c 'run_one_request "$@"' _
end_ms="$(now_epoch_ms)"

python3 - "${RAW_CSV}" "${SUMMARY_TXT}" "${start_ms}" "${end_ms}" <<'PY'
import csv
import statistics
import sys
from collections import defaultdict

csv_path, summary_path, start_ms, end_ms = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
rows = []
with open(csv_path, newline="") as f:
    reader = csv.DictReader(f)
    rows = list(reader)

if not rows:
    with open(summary_path, "a") as out:
        out.write("no_requests_recorded=true\n")
    sys.exit(0)

duration_s = max((end_ms - start_ms) / 1000.0, 0.001)
times = [float(r["time_seconds"]) for r in rows]
status_ok = sum(1 for r in rows if r["status"].startswith("2"))
status_429 = sum(1 for r in rows if r["status"] == "429")
status_5xx = sum(1 for r in rows if r["status"].startswith("5"))
rps = len(rows) / duration_s

per_endpoint = defaultdict(list)
for r in rows:
    per_endpoint[r["endpoint"]].append(float(r["time_seconds"]))

def p95(values):
    if len(values) == 1:
        return values[0]
    return statistics.quantiles(values, n=100, method="inclusive")[94]

with open(summary_path, "a") as out:
    out.write(f"duration_seconds={duration_s:.3f}\n")
    out.write(f"throughput_rps={rps:.2f}\n")
    out.write(f"requests_recorded={len(rows)}\n")
    out.write(f"status_2xx={status_ok}\n")
    out.write(f"status_429={status_429}\n")
    out.write(f"status_5xx={status_5xx}\n")
    out.write(f"latency_p50_seconds={statistics.median(times):.4f}\n")
    out.write(f"latency_p95_seconds={p95(times):.4f}\n")
    for endpoint, vals in sorted(per_endpoint.items()):
        out.write(
            f"endpoint={endpoint} count={len(vals)} p50={statistics.median(vals):.4f}s p95={p95(vals):.4f}s\n"
        )
PY

echo "scenario completed: ${SCENARIO_NAME}"
echo "results: ${OUT_DIR}"
