#!/usr/bin/env bash

set -euo pipefail

SCENARIO_NAME="ws-api-pressure"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${SCRIPT_DIR}/../lib/common.sh"

require_cmd go

BASE_URL="${BASE_URL:-http://localhost:8080}"
WS_URL="${WS_URL:-}"
DURATION="${DURATION:-30s}"
WARMUP="${WARMUP:-2s}"
REQUEST_TIMEOUT="${REQUEST_TIMEOUT:-10s}"
WS_CLIENTS="${WS_CLIENTS:-16}"
API_CLIENTS="${API_CLIENTS:-16}"
SCENARIO_FILE="${SCENARIO_FILE:-}"

# Fixture identifiers default to the checked-in replay fixture IDs so runs
# against a seeded database exercise populated read paths.
PUBKEY="${PUBKEY:-37ce94259421d17a13e04382205c6061323ebc6bbfa46aab1f73e6f93c774a5e}"
EVENT_ID="${EVENT_ID:-c108c0bfe77ffc3c0e07f1056d0b5d008e2b4e2a8c4197af5b8c7e3582d41f74}"
QUERY="${QUERY:-nostr}"
HASHTAG="${HASHTAG:-nostr}"

OUT_DIR="$(create_result_dir "${SCENARIO_NAME}")"
SUMMARY_JSON="${OUT_DIR}/summary.json"
SUMMARY_TXT="${OUT_DIR}/summary.txt"

args=(
	-base-url "${BASE_URL}"
	-duration "${DURATION}"
	-warmup "${WARMUP}"
	-timeout "${REQUEST_TIMEOUT}"
	-ws-clients "${WS_CLIENTS}"
	-api-clients "${API_CLIENTS}"
	-pubkey "${PUBKEY}"
	-event-id "${EVENT_ID}"
	-query "${QUERY}"
	-hashtag "${HASHTAG}"
	-out "${SUMMARY_JSON}"
)
if [[ -n "${WS_URL}" ]]; then
	args+=(-ws-url "${WS_URL}")
fi
if [[ -n "${SCENARIO_FILE}" ]]; then
	args+=(-scenario "${SCENARIO_FILE}")
fi

echo "scenario=${SCENARIO_NAME}" | tee "${SUMMARY_TXT}"
echo "base_url=${BASE_URL}" | tee -a "${SUMMARY_TXT}"
echo "ws_clients=${WS_CLIENTS} api_clients=${API_CLIENTS} duration=${DURATION}" | tee -a "${SUMMARY_TXT}"

cd "${SCRIPT_DIR}/../.."
go run ./loadtest/harness "${args[@]}" | tee -a "${SUMMARY_TXT}"

echo "json_summary=${SUMMARY_JSON}"
