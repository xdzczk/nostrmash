#!/usr/bin/env bash

set -euo pipefail

SCENARIO_NAME="ingest-throughput-pressure"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${SCRIPT_DIR}/../lib/common.sh"

require_cmd go
require_cmd python3

FIXTURE_SOURCE_PATH="${FIXTURE_SOURCE_PATH:-internal/replay/testdata/relay_payloads/basic_flow.ndjson}"
FIXTURE_REPEAT_COUNT="${FIXTURE_REPEAT_COUNT:-1000}"

OUT_DIR="$(create_result_dir "${SCENARIO_NAME}")"
RUN_LOG="${OUT_DIR}/ingestor_replay.log"
SUMMARY_TXT="${OUT_DIR}/summary.txt"
AMPLIFIED_FIXTURE_PATH="${OUT_DIR}/amplified_fixture.ndjson"

echo "scenario=${SCENARIO_NAME}" | tee "${SUMMARY_TXT}"
echo "fixture_source_path=${FIXTURE_SOURCE_PATH}" | tee -a "${SUMMARY_TXT}"
echo "fixture_repeat_count=${FIXTURE_REPEAT_COUNT}" | tee -a "${SUMMARY_TXT}"
echo "output_dir=${OUT_DIR}" | tee -a "${SUMMARY_TXT}"

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

line_count="$(python3 - "${AMPLIFIED_FIXTURE_PATH}" <<'PY'
import pathlib
import sys
path = pathlib.Path(sys.argv[1])
print(sum(1 for _ in path.open()))
PY
)"

start_ms="$(now_epoch_ms)"
set +e
INGESTOR_MODE=replay \
INGESTOR_REPLAY_FIXTURE_PATH="${AMPLIFIED_FIXTURE_PATH}" \
go run ./cmd/ingestor >"${RUN_LOG}" 2>&1
exit_code=$?
set -e
end_ms="$(now_epoch_ms)"

python3 - "${SUMMARY_TXT}" "${line_count}" "${start_ms}" "${end_ms}" "${exit_code}" <<'PY'
import sys

summary_path, lines, start_ms, end_ms, exit_code = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
elapsed = max((end_ms - start_ms) / 1000.0, 0.001)
rps = lines / elapsed
with open(summary_path, "a") as out:
    out.write(f"fixture_lines={lines}\n")
    out.write(f"elapsed_seconds={elapsed:.3f}\n")
    out.write(f"effective_input_lines_per_second={rps:.2f}\n")
    out.write(f"go_run_exit_code={exit_code}\n")
PY

echo "scenario completed: ${SCENARIO_NAME}"
echo "results: ${OUT_DIR}"
