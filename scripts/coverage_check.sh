#!/usr/bin/env bash

set -euo pipefail

# Pragmatic risk-focused coverage policy:
# - enforce minimum package coverage where runtime risk is highest
# - avoid vanity global thresholds
POLICY=(
  "./internal/query:25"
  "./internal/store:20"
  "./internal/api_primal:60"
)

failures=0

echo "Running coverage policy checks..."
for entry in "${POLICY[@]}"; do
  pkg="${entry%%:*}"
  min="${entry##*:}"

  output="$(go test -covermode=atomic "${pkg}" 2>&1)" || {
    echo "${output}"
    echo "FAIL ${pkg}: tests failed"
    failures=$((failures + 1))
    continue
  }

  echo "${output}"
  got="$(echo "${output}" | sed -n 's/.*coverage: \([0-9.][0-9.]*\)% of statements.*/\1/p' | tail -n1)"
  if [[ -z "${got}" ]]; then
    echo "FAIL ${pkg}: could not determine coverage percentage"
    failures=$((failures + 1))
    continue
  fi

  if awk -v got="${got}" -v min="${min}" 'BEGIN { exit !(got + 0 >= min + 0) }'; then
    echo "PASS ${pkg}: ${got}% >= ${min}%"
  else
    echo "FAIL ${pkg}: ${got}% < ${min}%"
    if [[ "${pkg}" == "./internal/store" && "${got}" == "0.0" ]]; then
      echo "hint: store coverage is usually meaningful when TEST_DATABASE_URL points to a running Postgres"
    fi
    failures=$((failures + 1))
  fi
done

if [[ "${failures}" -ne 0 ]]; then
  echo "Coverage policy failed (${failures} package check(s) under threshold)."
  exit 1
fi

echo "Coverage policy passed."
