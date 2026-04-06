#!/usr/bin/env bash

set -euo pipefail

# Pragmatic risk-focused coverage policy:
# - enforce minimum package coverage where runtime risk is highest
# - avoid vanity global thresholds
POLICY=(
  "./internal/api:35"
  "./internal/query:25"
  "./internal/store:20"
  "./internal/api_primal:60"
)

failures=0

profile="${COVERAGE_PROFILE:-}"
if [[ -z "${profile}" && -f "coverage.out" ]]; then
  profile="coverage.out"
fi

coverage_from_profile() {
  local pkg="$1"
  local profile_file="$2"
  local pkg_path="${pkg#./}"

  awk -v pkg_path="${pkg_path}" '
  BEGIN {
    covered = 0
    total = 0
  }
  NR == 1 { next }
  {
    split($1, path_and_range, ":")
    file = path_and_range[1]
    if (file == pkg_path || index(file, pkg_path "/") == 1 || index(file, "/" pkg_path "/") > 0) {
      stmts = $2 + 0
      count = $3 + 0
      total += stmts
      if (count > 0) {
        covered += stmts
      }
    }
  }
  END {
    if (total == 0) {
      print ""
      exit 0
    }
    printf "%.1f", (covered * 100.0) / total
  }
  ' "${profile_file}"
}

echo "Running coverage policy checks..."
for entry in "${POLICY[@]}"; do
  pkg="${entry%%:*}"
  min="${entry##*:}"
  got=""

  if [[ -n "${profile}" && -f "${profile}" ]]; then
    got="$(coverage_from_profile "${pkg}" "${profile}")"
    if [[ -n "${got}" ]]; then
      echo "INFO ${pkg}: using coverage from ${profile}"
    fi
  fi

  if [[ -z "${got}" ]]; then
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
