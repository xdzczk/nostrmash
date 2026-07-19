#!/usr/bin/env bash

set -euo pipefail

# Pragmatic risk-focused coverage policy:
# - enforce minimum package coverage where runtime risk is highest
# - avoid vanity global thresholds
POLICY=(
  "./internal/api:38"
  "./internal/query:28"
  "./internal/store:22"
  "./internal/api_primal:60"
  # High-risk packages ratcheted up as real tests landed. Floors sit a few
  # points under the observed coverage so they can only be raised further as
  # more tests land, never spuriously fail on run-to-run variance. Measured
  # (Jul 2026): derivation 70.9% (DB-backed CI run), worker/runtime 32.5%.
  "./internal/derivation:65"
  "./internal/worker/runtime:30"
  "./internal/trust:15"
)

# Packages whose coverage is only meaningful with a live Postgres (they are
# thin wrappers over SQL). Their strict floor is skipped when TEST_DATABASE_URL
# is unset so local/non-integration runs don't fail spuriously; CI runs with a
# Postgres service and therefore enforces them.
DB_DEPENDENT=(
  "./internal/store"
  "./internal/derivation"
  "./internal/worker/runtime"
  "./internal/trust"
)

is_db_dependent() {
  local candidate="$1"
  local db_pkg
  for db_pkg in "${DB_DEPENDENT[@]}"; do
    if [[ "${db_pkg}" == "${candidate}" ]]; then
      return 0
    fi
  done
  return 1
}

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
  effective_min="${min}"
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

  if is_db_dependent "${pkg}" && [[ -z "${TEST_DATABASE_URL:-}" ]]; then
    effective_min="0"
    echo "INFO ${pkg}: TEST_DATABASE_URL is unset; skipping strict threshold (${min}%) for local/non-integration runs"
  fi

  if awk -v got="${got}" -v min="${effective_min}" 'BEGIN { exit !(got + 0 >= min + 0) }'; then
    echo "PASS ${pkg}: ${got}% >= ${effective_min}%"
  else
    echo "FAIL ${pkg}: ${got}% < ${effective_min}%"
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
