#!/usr/bin/env bash

# Run every Fuzz* target in the WS/API surface for a short, bounded budget.
#
# Go can only drive one fuzz target per `go test -fuzz` invocation, so this
# enumerates the targets per package with `go test -list` and runs each
# sequentially with `-fuzztime`. Any crash fails the whole run (a fuzz crash is
# a real bug); crashers are written under the package's testdata/fuzz corpus by
# the Go toolchain so they can be replayed and committed as regression seeds.
#
# Env knobs:
#   FUZZTIME     per-target budget (default 20s)
#   FUZZ_PKGS    space-separated packages to scan (default: the WS/API packages)

set -euo pipefail

FUZZTIME="${FUZZTIME:-20s}"
FUZZ_PKGS="${FUZZ_PKGS:-./internal/api ./internal/api_primal}"

failures=0
ran=0

for pkg in ${FUZZ_PKGS}; do
	# `go test -list` prints one identifier per line plus a trailing "ok" status
	# line; keep only names that look like fuzz targets.
	targets="$(go test -list '^Fuzz' "${pkg}" 2>/dev/null | grep -E '^Fuzz' || true)"
	if [[ -z "${targets}" ]]; then
		echo "no fuzz targets in ${pkg}"
		continue
	fi
	while IFS= read -r target; do
		[[ -z "${target}" ]] && continue
		ran=$((ran + 1))
		echo "==> fuzzing ${pkg} ${target} (-fuzztime ${FUZZTIME})"
		if ! go test "${pkg}" -run '^$' -fuzz "^${target}\$" -fuzztime "${FUZZTIME}"; then
			echo "!! fuzz target failed: ${pkg} ${target}" >&2
			failures=$((failures + 1))
		fi
	done <<<"${targets}"
done

echo "fuzz summary: ran=${ran} failures=${failures}"
if [[ "${failures}" -gt 0 ]]; then
	exit 1
fi
