#!/usr/bin/env bash
set -euo pipefail

DIST_DIR="${DIST_DIR:-dist}"
VERSION="${VERSION:-}"

if [[ -z "${VERSION}" ]]; then
  if [[ "${GITHUB_REF_TYPE:-}" == "tag" && -n "${GITHUB_REF_NAME:-}" ]]; then
    VERSION="${GITHUB_REF_NAME}"
  elif git describe --tags --exact-match >/dev/null 2>&1; then
    VERSION="$(git describe --tags --exact-match)"
  else
    SHORT_SHA="$(git rev-parse --short=12 HEAD)"
    VERSION="dev-${SHORT_SHA}"
  fi
fi

rm -rf "${DIST_DIR}"
mkdir -p "${DIST_DIR}"

SERVICES=(api ingestor worker trust_worker)
GOOS_TARGET="linux"
GOARCH_TARGET="amd64"

for service in "${SERVICES[@]}"; do
  bin_name="${service}"
  build_path="${DIST_DIR}/${bin_name}"
  archive_name="${service}_${VERSION}_${GOOS_TARGET}_${GOARCH_TARGET}.tar.gz"

  CGO_ENABLED=0 GOOS="${GOOS_TARGET}" GOARCH="${GOARCH_TARGET}" \
    go build -trimpath -ldflags="-s -w" -o "${build_path}" "./cmd/${service}"

  tar -C "${DIST_DIR}" -czf "${DIST_DIR}/${archive_name}" "${bin_name}"
  rm -f "${build_path}"
done

(
  cd "${DIST_DIR}"
  shasum -a 256 ./*.tar.gz > sha256sums.txt
)

for forbidden in coverage.out coverage*.out *.coverprofile *.prof *.pprof *.trace *.log; do
  if compgen -G "${DIST_DIR}/${forbidden}" > /dev/null; then
    echo "unexpected generated artifact in ${DIST_DIR}: pattern ${forbidden}" >&2
    exit 1
  fi
done
