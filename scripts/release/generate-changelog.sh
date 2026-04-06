#!/usr/bin/env bash
set -euo pipefail

VERSION="${1:-}"
OUTPUT_PATH="${2:-dist/changelog.md}"

if [[ -z "${VERSION}" ]]; then
  if [[ "${GITHUB_REF_TYPE:-}" == "tag" && -n "${GITHUB_REF_NAME:-}" ]]; then
    VERSION="${GITHUB_REF_NAME}"
  else
    VERSION="unreleased-$(git rev-parse --short=12 HEAD)"
  fi
fi

mkdir -p "$(dirname "${OUTPUT_PATH}")"

CURRENT_TAG=""
if git rev-parse "refs/tags/${VERSION}" >/dev/null 2>&1; then
  CURRENT_TAG="${VERSION}"
fi

PREVIOUS_TAG=""
if [[ -n "${CURRENT_TAG}" ]]; then
  PREVIOUS_TAG="$(git describe --tags --abbrev=0 "${CURRENT_TAG}^" 2>/dev/null || true)"
fi

if [[ -n "${CURRENT_TAG}" && -n "${PREVIOUS_TAG}" ]]; then
  RANGE="${PREVIOUS_TAG}..${CURRENT_TAG}"
elif [[ -n "${CURRENT_TAG}" ]]; then
  RANGE="${CURRENT_TAG}"
else
  LAST_TAG="$(git describe --tags --abbrev=0 2>/dev/null || true)"
  if [[ -n "${LAST_TAG}" ]]; then
    RANGE="${LAST_TAG}..HEAD"
  else
    RANGE="HEAD"
  fi
fi

TODAY_UTC="$(date -u +"%Y-%m-%d")"

{
  echo "## ${VERSION} (${TODAY_UTC})"
  echo
  echo "### Highlights"
  git log --no-merges --pretty=format:"- %s (%h)" "${RANGE}" || true
  echo
  echo
  echo "### Full Diff Context"
  if [[ -n "${PREVIOUS_TAG}" && -n "${CURRENT_TAG}" ]]; then
    echo "- Compare: ${PREVIOUS_TAG}...${CURRENT_TAG}"
  elif [[ -n "${CURRENT_TAG}" ]]; then
    echo "- Snapshot tag: ${CURRENT_TAG}"
  else
    echo "- Snapshot range: ${RANGE}"
  fi
} > "${OUTPUT_PATH}"
