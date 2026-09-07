#!/usr/bin/env bash
# Push HEAD to origin/$branch, rebasing if another job landed first.
# The Performance Snapshot baseline jobs both commit to main on the same
# push and otherwise lose the race with `! [rejected] main -> main`.
set -euo pipefail

branch="${1:-${GITHUB_REF_NAME:-main}}"

git config user.name "github-actions[bot]"
git config user.email "github-actions[bot]@users.noreply.github.com"

for attempt in 1 2 3 4 5; do
	if git push origin "HEAD:${branch}"; then
		exit 0
	fi
	echo "push rejected (attempt ${attempt}/5); rebasing onto origin/${branch}"
	git fetch origin "${branch}"
	git rebase "origin/${branch}"
done

echo "failed to push after rebasing onto origin/${branch}" >&2
exit 1
