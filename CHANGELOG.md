# Changelog

This changelog is generated from git history by `scripts/release/generate-changelog.sh` and is used as the GitHub Release body in `.github/workflows/release.yml` for tag releases (`v*`).

## Format

Each release entry includes:

- version tag and release date
- highlights (non-merge commits in release range)
- full diff context (`previous_tag...current_tag` when available)

## Latest

For the latest shipped notes, see GitHub Releases. Keep this file as policy/reference unless maintainers choose to append generated entries.

`build.yml` also produces a changelog preview artifact (`dist/changelog-preview.md`) for non-release validation runs; treat that as preview evidence, not a published release note.
