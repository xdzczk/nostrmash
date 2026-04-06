# Versioning

NostrMash uses semantic versioning intent:

- `MAJOR`: incompatible API/behavior changes requiring explicit migration by integrators/operators
- `MINOR`: backward-compatible feature additions and compatibility-surface expansion
- `PATCH`: backward-compatible fixes (correctness, security, performance, docs)

## Compatibility Expectations

- Native API contracts should remain stable within a major version.
- Compatibility surfaces (`/primal/v1`, `/primal/ws`) may evolve in phased increments, but breaking behavior should still be treated as major-version material.
- Operator-facing behavior/config changes should be documented in release notes and relevant docs.
- Deprecations should follow the documented overlap/removal process in `docs/compatibility.md` unless urgent safety/security concerns require faster action.

Maintainer confirmation item:

- confirm whether every compatibility-surface break is always `MAJOR`, or whether narrowly scoped compatibility exceptions are permitted with explicit release-notes sign-off.

Detailed compatibility/deprecation policy lives in `docs/compatibility.md`.
Schema evolution and rollback-aware migration guidance lives in `docs/migrations.md`.
Release execution flow lives in `RELEASE.md`.

## Release Tag Contract

- Release automation is tag-driven.
- Tags matching `v*` trigger `.github/workflows/release.yml`.
- Preferred stable tag format is `vMAJOR.MINOR.PATCH` (for example `v1.4.0`).
- Pre-release tags (for example `v1.5.0-rc.1`) are allowed when needed; treat them as non-stable signals in release communication.
- Pre-release tags still trigger tag-driven release automation; they should be clearly marked as pre-release in release notes and downstream operator communication.

If a change is not ready for an externally visible release, do not create a `v*` tag.

## Build Metadata

Runtime/build identity fields (`version`, `commit`, `build_time`) are operationally meaningful and should map cleanly to release tags.

## Practical Rule

If a change forces existing clients/operators to change behavior urgently, it is probably not a patch release.
