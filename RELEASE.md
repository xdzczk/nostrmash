# Release Guide

This is the lightweight release flow for NostrMash. Keep it predictable; avoid ceremony.

Release artifact security metadata (SBOM, signatures, attestation foundations, and verification commands) is documented in [docs/release-security.md](docs/release-security.md).
Migration safety expectations are documented in [docs/migrations.md](docs/migrations.md), and external behavior compatibility/deprecation expectations are documented in [docs/compatibility.md](docs/compatibility.md).
Operational rollout and alert-response references are in [docs/operations.md](docs/operations.md) and [docs/observability.md](docs/observability.md).

## High-Level Flow

1. Cut a release candidate from `main`.
2. Run quality + operational validation (below).
3. Create and push a version tag (`v*`, for example `v0.8.2`).
4. GitHub Actions `release.yml` runs automatically from that tag and:
   - builds release binaries and bundle checksums
   - builds/pushes the container image to GHCR
   - generates SBOM and release changelog
   - signs security metadata and emits provenance attestation foundations
   - publishes GitHub release assets and notes
5. Monitor post-release health and be ready to roll back.

Changelog generation source: `scripts/release/generate-changelog.sh` (consumed by `.github/workflows/release.yml`).

Manual release workflow runs:

- `workflow_dispatch` is available for rehearsal/evidence collection.
- Manual runs still build/sign/attest and upload workflow artifacts.
- Publishing GitHub Release assets and pushing GHCR image tags remains tag-gated (`refs/tags/v*`).

## Validation Expectations Before Release

Minimum checks:

- `make ci`
- `make contract-drift` (already included by `make ci`; keep explicit if running targeted release checks outside full CI wrapper)
- targeted perf validation:
  - `make benchmark-protected`
  - run manual perf snapshot/comparison workflow if hot paths changed
- operator sanity:
  - `/health`, `/ready`, `/metrics` pass in staging-like environment
  - admin inspection routes needed for operations are healthy

If release touches compatibility-heavy surfaces, also run representative load scenarios from `loadtest/README.md`.

Maintainer confirmation item:

- define when release-candidate perf checks should be strictly blocking (for example, always for `release_candidate` or only for high-risk slices) and keep that decision aligned with `.github/workflows/perf.yml` inputs.

If release includes schema migrations or compatibility-surface changes, include an explicit risk note in release PR/release notes covering:

- rollout order and staging expectations
- rollback posture (binary-only vs requires DB/operator actions)
- deprecation window (if behavior is being phased out)

## Rollback Expectations

Release notes should include:

- previous stable version tag
- any migration/config assumptions
- whether rollback is binary-only or requires operator action

Operational rollback references:

- health and triage flow: `docs/operations.md`
- rebuild/version recovery surfaces: `GET /admin/v1/rebuilds`, `GET /admin/v1/derivation-versions`

If rollback occurs, capture root cause and follow-up actions in the next patch release notes.
