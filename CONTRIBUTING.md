# Contributing

Practical contributor workflow for NostrMash: start locally, keep changes reviewable, and preserve the raw-truth/rebuildable-read-model boundary.

Use this page as the contributor entrypoint, then jump to deep docs by change type:

- architecture and ownership: `docs/architecture.md`, `docs/architecture/orchestration-surfaces.md`
- config/env behavior: `docs/configuration.md`
- quality/testing gates: `docs/testing.md`
- operations/triage expectations: `docs/operations.md`, `docs/observability.md`
- performance validation: `docs/performance.md`
- compatibility/deprecations: `docs/compatibility.md`
- migration safety: `docs/migrations.md`
- release/versioning process: `RELEASE.md`, `VERSIONING.md`

## Local Start

Use either full containers or host `go run` workflow:

```bash
docker compose up --build
```

or:

```bash
cp .env.example .env
set -a && source .env && set +a
go run ./cmd/api
go run ./cmd/ingestor
go run ./cmd/worker
```

For details and replay mode, see `docs/development.md`.

## Before Opening A PR

Run this baseline sequence locally for CI parity:

```bash
make ci
```

`make ci` runs the same blocking gates as CI (`fmt-check`, `imports-check`, `lint`, `mod-verify`, `vulncheck`, `test-race-policy`, `cover`, `coverage-policy`, `contract-drift`, `configdoc-check`, `build`).

If formatting/import checks fail:

```bash
make format
```

If your change is focused and not all checks are needed initially, use targeted guidance in `docs/testing.md` and finish with `make ci` before PR review.

## Architecture Change Expectations

- Keep ingest truth durable first; do not move product-shaping logic into canonical storage paths.
- Keep read orchestration in `internal/query` and data access in `internal/store`.
- Keep compatibility behavior isolated at transport boundaries (`internal/api_primal`).
- If you introduce or change a projection, keep rebuild behavior explicit and testable.

If your change affects boundaries or orchestration ownership, update `docs/architecture.md` and `docs/architecture/orchestration-surfaces.md`.

## Change-Type Playbook

Use this as a safe default for what to update and validate:

- **API or compatibility behavior**
  - update contracts/docs: `docs/api.md`, `docs/openapi.yaml`, `docs/compatibility.md`
  - run checks: `make contract-drift`, relevant tests in `docs/testing.md`
- **Schema, rebuild, or queue/store semantics**
  - update safety notes: `docs/migrations.md`, `RELEASE.md` rollback notes
  - run checks: `make test`, `make test-race-policy`, `make coverage-policy`
- **Config/env or operator behavior**
  - update docs: `docs/configuration.md`, `docs/operations.md`
  - validate: local boot + `/health` + `/ready` + affected admin endpoints
- **Perf-sensitive paths**
  - update evidence notes as needed: `docs/performance.md`
  - run benchmarks/load tests relevant to touched hot paths

## Documentation Expectations

When behavior or operator workflows change, update docs in the same PR:

- API/contract behavior: `docs/api.md` and `docs/openapi.yaml` (plus `make contract-drift`)
- runtime/incident behavior: `docs/operations.md`
- testing/perf workflows: `docs/testing.md`, `docs/performance.md`
- contributor/release/security policy changes: `CONTRIBUTING.md`, `RELEASE.md`, `SECURITY.md`, `VERSIONING.md`

For migration/compatibility behavior changes, also update:

- `docs/migrations.md`
- `docs/compatibility.md`

## CI Gates

CI blocks on:

- `make fmt-check`
- `make imports-check`
- `make lint`
- `go mod verify`
- `make vulncheck`
- `make cover`
- `make coverage-policy`
- `make test-race-policy`
- `make contract-drift`
- `make rules-check`
- `make configdoc-check`
- `make build`

Detailed rationale and local variants (fuzz, benchmarks, load tests) are in `docs/testing.md`.

## Useful Optional Checks

- Targeted fuzzing and benchmark commands: `docs/testing.md`
- Load/perf snapshots and regression checks: `docs/performance.md`
