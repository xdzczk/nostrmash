# Contributing

Use this page as the contributor entrypoint. It is the shortest path from local boot to safe PR, and it tells you which deeper docs to open next based on the kind of change you are making.

## Start here

| If your change touches... | Read this first | Then use |
| --- | --- | --- |
| architecture or ownership boundaries | `docs/architecture.md` | `docs/architecture/orchestration-surfaces.md` |
| config or operator behavior | `docs/configuration.md` | `docs/operations.md` |
| API or compatibility behavior | `docs/api.md` | `docs/testing.md`, `docs/compatibility.md` |
| performance-sensitive paths | `docs/performance.md` | `docs/testing.md` |
| schema or rollout posture | `docs/migrations.md` | `RELEASE.md`, `VERSIONING.md` |
| release and policy work | `RELEASE.md` | `SECURITY.md`, `VERSIONING.md` |

## Local start

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

## Before opening a PR

Run this baseline sequence locally for CI parity:

```bash
make ci
```

`make ci` runs the same blocking gates as CI (`fmt-check`, `imports-check`, `lint`, `mod-verify`, `vulncheck`, `test-race-policy`, `cover`, `coverage-policy`, `contract-drift`, `configdoc-check`, `build`).

Blocking race policy currently covers `internal/jobs`, `internal/store`, `internal/ingestor/...`, `internal/api_primal`, `cmd/worker`, and `internal/api`.

If formatting/import checks fail:

```bash
make format
```

If your change is focused and not all checks are needed initially, use targeted guidance in `docs/testing.md` and finish with `make ci` before PR review.

### Example workflow: shipping an API change safely

If you add or change an API behavior, a safe path usually looks like this:

1. Boot locally with `docker compose up --build` or your normal `go run` workflow.
2. Make the change and update `docs/api.md` plus `docs/openapi.yaml` in the same branch.
3. Run the relevant handler or compatibility tests first for fast feedback.
4. Run `make contract-drift` to ensure route and OpenAPI ownership still line up.
5. Finish with `make ci` before opening the PR.
6. If the change affects operators, also update `docs/operations.md` and call that out in the PR description.

## Architecture change expectations

- Keep ingest truth durable first; do not move product-shaping logic into canonical storage paths.
- Keep read orchestration in `internal/query` and data access in `internal/store`.
- Keep compatibility behavior isolated at transport boundaries (`internal/api_primal`).
- If you introduce or change a projection, keep rebuild behavior explicit and testable.

If your change affects boundaries or orchestration ownership, update `docs/architecture.md` and `docs/architecture/orchestration-surfaces.md`.

## Change-type playbook

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

## Documentation expectations

When behavior or operator workflows change, update docs in the same PR:

- API/contract behavior: `docs/api.md` and `docs/openapi.yaml` (plus `make contract-drift`)
- runtime/incident behavior: `docs/operations.md`
- testing/perf workflows: `docs/testing.md`, `docs/performance.md`
- contributor/release/security policy changes: `CONTRIBUTING.md`, `RELEASE.md`, `SECURITY.md`, `VERSIONING.md`

For migration/compatibility behavior changes, also update:

- `docs/migrations.md`
- `docs/compatibility.md`

## Formatting, imports, and docs style

Formatting and import ordering are blocking CI checks:

```bash
make fmt-check
make imports-check
```

To fix formatting and imports locally:

```bash
make format
```

This applies:

- `gofmt -w .`
- `goimports -w .` (via `go run golang.org/x/tools/cmd/goimports@latest`)

Documentation changes should follow the same style system as the rest of the repo:

- use sentence case for headings
- keep intros short and purposeful
- prefer tables for comparisons and routing decisions
- use numbered lists for workflows and walkthroughs
- use mermaid only when a diagram truly reduces cognitive load
- avoid long undifferentiated bullet walls and oversized related-doc footers

When in doubt, write the shortest version that still helps a contributor choose, decide, or act.

## CI gates

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

For higher-cost confidence checks (for example broad `-race` sweeps), use the advisory `Deep Confidence` workflow. It is intentionally non-blocking for normal PR flow.

## Useful optional checks

- Targeted fuzzing and benchmark commands: `docs/testing.md`
- Load/perf snapshots and regression checks: `docs/performance.md`
