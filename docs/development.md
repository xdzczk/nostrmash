# Development

This page is the contributor workflow reference for NostrMash. Use the top-level [README](../README.md) for first boot; use this document for day-to-day local work, tests, migrations, replay, and safe projection changes.

## On This Page

- [Fast Path](#fast-path)
- [Common Loops](#common-loops)
- [Migrations](#migrations)
- [Running Tests](#running-tests)
- [Adding a New Projection Safely](#adding-a-new-projection-safely)
- [Codebase Conventions](#codebase-conventions)
- [How Not To Break The Architecture](#how-not-to-break-the-architecture)

## Fast Path

Prerequisites:

- Go `1.25`
- Docker and Docker Compose
- Either the Compose-managed Postgres on `localhost:5432` for local `go run` / `go test`, or the full container stack via `docker compose up --build`

Fast path:

```bash
cp .env.example .env
set -a
source .env
set +a
```

Important local DB note:

- `.env.example` points at `localhost:5432`, which matches the checked-in Compose Postgres service.
- The checked-in `docker-compose.yml` publishes the `postgres` service to the host, so `docker compose up -d postgres` is enough for local `go run`, `go test`, and `make ci` commands.
- If you want the simplest boot path, run the full stack in containers with `docker compose up --build`.

Run services in separate terminals:

```bash
go run ./cmd/api
go run ./cmd/ingestor
go run ./cmd/worker
```

Migrations are embedded and auto-run on startup, so you do not need a separate migration command for normal local work.

## Common Loops

Run the same quality gate as CI:

```bash
make ci
```

Run one service directly:

```bash
go run ./cmd/api
go run ./cmd/ingestor
go run ./cmd/worker
```

Run the full stack in containers:

```bash
docker compose up --build
```

Run deterministic replay instead of live ingest:

```bash
INGESTOR_MODE=replay \
INGESTOR_REPLAY_FIXTURE_PATH=internal/replay/testdata/relay_payloads/basic_flow.ndjson \
go run ./cmd/ingestor
```

Replay is useful when changing derivations or compatibility behavior because it exercises the canonical ingest path and then drains jobs in deterministic order.

## Migrations

Schema files live under `migrations/` and are embedded into the binaries.

Rules:

- Add new migrations; do not edit applied migrations.
- Migration checksums are audited on startup. If you rewrite an existing migration, startup can fail with a checksum mismatch.
- Keep migrations forward-only and small enough to review.

Because all services call `store.Migrate()` on startup, migration changes should be treated as part of the deploy surface, not a side step.

For rollback-aware migration practices, staged rollout expectations, and migration PR checklist guidance, see [migrations.md](migrations.md).

## Running Tests

The repository already has focused unit and integration-style tests around:

- ingest validation
- relay lifecycle and backfill
- queue behavior
- projections and rebuilds
- API handlers
- replay fixtures

Start with:

```bash
make test
```

Note on integration coverage:

- Several integration tests require Postgres and will skip when neither `TEST_DATABASE_URL` nor `DATABASE_URL` is set.
- CI sets `TEST_DATABASE_URL` so DB-backed integration tests run on every push/PR.
- For local parity with CI, point `TEST_DATABASE_URL` at the Compose-managed Postgres on `localhost:5432` before testing.

When changing derivation behavior, also run the most relevant targeted packages:

```bash
go test ./internal/derivation ./internal/replay ./internal/store ./internal/api ./internal/api_primal
```

Run static analysis locally:

```bash
make lint
```

Run race checks for concurrency-sensitive packages:

```bash
make test-race
```

Generate a coverage profile and summary:

```bash
make cover
```

Verify module integrity and known vulnerabilities:

```bash
make mod-verify
make vulncheck
```

Optionally verify local build parity with CI:

```bash
make build
```

## Adding a New Projection Safely

Use this sequence. If you skip pieces, the projection usually becomes non-rebuildable or invisible to operations.

1. Add schema in a new migration.
2. Define a derivation name, job type, and version constant in `internal/derivation`.
3. Enqueue the job from canonical ingest if the projection should react to new events.
4. Implement the handler as an idempotent function of canonical data and existing lower-layer derivations.
5. Register the handler in `ProcessJob()`.
6. Register rebuild support so operators can re-run it by scope or full rebuild.
7. Expose read paths only after the projection has a clear consistency model.
8. Add tests for ingest, replay, and rebuild behavior.

Good projection behavior in this repo means:

- rerunning the same job is safe
- deleting and recomputing derived rows is safe
- version changes are explicit
- rebuilds do not require hand-edited data repair

## Codebase Conventions

- Keep compatibility logic at the boundary. `internal/api_primal` should translate shapes, not leak compatibility rules into core storage models.
- Keep raw ingest strict and side-effect free until the canonical write boundary.
- Prefer deterministic ordering in queries and tests.
- Keep Postgres as the primary consistency boundary unless there is a strong reason not to.
- Treat `APP_VERSION`, derivation versions, and migration history as operationally meaningful, not decorative metadata.

## How Not To Break The Architecture

- Do not read Layer 3 projections inside the ingest path to decide what raw data to store.
- Do not mutate or overwrite canonical raw events to fit new product behavior.
- Do not bypass the queue for derived state that can be eventually consistent.
- Do not introduce a new read model without a rebuild story.
- Do not change a projection algorithm without thinking about active versus target derivation versions.
- Do not edit old migrations after they have been applied anywhere.

If a change makes rebuilds harder, it is probably pushing logic into the wrong layer.

Automated boundary checks:

- Import boundaries are enforced by [`internal/archtest/boundaries_test.go`](../internal/archtest/boundaries_test.go).
- Keep new package dependencies aligned with that test and the layering model in [`architecture.md`](architecture.md).

## Related Docs

- [../README.md](../README.md)
- [README.md](README.md)
- [architecture.md](architecture.md)
- [operations.md](operations.md)
- [migrations.md](migrations.md)
- [compatibility.md](compatibility.md)
- [testing.md](testing.md)
- [api.md](api.md)
