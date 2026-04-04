# Development

## Local Setup

Prerequisites:

- Go `1.23`
- Docker and Docker Compose
- A local Postgres reachable through `DATABASE_URL`

Fast path:

```bash
docker compose up -d postgres
cp .env.example .env
set -a
source .env
set +a
```

Run services in separate terminals:

```bash
go run ./cmd/api
go run ./cmd/ingestor
go run ./cmd/worker
```

Migrations are embedded and auto-run on startup, so you do not need a separate migration command for normal local work.

## Main Commands

Run tests:

```bash
go test ./...
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
go test ./...
```

When changing derivation behavior, also run the most relevant targeted packages:

```bash
go test ./internal/derivation ./internal/replay ./internal/store ./internal/api ./internal/api_primal
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
