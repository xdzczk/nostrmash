# NostrMash

NostrMash is a Go-based, Docker-native Nostr ingestion and indexing platform. It connects to Nostr relays, validates and durably stores canonical raw events in Postgres, then builds derived read models asynchronously for profiles, replies, threads, counts, and native read APIs.

It exists to keep one boundary uncompromised: raw Nostr truth should be durable, inspectable, and replayable; product-facing query state should be rebuildable, versioned, and operationally boring.

## Why This Exists

Most systems get into trouble by mixing ingest truth with read-time convenience. NostrMash does the opposite:

- raw events are stored first and treated as durable truth
- invalid payloads are quarantined, not silently dropped
- derived state is asynchronous and rebuildable
- projection changes are explicit, versioned, and observable

## System Model

NostrMash is built as four simple pieces:

- `api` serves the native read API, a minimal Primal compatibility adapter, health/metrics, and admin inspection endpoints
- `ingestor` connects to relays, validates payloads, quarantines invalid events, writes canonical rows, and enqueues derivation jobs
- `worker` drains the Postgres-backed job queue and materializes derived state
- `postgres` is the source of truth for raw events, relay provenance, invalid payloads, checkpoints, job state, derivation metadata, and projections

In steady state the flow is:

`relay -> validation -> canonical storage -> jobs -> derived projections -> API`

## Design Principles

- Raw truth is durable. Canonical event JSON, expanded tags, and relay provenance are stored first.
- Derived state is rebuildable. Projections can be recomputed from canonical storage.
- Derivations are versioned. The system tracks compiled, target, and active derivation versions.
- Boring operations over cleverness. One primary datastore, transactional writes, explicit rebuilds, minimal moving parts.

## Quick Start

### Docker Compose

Bring up the full stack:

```bash
docker compose up --build
```

The API will be available at `http://localhost:8080`.

Start by checking:

```bash
curl http://localhost:8080/health
curl http://localhost:8080/ready
curl http://localhost:8080/metrics
```

Then read:

- `docs/openapi.yaml` for request and response contracts
- `docs/architecture.md` for system boundaries and layering

Runtime notes:

- Compose starts `postgres`, `api`, `ingestor`, and `worker`
- Migrations are embedded and run automatically on service startup
- Admin routes require `ADMIN_BEARER_TOKEN`; without it, `/admin/*` returns `503 admin_unavailable`

### Local Run

Start Postgres only:

```bash
docker compose up -d postgres
```

Load environment from `.env.example`:

```bash
cp .env.example .env
set -a
source .env
set +a
```

Run each service in its own terminal:

```bash
go run ./cmd/api
go run ./cmd/ingestor
go run ./cmd/worker
```

The ingestor defaults to live relay mode. For deterministic replay instead:

```bash
INGESTOR_MODE=replay go run ./cmd/ingestor
```

Replay runs the same validation and canonical ingest path, then drains jobs deterministically without connecting to live relays.

## Repository Layout

- `cmd/api`, `cmd/ingestor`, `cmd/worker`: service entrypoints
- `internal/api`: native read API and admin handlers
- `internal/api_primal`: isolated compatibility adapter for a minimal `/primal/v1` surface
- `internal/ingestor`: live relay handling, backfill runner, relay lifecycle management
- `internal/nostr`: parse and validate Nostr events before storage
- `internal/store`: Postgres access, migrations, checkpoints, canonical read/write paths
- `internal/derivation`: job dispatch, projections, derivation versioning, rebuild orchestration
- `internal/jobs`: Postgres-backed worker queue
- `migrations`: embedded schema migrations
- `docs/openapi.yaml`: request/response contract details

## Read Next

- `docs/architecture.md`: service boundaries, data flow, layering, and rebuild model
- `docs/development.md`: local setup, commands, migrations, and safe projection changes
- `docs/operations.md`: health, checkpoints, jobs, rebuilds, and troubleshooting
- `docs/api.md`: API surfaces, consistency model, pagination, and error shape
