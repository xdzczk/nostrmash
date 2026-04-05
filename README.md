# NostrMash

Durable Nostr ingest and rebuildable read models in Go and Postgres.

![Go 1.23+](https://img.shields.io/badge/go-1.23%2B-00ADD8?logo=go&logoColor=white)
![Postgres Primary](https://img.shields.io/badge/postgres-primary-4169E1?logo=postgresql&logoColor=white)
![Docker Native](https://img.shields.io/badge/docker-native-2496ED?logo=docker&logoColor=white)

NostrMash is a Go-based, Docker-native Nostr ingestion and indexing platform. It connects to relays, validates and durably stores canonical raw events in Postgres, then builds derived read models asynchronously for profiles, replies, threads, counts, and native read APIs.

It exists to keep one boundary uncompromised: raw Nostr truth should be durable, inspectable, and replayable; product-facing query state should be rebuildable, versioned, and operationally boring.

## At A Glance

| Area | Value |
| --- | --- |
| Runtime | Go `1.23+` |
| Primary datastore | Postgres |
| Services | `api`, `ingestor`, `worker` |
| Default API address | `http://localhost:8080` |
| Ingest modes | `live`, optional `backfill`, deterministic `replay` |

## Why This Exists

Most systems get into trouble by mixing ingest truth with read-time convenience. NostrMash does the opposite:

- raw events are stored first and treated as durable truth
- invalid payloads are quarantined, not silently dropped
- derived state is asynchronous and rebuildable
- projection changes are explicit, versioned, and observable

## System Model

NostrMash is built as four simple pieces:

- `api` serves the native read API, a focused but growing Primal compatibility adapter, health/metrics, and admin inspection endpoints
- `ingestor` connects to relays, validates payloads, quarantines invalid events, writes canonical rows, and enqueues derivation jobs
- `worker` drains the Postgres-backed job queue and materializes derived state
- `postgres` is the source of truth for raw events, relay provenance, invalid payloads, checkpoints, job state, derivation metadata, and projections

```text
relay -> validation -> canonical storage -> jobs -> derived projections -> API
```

## Design Principles

- Raw truth is durable. Canonical event JSON, expanded tags, and relay provenance are stored first.
- Derived state is rebuildable. Projections can be recomputed from canonical storage.
- Derivations are versioned. The system tracks compiled, target, and active derivation versions.
- Boring operations over cleverness. One primary datastore, transactional writes, explicit rebuilds, minimal moving parts.

## Quick Start

Bring up the full stack:

```bash
docker compose up --build
```

Sanity-check the stack:

```bash
curl http://localhost:8080/health
curl http://localhost:8080/ready
curl http://localhost:8080/metrics
```

Runtime notes:

- Compose starts `postgres`, `api`, `ingestor`, and `worker`
- Migrations are embedded and run automatically on service startup
- Compose configures the ingestor for `live` mode with `default_v1` filtering and default relay URLs for local bootstrap
- Override relay env vars if needed, for example: `INGESTOR_RELAY_URLS=... INGESTOR_RELAY_ALLOWLIST=... docker compose up --build`
- Admin routes require `ADMIN_BEARER_TOKEN`; without it, `/admin/*` returns `503 admin_unavailable`

For a local multi-terminal workflow and replay mode, go straight to [docs/development.md](docs/development.md).

## Quality Checks

Use the local quality wrapper (mirrors CI):

```bash
make ci
```

Run individual checks:

```bash
make lint
make test
make test-race
make cover
make build
make mod-verify
make vulncheck
```

Integration test note:

- DB-backed integration tests require `TEST_DATABASE_URL` or `DATABASE_URL`.
- For CI parity, prefer setting `TEST_DATABASE_URL` explicitly.
- Local `make ci` and `go test` runs need a Postgres instance reachable from the host.
- The checked-in `docker-compose.yml` does not publish Postgres on `localhost:5432`, so `docker compose up -d postgres` alone is only enough for container-to-container traffic.
- Example local setup against a host-reachable Postgres:

```bash
export TEST_DATABASE_URL=postgres://nostrmash:nostrmash@localhost:5432/nostrmash?sslmode=disable
make ci
```

Coverage output:

```bash
make cover
# writes coverage.out and prints package summary
```

## Documentation Map

| If you are... | Start here | Then read |
| --- | --- | --- |
| New to the system | [docs/architecture.md](docs/architecture.md) | [docs/api.md](docs/api.md) |
| Building locally | [docs/development.md](docs/development.md) | [docs/architecture.md](docs/architecture.md) |
| Operating the stack | [docs/operations.md](docs/operations.md) | [docs/api.md](docs/api.md) |
| Integrating with the API | [docs/api.md](docs/api.md) | [docs/openapi.yaml](docs/openapi.yaml) |

There is also a docs index at [docs/README.md](docs/README.md).

## Repository Layout

- `cmd/api`, `cmd/ingestor`, `cmd/worker`: service entrypoints
- `internal/api`: native read API and admin handlers
- `internal/api_primal`: isolated compatibility adapter and WebSocket gateway for `/primal` support
- `internal/ingestor`: live relay handling, backfill runner, relay lifecycle management
- `internal/nostr`: parse and validate Nostr events before storage
- `internal/store`: Postgres access, migrations, checkpoints, canonical read/write paths
- `internal/derivation`: job dispatch, projections, derivation versioning, rebuild orchestration
- `internal/jobs`: Postgres-backed worker queue
- `internal/query`, `internal/config`, `internal/metrics`: shared query services, runtime config, and observability plumbing
- `internal/replay`, `internal/archtest`: deterministic replay tooling and architecture boundary checks
- `migrations`: embedded schema migrations

## Status And Scope

- Postgres is the only primary datastore in this repository today
- Compatibility support is still partial relative to full Primal product parity, but it now includes a substantial HTTP + WebSocket surface for events, profiles, threads, social graph, moderation, search, zaps, DMs, and curated parity reads
- Compatibility rollout is phased; see `docs/primal_compatibility_matrix.md` and `docs/compatibility_rollout.md`
- Trust/ranking layers are future work, not hidden present features
- Migrations are embedded and run on service startup
