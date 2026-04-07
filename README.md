# NostrMash

Durable Nostr ingest and rebuildable read models in Go and Postgres.

![Go 1.25+](https://img.shields.io/badge/go-1.25%2B-00ADD8?logo=go&logoColor=white)
![Postgres Primary](https://img.shields.io/badge/postgres-primary-4169E1?logo=postgresql&logoColor=white)
![Docker Native](https://img.shields.io/badge/docker-native-2496ED?logo=docker&logoColor=white)

NostrMash is a Go-based, Docker-native Nostr ingestion and indexing platform. It connects to relays, stores canonical raw events durably in Postgres, then builds higher-level read models asynchronously for profiles, replies, threads, counts, trust outputs, and product-facing query surfaces.

The design goal is simple: keep raw Nostr truth durable, inspectable, and replayable, while keeping product-facing state rebuildable, versioned, and operationally calm.

## At a glance

| Area | Value |
| --- | --- |
| Runtime | Go `1.25+` |
| Primary datastore | Postgres (canonical) |
| Trust working state | Redis |
| Services | `api`, `ingestor`, `worker`, `trust_worker` |
| Default API address | `http://localhost:8080` |
| Ingest modes | `live`, optional `backfill`, deterministic `replay` |

## Why this exists

Most ingest systems get into trouble when they collapse durable truth and read-time convenience into the same layer. NostrMash keeps those concerns separate:

- raw events are stored first and treated as durable truth
- invalid payloads are quarantined instead of disappearing silently
- derived state is asynchronous and rebuildable
- projection changes are explicit, versioned, and observable

## System model

NostrMash runs as six cooperating pieces:

- `api` serves the native API, the Primal-compatible boundary, health/metrics, and operator-facing admin endpoints
- `ingestor` connects to relays, validates payloads, writes canonical rows, records checkpoints, and enqueues derivation work
- `worker` drains the default Postgres-backed job queue and materializes derived state
- `trust_worker` isolates trust-specific jobs, maintains Redis-backed working state, and promotes published trust outputs
- `postgres` is the canonical store for raw events, checkpoints, queue state, derivation metadata, projections, and published trust outputs
- `redis` is disposable working state for the trust pipeline

```text
relay -> validation -> canonical storage -> jobs -> derived projections -> API
```

## Start here

Choose the path that matches what you need:

| If you are... | Start here | Then read |
| --- | --- | --- |
| New to the project | [docs/architecture.md](docs/architecture.md) | [docs/api.md](docs/api.md) |
| Booting locally | [docs/development.md](docs/development.md) | [docs/architecture.md](docs/architecture.md) |
| Deploying to production | [docs/coolify.md](docs/coolify.md) | [docs/operations.md](docs/operations.md) |
| Operating the stack | [docs/operations.md](docs/operations.md) | [docs/observability.md](docs/observability.md) |
| Integrating with the API | [docs/api.md](docs/api.md) | [docs/openapi.yaml](docs/openapi.yaml) |
| Contributing changes | [CONTRIBUTING.md](CONTRIBUTING.md) | [docs/testing.md](docs/testing.md) |
| Planning compatibility work | [docs/primal_compatibility_matrix.md](docs/primal_compatibility_matrix.md) | [docs/compatibility_rollout.md](docs/compatibility_rollout.md) |
| Planning trust and ranking work | [docs/architecture/trust-subsystem.md](docs/architecture/trust-subsystem.md) | [docs/architecture.md](docs/architecture.md) |

For the complete docs hub and source-of-truth map, use [docs/README.md](docs/README.md).

## Quick start

Bring up the local stack:

```bash
docker compose up --build
```

Verify the stack:

```bash
curl http://localhost:8080/health
curl http://localhost:8080/ready
curl http://localhost:8080/metrics
```

What this starts:

- `postgres`
- `redis`
- `api`
- `ingestor`
- `worker`
- `trust_worker`

Runtime notes:

- migrations are embedded and run on service startup
- the local ingestor is configured for `live` mode with the `default_v1` filter group
- admin routes require `ADMIN_BEARER_TOKEN`; without it, `/admin/*` returns `503 admin_unavailable`
- relay settings can be overridden at boot, for example `INGESTOR_RELAY_URLS=... INGESTOR_RELAY_ALLOWLIST=... docker compose up --build`

If you want the day-to-day local workflow instead of the one-command boot path, go to [docs/development.md](docs/development.md).

## Quality checks

Use [CONTRIBUTING.md](CONTRIBUTING.md) as the contributor entrypoint. The shortest path to CI parity is:

```bash
make ci
```

Common follow-up commands:

```bash
make format
make cover
```

Integration test note:

- DB-backed integration tests require `TEST_DATABASE_URL` or `DATABASE_URL`
- the checked-in `docker-compose.yml` publishes Postgres on `localhost:5432`
- for local CI parity, `docker compose up -d postgres` is enough for host-based test runs

Example:

```bash
export TEST_DATABASE_URL=postgres://nostrmash:nostrmash@localhost:5432/nostrmash?sslmode=disable
make ci
```

## Repository layout

- `cmd/api`, `cmd/ingestor`, `cmd/worker`, `cmd/trust_worker`: service entrypoints
- `internal/api`: native read API and admin handlers
- `internal/api_primal`: compatibility HTTP and WebSocket boundary for `/primal`
- `internal/ingestor`: live relay handling, backfill, and relay lifecycle management
- `internal/nostr`: event parsing and validation
- `internal/store`: Postgres access, migrations, checkpoints, canonical read/write paths
- `internal/derivation`: projection handlers, versioning, and rebuild orchestration
- `internal/jobs`: Postgres-backed queue
- `internal/query`, `internal/config`, `internal/metrics`: shared read orchestration, config, and telemetry
- `internal/replay`, `internal/archtest`: deterministic replay tooling and architectural boundary checks
- `migrations`: embedded schema migrations

## Status and scope

- Postgres remains the canonical datastore in this repository today
- compatibility support is still intentionally partial relative to full Primal parity, but the shipped HTTP and WebSocket surface is already substantial
- compatibility rollout remains phased; use [docs/primal_compatibility_matrix.md](docs/primal_compatibility_matrix.md) for current availability and [docs/compatibility_rollout.md](docs/compatibility_rollout.md) for adoption sequencing
- trust and ranking are active repository surfaces; the deeper design lives in [docs/architecture/trust-subsystem.md](docs/architecture/trust-subsystem.md)

## Project policy references

- contributor workflow: [CONTRIBUTING.md](CONTRIBUTING.md)
- security reporting: [SECURITY.md](SECURITY.md)
- dependency hygiene: [docs/security-dependencies.md](docs/security-dependencies.md)
- release flow: [RELEASE.md](RELEASE.md)
- release artifact verification: [docs/release-security.md](docs/release-security.md)
- migration safety: [docs/migrations.md](docs/migrations.md)
- compatibility policy: [docs/compatibility.md](docs/compatibility.md)
- versioning contract: [VERSIONING.md](VERSIONING.md)
- changelog policy: [CHANGELOG.md](CHANGELOG.md)
