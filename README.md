# NostrMash

Run your own Nostr data plane with durable ingest, rebuildable read models, and plug-in compatibility for existing clients.

![NostrMash hero](docs/readme-hero.svg)

[![CI](https://github.com/xdzczk/nostrmash/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/xdzczk/nostrmash/actions/workflows/ci.yml)
[![Latest release](https://img.shields.io/github/v/release/xdzczk/nostrmash?display_name=tag)](https://github.com/xdzczk/nostrmash/releases)
[![Go 1.26+](https://img.shields.io/badge/go-1.26%2B-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![Postgres primary](https://img.shields.io/badge/postgres-primary-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Docker first](https://img.shields.io/badge/docker-first-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Primal compatibility](https://img.shields.io/badge/primal-cache_compatibility-8B5CF6)](docs/primal_compatibility_matrix.md)
[![Self-hostable](https://img.shields.io/badge/self--hostable-0F766E)](docs/coolify.md)

NostrMash is a Go-based, Docker-native Nostr ingestion and indexing platform for teams that want to own their Nostr backend instead of renting a black box. It connects to relays, stores canonical raw events durably in Postgres, then builds higher-level read models asynchronously for profiles, replies, threads, counts, trust outputs, and product-facing query surfaces.

The design goal is simple: keep raw Nostr truth durable, inspectable, and replayable, while keeping product-facing state rebuildable, versioned, and operationally calm.

It is built for the boundary that usually gets messy: durable ingest on one side, product-shaped reads on the other, with explicit rebuild and operational control in between.

It is also built to be easy to run yourself and easy to adopt incrementally: Docker-first for low-friction self-hosting, and Primal-compatible at the boundary so systems already shaped around Primal Cache can plug NostrMash in with far less rewrite pressure.

If you want durable local ownership, operational visibility, and a realistic migration path from incumbent cache-shaped clients, this is what NostrMash is for.

## At a glance

| Area | Value |
| --- | --- |
| Runtime | Go `1.26+` (CI/Docker pin `1.26.2`) |
| Primary datastore | Postgres (canonical) |
| Trust working state | Redis |
| Deployment posture | Docker-first, self-hostable |
| Services | `api`, `ingestor`, `worker`, `trust_worker` |
| Default API address | `http://localhost:8080` |
| Ingest runtime modes | `live`, `replay` |
| Bootstrap options | optional `backfill` |

## Why this exists

Most ingest systems get into trouble when they collapse durable truth and read-time convenience into the same layer. NostrMash keeps those concerns separate:

- raw events are stored first and treated as durable truth
- invalid payloads are quarantined instead of disappearing silently
- derived state is asynchronous and rebuildable
- projection changes are explicit, versioned, and observable

The result is a system that can ingest like infrastructure, evolve like an application, and recover like an operated platform.

What that means in practice:

- you can self-host it quickly instead of assembling a bespoke ingest stack from scratch
- you can keep canonical event truth durable while changing product-facing views over time
- you can adopt it behind existing Primal Cache-style clients without forcing an all-at-once rewrite

Three design choices follow from that:

- **run your own instance easily**: Docker compatibility is a first-class choice, not an afterthought, so local boot and self-hosted deployment stay low-friction
- **keep core truth separate from product shape**: canonical ingest stays durable while read models remain explicit, rebuildable, and replaceable
- **adopt without rewriting everything at once**: the Primal-compatible HTTP and WebSocket boundary exists so environments already using Primal Cache-style surfaces can integrate NostrMash as a practical replacement path

## System model

NostrMash runs as six cooperating pieces:

- `api` serves the native API, the Primal-compatible boundary for Primal Cache-style integration, health/metrics, and operator-facing admin endpoints
- `ingestor` connects to relays, validates payloads, writes canonical rows, records checkpoints, and enqueues derivation work
- `worker` drains the default Postgres-backed job queue and materializes derived state
- `trust_worker` isolates trust-specific jobs, maintains Redis-backed working state, and promotes published trust outputs
- `postgres` is the canonical store for raw events, checkpoints, queue state, derivation metadata, projections, and published trust outputs
- `redis` is disposable working state for the trust pipeline

![NostrMash system overview](docs/readme-system-overview.svg)

## Start here

Choose the path that matches what you need:

| If you are... | Start here | Then read |
| --- | --- | --- |
| New to the project | [docs/architecture.md](docs/architecture.md) | [docs/api.md](docs/api.md) |
| Booting locally | [docs/development.md](docs/development.md) | [docs/architecture.md](docs/architecture.md) |
| Deploying to production | [docs/coolify.md](docs/coolify.md) | [docs/operations.md](docs/operations.md) |
| Operating the stack | [docs/operations.md](docs/operations.md) | [docs/observability.md](docs/observability.md) |
| Integrating with the API | [docs/api.md](docs/api.md) | [docs/openapi.yaml](docs/openapi.yaml) |
| Replacing or mirroring Primal Cache | [docs/primal_compatibility_matrix.md](docs/primal_compatibility_matrix.md) | [docs/compatibility_rollout.md](docs/compatibility_rollout.md) |
| Contributing changes | [CONTRIBUTING.md](CONTRIBUTING.md) | [docs/testing.md](docs/testing.md) |
| Operating compatibility surfaces | [docs/primal_compatibility_matrix.md](docs/primal_compatibility_matrix.md) | [docs/compatibility_rollout.md](docs/compatibility_rollout.md) |
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

### First 60 seconds

Use this sequence to confirm that the system is not only up, but actually doing its job:

1. Check process health:

```bash
curl http://localhost:8080/health
curl http://localhost:8080/ready
```

2. Check persisted relay ingest state from the public API:

```bash
curl http://localhost:8080/api/v1/relays/health
```

3. If you have `ADMIN_BEARER_TOKEN`, check the operator snapshot:

```bash
curl -H "Authorization: Bearer $ADMIN_BEARER_TOKEN" \
  http://localhost:8080/admin/v1/system
curl -H "Authorization: Bearer $ADMIN_BEARER_TOKEN" \
  http://localhost:8080/admin/v1/relays
```

4. Once relay data has arrived, try a real content read:

```bash
curl "http://localhost:8080/api/v1/events/<event-id>"
curl "http://localhost:8080/api/v1/profiles/<pubkey>"
```

The first two steps are deterministic and should work on any healthy local boot. The content reads depend on what your configured relays have delivered so far; `not_found` usually means the ingestor has not seen that object locally yet.

If you want the day-to-day local workflow instead of the one-command boot path, go to [docs/development.md](docs/development.md).

## Quality checks

Use [CONTRIBUTING.md](CONTRIBUTING.md) as the contributor entrypoint. The canonical reproducible verification path (pinned toolchain + ephemeral Postgres) is:

```bash
make verify-docker
```

Use `make verify-local` or `make ci` when you already have Go `1.26.2` and local dependencies aligned and want faster inner-loop checks.

Common local follow-up commands:

```bash
make format
make cover
```

## Go toolchain policy

- Minimum supported language/runtime version: Go `1.26` (`go.mod` uses `go 1.26`)
- Recommended local toolchain: Go `1.26.2` (`go.mod` uses `toolchain go1.26.2`)
- CI and Docker builders are pinned to Go `1.26.2` for reproducible verification (including `make verify-docker`)

Why this split:

- `go` directive communicates the minimum supported major/minor line for contributors
- exact patch pinning is handled by CI/Docker, while the `toolchain` directive keeps local behavior consistent when your installed Go supports toolchain auto-selection

## Local verification commands

Prefer the containerized path for outsider/reviewer verification:

```bash
make verify-docker
```

Use local/native commands when validating changes against the intended Go `1.26.x` policy:

```bash
# Build
make build

# Test (full)
make test

# Lint
make lint

# Integration-backed parity (requires Postgres)
export TEST_DATABASE_URL=postgres://nostrmash:nostrmash@localhost:5432/nostrmash?sslmode=disable
make ci
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
- `cmd/configdoc`, `cmd/rulecheck`: repo maintenance helpers for generated config docs and rule validation
- `internal/api`: native read API and admin handlers
- `internal/api_primal`: compatibility HTTP and WebSocket boundary for `/primal`
- `internal/ingestor`: live relay handling, backfill, and relay lifecycle management
- `internal/nostr`: event parsing and validation
- `internal/store`: Postgres access, migrations, checkpoints, canonical read/write paths
- `internal/derivation`: projection handlers, versioning, and rebuild orchestration
- `internal/jobs`: Postgres-backed queue
- `internal/query`, `internal/config`, `internal/metrics`, `internal/runtimebootstrap`: shared read orchestration, config, telemetry, and runtime startup wiring
- `internal/trust`, `internal/worker`: trust/ranking pipeline and shared worker runtime behavior
- `internal/replay`, `internal/archtest`: deterministic replay tooling and architectural boundary checks
- `migrations`: embedded schema migrations

## Status and scope

- Postgres remains the canonical datastore in this repository today
- the compatibility HTTP and WebSocket surface implements the currently supported legacy-shaped cache/API surface documented in this repository
- use [docs/primal_compatibility_matrix.md](docs/primal_compatibility_matrix.md) for the current compatibility inventory and [docs/compatibility_rollout.md](docs/compatibility_rollout.md) for operational guidance
- trust and ranking are active repository surfaces; the deeper design lives in [docs/architecture/trust-subsystem.md](docs/architecture/trust-subsystem.md)

## Project policy references

- contributor workflow: [CONTRIBUTING.md](CONTRIBUTING.md)
- security reporting and dependency hygiene: [SECURITY.md](SECURITY.md)
- release flow and artifact verification: [RELEASE.md](RELEASE.md)
- migration safety: [docs/migrations.md](docs/migrations.md)
- compatibility policy: [docs/compatibility.md](docs/compatibility.md)
- versioning contract: [VERSIONING.md](VERSIONING.md)
- changelog policy: [CHANGELOG.md](CHANGELOG.md)
