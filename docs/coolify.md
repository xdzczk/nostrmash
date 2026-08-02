# Coolify Deployment

Use this page when you want the production-oriented Coolify layout. The local development stack lives in [`../docker-compose.yml`](../docker-compose.yml); this document covers the shape where Postgres and Redis are managed separately and only the runtime services live in the application stack:

- `api`
- `ingestor`
- `worker`
- `trust_worker`

Use [`../docker-compose.coolify.yml`](../docker-compose.coolify.yml) as the Compose source for this setup.

## Why this layout

The checked-in [`../docker-compose.yml`](../docker-compose.yml) is a local/dev stack. It bundles `postgres` and `redis` and publishes their ports to the host.

For Coolify production deployments:

- let Coolify manage `Postgres`
- let Coolify manage `Redis`
- run only the NostrMash binaries in the application stack
- expose only the `api` service publicly

## Coolify resources

Create these resources first:

1. A Postgres resource, ideally `postgres:18-alpine`
2. A Redis resource, ideally `redis:7-alpine` or `redis:7.2`

Keep both internal-only:

- do not enable public proxying
- do not expose public ports
- enable persistent storage

### Redis memory bounds

Coolify's **Maximum Memory Limit** is a Docker/cgroup OOM kill cap. It is not Redis `maxmemory`. Set both:

1. Coolify resource limit (example): `4g`
2. Coolify **Redis Conf** (Configuration → Redis Conf):

```conf
appendonly yes
maxmemory 3gb
maxmemory-policy allkeys-lru
```

Keep Redis `maxmemory` below the Coolify memory limit so Redis evicts before the kernel kills the container. After saving Redis Conf, restart the Redis resource so Coolify regenerates compose with the `redis.conf` bind-mount.

## Coolify application

Create a Docker Compose application from this repository and point Coolify at:

```text
docker-compose.coolify.yml
```

The Compose file builds the repo `Dockerfile` once and starts four services:

- `api` via `/app/api`
- `ingestor` via `/app/ingestor`
- `worker` via `/app/worker`
- `trust_worker` via `/app/trust_worker`

## Public routing

Attach the public domain only to the `api` service.

Use internal port:

```text
8080
```

Do not expose public domains for:

- `ingestor`
- `worker`
- `trust_worker`

## Required environment variables

Set these in the Coolify application:

```bash
DATABASE_URL=<coolify-postgres-internal-url>
ADMIN_BEARER_TOKEN=<long-random-secret>
INGESTOR_RELAY_URLS=wss://relay.primal.net,wss://relay.damus.io,wss://nos.lol
INGESTOR_RELAY_ALLOWLIST=wss://relay.primal.net,wss://relay.damus.io,wss://nos.lol
```

Relay registry seeds (worker) are a cold-start floor, not permanent pins. Prefer a small general-purpose list; specialty/directory relays (e.g. `wss://purplepag.es`) should not be seeds. Use admin `manual_policy=pinned` only for rare ops overrides.

```bash
# Worker — bootstrap seeds compete for active slots via admission scoring/caps
RELAY_REGISTRY_SEED_RELAYS=wss://relay.primal.net,wss://nos.lol,wss://relay.damus.io

# Optional: on-demand profile/relay-list lookup (not firehose ingest)
# API_RELAY_FALLBACK_ENABLED=true
# API_RELAY_FALLBACK_URLS=wss://purplepag.es
```

Set this only when Redis sync is enabled:

```bash
TRUST_REDIS_URL=<coolify-redis-internal-url>
```

Recommended production defaults:

```bash
ENVIRONMENT=production
LOG_LEVEL=info
APP_VERSION=<git-sha-or-release-tag>
INGESTOR_MODE=live
INGESTOR_FILTER_GROUP=default_v1
WORKER_CONCURRENCY=4
TRUST_WORKER_CONCURRENCY=2
TRUST_WORKER_CLAIM_BATCH_SIZE=5
TRUST_ENABLE_SCORE_COMPUTE=true
TRUST_ENABLE_REDIS_SYNC=false
```

Trust-bounded ingest (storage bounding) — set on the shared app env or per-service overrides:

```bash
# Required: at least one seed pubkey (hex, comma-separated)
TRUST_SEED_PUBKEYS=<hex_pubkey>

# trust_worker — feeds the ingest gate snapshot
TRUST_GRAPH_SNAPSHOT_REFRESH_INTERVAL=10m
TRUST_RUN_INTERVAL=1h

# ingestor — start in shadow, flip to trusted_only after warmup
INGESTOR_TRUST_GATE_MODE=open
INGESTOR_TRUST_GATE_MAX_HOPS=2
INGESTOR_TRUST_GATE_REFRESH_INTERVAL=2m

# worker — raw-event retention (defaults are fine; shown for clarity)
WORKER_RETENTION_ENGAGEMENT_ENABLED=true
WORKER_RETENTION_ENGAGEMENT_MAX_AGE=336h
WORKER_RETENTION_REPLACEABLE_ENABLED=true
WORKER_RETENTION_REPLACEABLE_MIN_AGE=24h
WORKER_RETENTION_DELETION_ENABLED=true
WORKER_RETENTION_DELETION_MAX_AGE=336h
```

See [operations.md#trust-bounded-ingest-rollout](operations.md#trust-bounded-ingest-rollout) for the full rollout checklist.

Trust-worker mode matrix:

1. `TRUST_ENABLE_REDIS_SYNC=true` and `TRUST_ENABLE_SCORE_COMPUTE=true`  
   Runs Redis sync + score compute (Redis required).
2. `TRUST_ENABLE_REDIS_SYNC=false` and `TRUST_ENABLE_SCORE_COMPUTE=true`  
   Runs score compute only in postgres-only mode (Redis optional).
3. `TRUST_ENABLE_REDIS_SYNC=false` and `TRUST_ENABLE_SCORE_COMPUTE=false`  
   Invalid startup configuration.

Optional browser/WebSocket origin control:

```bash
PRIMAL_WS_ALLOWED_ORIGINS=https://<your-frontend-domain>
```

## Metrics and debug endpoints

The Compose file keeps the default internal metrics listeners for background services:

- `ingestor`: `:9090`
- `worker`: `:9090`
- `trust_worker`: `:9090`

Because each service runs in its own container, these do not conflict.

Do not expose debug endpoints publicly. If you enable:

- `WORKER_DEBUG_ADDR`
- `TRUST_WORKER_DEBUG_ADDR`

bind them only to private/local addresses.

These are deployment-level convenience names used by the Compose/Coolify wiring. The binaries themselves consume `DEBUG_ADDR`, which is why [configuration.md](configuration.md) documents `DEBUG_ADDR` rather than the service-prefixed aliases.

## Validation checklist

After deployment:

1. Verify `GET /health`
2. Verify `GET /ready`
3. Verify `GET /metrics`
4. Verify admin endpoints using `ADMIN_BEARER_TOKEN`
5. Check `ingestor` logs for relay connectivity
6. Check `worker` logs for queue polling and job execution
7. Check `trust_worker` logs for mode startup and Postgres connectivity (and Redis connectivity only when Redis sync is enabled)

### Example: first production verification pass

A clean first pass looks like this:

1. Open `GET /health` and `GET /ready` through the public `api` domain.
2. Confirm the `api` container is reachable and the background services are healthy in Coolify.
3. Verify `GET /metrics` on the API surface, then confirm background metrics listeners are reachable internally if you scrape them.
4. Use `ADMIN_BEARER_TOKEN` to open `GET /admin/v1/system`, `GET /admin/v1/relays`, and `GET /admin/v1/jobs`.
5. Check `ingestor` for successful relay connections, `worker` for normal queue polling, and `trust_worker` for successful mode startup (`redis-sync+compute`, `redis-sync-only`, or `postgres-only-compute`).
6. Only then treat the deployment as ready for external traffic or migration cutover.

Expected operator endpoints:

- `GET /admin/v1/system`
- `GET /admin/v1/relays`
- `GET /admin/v1/jobs`
- `GET /admin/v1/trust/runs`

## Notes

- The repo `Dockerfile` defaults to `CMD ["/app/api"]`, which is why a single-app deployment only runs the API.
- This Compose layout overrides the command per service so all production runtime roles are present.
- Migrations run on service startup, so Postgres connectivity problems will usually show up quickly in container logs.
