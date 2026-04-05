# Operations

This page is the runtime and incident-response reference for NostrMash. Use [../README.md](../README.md) for first boot; use this document for health, checkpoints, jobs, rebuilds, and what to inspect when the system is not behaving the way you expect.

## On This Page

- [Operator Checklist](#operator-checklist)
- [Running The Stack](#running-the-stack)
- [Health and Readiness](#health-and-readiness)
- [Key Operational Concepts](#key-operational-concepts)
- [What To Inspect First](#what-to-inspect-first)
- [Troubleshooting Flow](#troubleshooting-flow)
- [Backup and Restore Cautions](#backup-and-restore-cautions)

## Operator Checklist

Start here before diving into details:

1. Check `GET /health` and `GET /ready`.
2. Check `GET /admin/v1/system` if admin auth is configured.
3. Check `GET /admin/v1/relays` for checkpoint freshness and relay state.
4. Check `GET /admin/v1/jobs` for backlog, retries, and dead jobs.
5. Check `GET /admin/v1/invalid-events` if validation behavior looks off.
6. Check `GET /admin/v1/derivation-versions` and `GET /admin/v1/rebuilds` if projections look stale.

## Running The Stack

For the first boot path, use [../README.md](../README.md). The standard containerized command is:

```bash
docker compose up --build
```

This starts:

- `postgres`
- `api` on `:8080`
- `ingestor`
- `worker`

All services run embedded migrations on startup. That means schema mistakes show up immediately during boot, not later.

## Health and Readiness

- `GET /health`: liveness only. The process is up.
- `GET /ready`: readiness check for API only. Returns `200` when Postgres is reachable and `503` otherwise.
- `GET /metrics`: Prometheus metrics on the API process.
- `GET /primal/ws`: Primal-compatible WebSocket upgrade endpoint for `REQ`/`CLOSE` traffic.

The ingestor and worker also expose metrics on `METRICS_ADDR` when configured. By default that address is set internally, but Compose does not publish those ports.

Compatibility gateway tuning:

- `PRIMAL_WS_MAX_SUBSCRIPTIONS` bounds active subscriptions per WebSocket connection.
- `PRIMAL_WS_REQUEST_TIMEOUT` bounds per-request filter processing.
- `PRIMAL_WS_MAX_MESSAGE_BYTES` caps inbound frame size.
- `PRIMAL_WS_MAX_REQ_PER_MINUTE` caps REQ frame rate per connection.
- `PRIMAL_WS_ALLOWED_ORIGINS` defines explicit browser-origin allowlist.
- `PRIMAL_WS_ALLOW_ANY_ORIGIN` disables origin enforcement when explicitly enabled.

Compatibility WS observability metrics:

- `nostrmash_primal_ws_connections`
- `nostrmash_primal_ws_frames_total{frame_type=...}`
- `nostrmash_primal_ws_requests_total{request_kind=...,outcome=...}`
- `nostrmash_primal_ws_request_duration_seconds{request_kind=...}`

## Key Operational Concepts

### Relay State

Relay lifecycle and ingest progress are persisted, not held only in memory.

Useful views:

- `GET /api/v1/relays/health`
- `GET /admin/v1/relays`

`/admin/v1/relays` is anchored on durable `ingest_checkpoints` rows. Process memory may be fresher while a relay is actively connected, but persisted checkpoints are the last-known operational truth that survives restarts.

Checkpoint rows currently carry:

- relay identity and scope: `relay_url`, `mode`, `filter_group`
- ingest range markers: `since`, optional `until`, optional `cursor`
- lifecycle status: `status`
- liveness metadata: `updated_at`, optional `eose_seen_at`

After restart, missing live connections do not imply there is no state. Treat rows as stale-but-useful last-known operational truth until new updates arrive.

### Checkpoints

`ingest_checkpoints` are the durable record of where live or backfill ingest last got to for a relay/filter group.

Checkpoint semantics:

- `since` and optional `until` bound the relay history scope
- `cursor` is an optional relay-specific resume marker when backfill needs one
- `updated_at` is the last durable write to the checkpoint row
- `eose_seen_at` records when a relay signaled end-of-stored-events

Statuses in current code:

- `running`
- `completed`
- `failed`
- `paused` exists in the model but is not actively driven by the current ingestor flow

Backfill completion is based on relay EOSE or repeated empty pages when EOSE is not sent.

### Jobs

Jobs live in Postgres and move through:

- `pending`
- `running`
- `succeeded`
- `dead`

The worker claims jobs with `FOR UPDATE SKIP LOCKED`, retries failures, and dead-letters after the configured max attempts.

Primary inspection endpoint:

- `GET /admin/v1/jobs`

### Rebuilds

Projection rebuilds are first-class operational actions, not ad hoc scripts.

Useful endpoints:

- `GET /admin/v1/rebuilds`
- `POST /admin/v1/rebuilds`
- `GET /admin/v1/derivation-versions`

Full rebuilds are the version-promotion path. Narrow rebuild scopes exist for single-event, pubkey, and time-range repair.

### Invalid Events

Invalid relay payloads are not dropped silently. They are written to `invalid_events` with error code, message, optional payload, and relay source when available.

Primary inspection endpoint:

- `GET /admin/v1/invalid-events`

## What To Inspect First

When something breaks, check in this order:

1. Is `api` alive and `ready`?
2. Is Postgres reachable and applying migrations cleanly?
3. Are relay checkpoints moving, stalled, or failing?
4. Is the job backlog growing or are jobs going `dead`?
5. Is the invalid-event rate spiking?
6. Do derivation versions show `rebuild_pending`?

Useful endpoints:

- `GET /health`
- `GET /ready`
- `GET /admin/v1/system`
- `GET /admin/v1/relays`
- `GET /admin/v1/jobs`
- `GET /admin/v1/invalid-events`
- `GET /admin/v1/derivation-versions`
- `GET /admin/v1/rebuilds`

## Troubleshooting Flow

### API returns `503` on `/ready`

- Check Postgres availability first.
- Check whether startup migrations failed.
- Check `GET /admin/v1/system` once admin auth is configured.

### Ingest appears stalled

- Inspect relay checkpoint freshness.
- Check whether configured relays are disabled or backing off.
- Verify `INGESTOR_RELAY_URLS`, allowlist, and filter group configuration.
- Remember that only `default_v1` is implemented today.

### Raw events exist but projections look stale

- Check `GET /admin/v1/jobs` for backlog or dead jobs.
- Check derivation versions for `rebuild_pending`.
- Trigger a scoped or full rebuild if the projection logic changed or a worker was down.

### Invalid event volume spikes

- Inspect `GET /admin/v1/invalid-events`.
- Look for relay-specific issues, malformed client traffic, or a validator change.
- Treat spikes carefully before loosening validation, since invalid storage is part of the safety boundary.

## Backup and Restore Cautions

Postgres holds more than application data. It also holds queue state, relay checkpoints, invalid payloads, and derivation version metadata.

Operationally that means:

- take consistent Postgres backups
- do not treat projections as the only thing worth preserving
- after restore, prefer rebuilding projections rather than patching tables by hand
- never modify old migration files in place after a database has seen them

If projections are suspect after restore, rebuild them. If canonical raw storage is suspect, treat that as a data integrity incident.

## Related Docs

- [../README.md](../README.md)
- [docs/README.md](README.md)
- [architecture.md](architecture.md)
- [development.md](development.md)
- [api.md](api.md)
