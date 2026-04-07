# Operations

Use this page when you are running NostrMash in production or under pressure. Boot validation, health checks, incident triage, rebuild recovery, trust runs, and operator-facing workflows all live here. Use [../README.md](../README.md) for first boot and [observability.md](observability.md) when you need the fuller telemetry catalog.

What this page owns:

- runtime checklists and first-response flow
- interpretation of admin endpoints and operational states
- alert response playbooks
- operator expectations for rebuilds, trust runs, invalid events, and curated data

What it does not try to be:

- a full environment-variable reference; use [configuration.md](configuration.md)
- the full metrics and tracing catalog; use [observability.md](observability.md)
- migration or compatibility policy; use [migrations.md](migrations.md) and [compatibility.md](compatibility.md)

## On this page

- [Operator checklist](#operator-checklist)
- [Running the stack](#running-the-stack)
- [Health and readiness](#health-and-readiness)
- [Key operational concepts](#key-operational-concepts)
- [What to inspect first](#what-to-inspect-first)
- [Troubleshooting flow](#troubleshooting-flow)
- [Backup and restore cautions](#backup-and-restore-cautions)
- [SLO-driven triage](#slo-driven-triage)
- [Alert response playbook](#alert-response-playbook)
- [Debug and build identity](#debug-and-build-identity)
- [Contributor handoff checklist](#contributor-handoff-checklist)

## Operator checklist

Start here before diving deeper:

1. Check `GET /health` and `GET /ready`.
2. Check `GET /admin/v1/system` if admin auth is configured.
3. Check `GET /admin/v1/relays` for checkpoint freshness and relay state.
4. Check `GET /admin/v1/jobs` for backlog, retries, and dead jobs.
5. Check `GET /admin/v1/invalid-events` if validation behavior looks off.
6. Check `GET /admin/v1/derivation-versions` and `GET /admin/v1/rebuilds` if projections look stale.

```mermaid
flowchart TD
    Incident[IncidentObserved] --> Health{health_or_ready}
    Health -->|"failing"| SystemCheck[Check_admin_system]
    Health -->|"healthy"| Freshness{data_or_projection_issue}
    SystemCheck --> RelayCheck[Check_relays_jobs]
    Freshness -->|"ingest_or_stale_data"| RelayCheck
    Freshness -->|"projection_or_read_issue"| RebuildCheck[Check_rebuilds_and_versions]
    RelayCheck --> NextSignals[Use_metrics_logs_traces]
    RebuildCheck --> NextSignals
```

### Example: service looks stale

Use this sequence when the API is up but the data feels behind:

1. Check `GET /ready` to make sure the API still has database connectivity.
2. Check `GET /admin/v1/relays` for stale checkpoints or relay errors.
3. Check `GET /admin/v1/jobs` for backlog, retries, or dead jobs that would slow projection freshness.
4. If raw events appear current but product-facing reads still lag, inspect `GET /admin/v1/derivation-versions` and `GET /admin/v1/rebuilds`.
5. Use [observability.md](observability.md) to decide whether the issue is ingest freshness, queue pressure, DB saturation, or a failing derivation path.

## Contributor handoff checklist

Before merging/releasing behavior changes, contributors should provide operators with:

1. What changed operationally (routes, config defaults, queue/rebuild behavior, migration impact).
2. Which dashboards/metrics/endpoints should be watched first.
3. Rollback posture (binary-only or requires DB/operator action).

For this handoff, use:

- `migrations.md` for schema risk and rollback-aware guidance
- `compatibility.md` for external behavior/deprecation communication
- `../RELEASE.md` for release notes and rollback expectations

## Running the stack

For the first boot path, use [../README.md](../README.md). The standard containerized command is:

```bash
docker compose up --build
```

This starts:

- `postgres`
- `redis`
- `api` on `:8080`
- `ingestor`
- `worker`
- `trust_worker`

All services run embedded migrations on startup. That means schema mistakes show up immediately during boot, not later.

## Health and readiness

- `GET /health`: liveness only. The process is up.
- `GET /ready`: readiness check for API only. Returns `200` when Postgres is reachable and `503` otherwise.
- `GET /metrics`: Prometheus metrics on the API process.
- `GET /primal/ws`: Primal-compatible WebSocket upgrade endpoint for `REQ`/`CLOSE` traffic.

The ingestor, worker, and trust worker expose metrics on `METRICS_ADDR` when configured. API metrics stay on `HTTP_ADDR` at `GET /metrics`. Use [observability.md](observability.md) when you need the complete metric and tracing catalog rather than the operator-first subset below.

Compatibility gateway tuning:

- `PRIMAL_WS_MAX_SUBSCRIPTIONS` bounds active subscriptions per WebSocket connection.
- `PRIMAL_WS_REQUEST_TIMEOUT` bounds per-request filter processing.
- `PRIMAL_WS_MAX_MESSAGE_BYTES` caps inbound frame size.
- `PRIMAL_WS_MAX_REQ_PER_MINUTE` caps REQ frame rate per connection.
- `HTTP_DM_COMPAT_RATE_LIMIT_RPM` caps `get_directmsgs` compatibility requests per connection.
- `PRIMAL_WS_ALLOWED_ORIGINS` defines explicit browser-origin allowlist.
- `PRIMAL_WS_ALLOW_ANY_ORIGIN` disables origin enforcement when explicitly enabled.

HTTP edge protection tuning:

- `HTTP_RATE_LIMIT_RPM` default per-client rate for public HTTP APIs.
- `HTTP_RATE_LIMIT_BURST` token-bucket burst budget.
- `HTTP_SEARCH_RATE_LIMIT_RPM` tighter override for `/api/v1/search`.
- `HTTP_BATCH_RATE_LIMIT_RPM` tighter override for batch POST endpoints.
- Batch JSON bodies are capped at ~2 MiB and admin JSON bodies at 256 KiB; oversized requests return `413`.

Compatibility WS observability metrics:

- `nostrmash_primal_ws_connections`
- `nostrmash_primal_ws_frames_total{frame_type=...}`
- `nostrmash_primal_ws_requests_total{request_kind=...,outcome=...}`
- `nostrmash_primal_ws_request_duration_seconds{request_kind=...}`

HTTP API observability metrics:

- `nostrmash_api_requests_total{method,path_template,status_code}`
- `nostrmash_api_request_duration_seconds{method,path_template}`

Metrics use route templates (for example `/api/v1/events/{id}`) to avoid label-cardinality explosions; structured request logs still carry both actual and template paths.

Runtime/process and DB pool saturation metrics:

- Go runtime and process collectors are exported on each process metrics endpoint (for example `go_goroutines`, `go_gc_duration_seconds`, `process_cpu_seconds_total`, `process_resident_memory_bytes`).
- DB pool metrics are exported from API and worker processes:
  - `nostrmash_db_pool_open_connections`
  - `nostrmash_db_pool_in_use_connections`
  - `nostrmash_db_pool_idle_connections`
  - `nostrmash_db_pool_max_open_connections`
  - `nostrmash_db_pool_max_open_usage_ratio`
  - `nostrmash_db_pool_wait_count_total`
  - `nostrmash_db_pool_acquire_count_total`
  - `nostrmash_db_pool_acquire_duration_seconds_total`
  - `nostrmash_db_pool_canceled_acquire_count_total`
  - `nostrmash_db_pool_constructing_connections`
- Initial interpretation guidance:
  - A sustained `nostrmash_db_pool_max_open_usage_ratio` above `0.8` means the process is frequently near pool limit.
  - A rising `nostrmash_db_pool_wait_count_total` means callers are queueing for DB connections.
  - High `process_resident_memory_bytes` with increasing `go_goroutines` often indicates workload or leak pressure; correlate with request/job throughput before scaling.
  - Persistent non-zero `nostrmash_db_pool_constructing_connections` with rising waits can indicate churn under pressure.

Critical DB + queue/job latency/error metrics:

- DB operation latency and errors:
  - `nostrmash_db_operation_duration_seconds{operation,result}`
  - `nostrmash_db_operation_errors_total{operation}`
- Queue/job operation latency and errors:
  - `nostrmash_queue_operation_duration_seconds{operation,result}`
  - `nostrmash_queue_operation_errors_total{operation}`
- Worker execution latency:
  - `nostrmash_worker_job_execution_duration_seconds{job_type,outcome}`
- Practical interpretation:
  - If `nostrmash_db_operation_errors_total{operation="insert_canonical_event"}` rises, inspect canonical ingest writes and transaction failures first.
  - If thread-read operations (`get_event_replies`, `get_event_ancestors`, `get_event_raw_by_id`) slow down, inspect DB pool saturation and query/index health.
  - If `nostrmash_queue_operation_errors_total{operation="claim_available"|"complete_job"|"fail_job"}` rises, inspect queue table health, lock contention, and worker connectivity.
  - If worker outcomes in `nostrmash_worker_job_execution_duration_seconds` shift toward `retry`/`dead`, inspect derivation handler errors and queue failure transitions together.

Tracing signals:

- Core flows emit OpenTelemetry spans with `trace_id` correlation in logs.
- API accepts upstream trace context via `Traceparent` and returns `X-Trace-ID` on responses.
- Primary traced boundaries:
  - `http.request`
  - `query.*` orchestration spans
  - `store.*` data-access spans
  - `worker.queue.claim_available` and `worker.job.execute`
  - `ingest.live.handle_event` and `ingest.backfill.*`
- Initial interpretation:
  - If `http.request` is slow and child `store.*` spans dominate, prioritize DB and pool-path investigation.
  - If `worker.job.execute` is slow but store spans are short, prioritize derivation logic and downstream compute paths.
  - If `ingest.backfill.fetch_page` dominates, prioritize relay/network fetch behavior before DB tuning.

Failure taxonomy and panic recovery:

- Failure classes in logs:
  - `client_input`: bad request shape/parameters.
  - `dependency_transient`: timeout/cancel/unavailable dependency.
  - `storage`: Postgres/storage failures.
  - `queue_job`: queue claim/complete/fail and worker job lifecycle failures.
  - `internal_bug`: panic or unexpected internal fault.
- HTTP panic recovery:
  - Panics are recovered and returned as `500` `internal_error`.
  - Structured logs include `failure_class=internal_bug` and request/trace correlation fields.
- Worker panic recovery:
  - Job panics are recovered in worker execution loop.
  - Panicking jobs enter normal failure handling (retry/dead-letter) instead of silently killing worker goroutines.

First-class freshness/backlog/rebuild gauges:

- `nostrmash_ingest_checkpoint_freshness_seconds{mode,filter_group}`
- `nostrmash_worker_queue_backlog_oldest_pending_age_seconds`
- `nostrmash_rebuild_runs_active`
- `nostrmash_rebuild_active_oldest_age_seconds`

Storage/fallback operating gauges and counters:

- `nostrmash_storage_database_bytes`
- `nostrmash_storage_table_bytes{table}`
- `nostrmash_storage_table_rows{table}`
- `nostrmash_lookup_local_total{surface,result}`
- `nostrmash_lookup_fallback_total{surface,outcome}`
- `nostrmash_lookup_fallback_latency_seconds{surface}`

Fallback `outcome` now distinguishes `success` (full batch recovery) from `partial_success` (only some missing entities were recovered).

Storage/retention signal ownership:

- Storage gauges are emitted by the API process (`GET /metrics` on API).
- Retention purge counters (`nostrmash_retention_purge_runs_total`, `nostrmash_retention_purged_rows_total`) are emitted by the worker process (`METRICS_ADDR` when enabled).
- Sustained growth slope matters more than one-off size steps after migrations/rebuilds.

Build/runtime identification:

- Each binary logs `build_info` at startup with:
  - `binary_role`
  - `version`
  - `commit`
  - `build_time`
  - `environment`
- The same identity is exposed in metrics via:
  - `nostrmash_build_info{binary_role,version,commit,build_time}`
  - `nostrmash_deployment_info{binary_role,service_name,environment}`

## Key operational concepts

### Relay state

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

The worker claims jobs with `FOR UPDATE SKIP LOCKED`, pushes claimed work to a bounded in-process worker pool, retries failures, and dead-letters after the configured max attempts.

Worker throughput tuning:

- `WORKER_CONCURRENCY` controls bounded in-process worker pool size (default `4`).
- `WORKER_JOB_RETENTION_ENABLED` enables periodic cleanup of old terminal jobs (default `true`).
- `WORKER_JOB_RETENTION_SUCCEEDED_MAX_AGE` and `WORKER_JOB_RETENTION_DEAD_MAX_AGE` control retention windows.
- `WORKER_JOB_RETENTION_RUN_INTERVAL` and `WORKER_JOB_RETENTION_DELETE_BATCH_LIMIT` bound cleanup cadence and delete volume.
- `WORKER_INVALID_EVENTS_RETENTION_ENABLED` enables periodic cleanup of old `invalid_events` rows (default `true`).
- `WORKER_INVALID_EVENTS_RETENTION_MAX_AGE`, `WORKER_INVALID_EVENTS_RETENTION_RUN_INTERVAL`, and `WORKER_INVALID_EVENTS_RETENTION_DELETE_BATCH_LIMIT` bound invalid-event retention behavior.
- `WORKER_INVALID_EVENTS_PAYLOAD_TRIM_ENABLED` enables optional payload-only trimming (`raw_payload -> NULL`) before full-row retention.
- `WORKER_INVALID_EVENTS_PAYLOAD_TRIM_MAX_AGE` and `WORKER_INVALID_EVENTS_PAYLOAD_TRIM_BATCH_LIMIT` bound payload trimming when enabled.

Primary inspection endpoint:

- `GET /admin/v1/jobs`

### Rebuilds

Projection rebuilds are first-class operational actions, not ad hoc scripts.

Useful endpoints:

- `GET /admin/v1/rebuilds`
- `POST /admin/v1/rebuilds`
- `GET /admin/v1/derivation-versions`

Full rebuilds are the version-promotion path. Narrow rebuild scopes exist for single-event, pubkey, and time-range repair.

### Trust runs

Trust computation is run by `trust_worker` and publishes durable global trust output into Postgres.

Useful endpoints:

- `POST /admin/v1/trust/runs` to trigger a run
- `GET /admin/v1/trust/runs` and `GET /admin/v1/trust/runs/{runID}` for run status
- `GET /admin/v1/trust/scores` and `GET /api/v1/trust/scores` for score visibility
- `GET /admin/v1/relays/suggestions` for operator-facing trust-weighted relay recommendations

Trust execution phases:

- `trust_sync_graph_redis`: materialize run-scoped graph snapshot keys in Redis from `follower_edges` and active `trust_seeds`
- `trust_compute_global_scores`: run deterministic iterative graph ranking and stage rows for promotion
- `trust_promote_run`: atomically publish staged rows into `trust_scores_global` and mark the run succeeded

Operational run metadata:

- `trust_runs.redis_snapshot_ref` links a durable run to the Redis snapshot used for compute
- `trust_runs.last_error` captures terminal failure context when phase execution fails
- `trust_runs.input_follower_edges_count` and `trust_runs.score_rows_published` expose run cardinality
- `trust_runs.current_phase`, `phase_started_at`, `phase_finished_at`, and `phase_last_error` capture phase state
- `trust_runs.sync_job_id`, `compute_job_id`, and `promote_job_id` identify per-phase queue jobs

Trust metrics:

- `nostrmash_trust_queue_backlog_oldest_pending_age_seconds`
- `nostrmash_trust_runs_active`
- `nostrmash_trust_active_oldest_run_age_seconds`
- `nostrmash_trust_active_snapshot_age_seconds`
- `nostrmash_trust_score_rows_published_total`
- `nostrmash_trust_phase_duration_seconds{phase,outcome}`

Trust incidents and recovery:

- If trust jobs are not running, check `jobs.worker_pool='trust'` queue backlog and `trust_worker` process health.
- If a run fails, inspect `trust_runs.last_error`, then retrigger with `POST /admin/v1/trust/runs` once corrected.
- Redis state is disposable working state. If Redis is lost/corrupted, trigger a fresh trust run to rehydrate and republish from Postgres.

Trust-driven ingest prioritization:

- Relay ordering is currently calculated at ingestor startup from `trust_scores_global` + `relay_lists_latest`.
- Use `INGESTOR_TRUST_PRIORITIZATION_ENABLED` and `INGESTOR_TRUST_PRIORITIZATION_TOP_PUBKEYS` to control trust ordering behavior.
- If trust prioritization is disabled or fails, ingest falls back to configured relay order.
- A bounded trust-targeted pubkey frontier can be enabled with `INGESTOR_TRUST_FETCH_ENABLED`; it fetches author-scoped replaceable slices (kinds `0`, `3`, `10002`) from configured relays.
- Frontier behavior is bounded and smoothed by `INGESTOR_TRUST_FETCH_MAX_TRACKED_PUBKEYS`, `INGESTOR_TRUST_FETCH_MAX_SELECTED_PER_CYCLE`, `INGESTOR_TRUST_FETCH_REFRESH_INTERVAL`, `INGESTOR_TRUST_FETCH_STABLE_WINDOW`, and `INGESTOR_TRUST_FETCH_MAX_PROMOTIONS_PER_CYCLE`.
- Retry and fetch pacing are controlled by `INGESTOR_TRUST_FETCH_COOLDOWN` and `INGESTOR_TRUST_FETCH_RETRY_DELAY`; recent lookback is bounded by `INGESTOR_TRUST_FETCH_RECENT_LOOKBACK_SECONDS`.

Trust fetch and suggestion metrics:

- `nostrmash_trust_fetch_frontier_count{state}`
- `nostrmash_trust_fetch_cycles_total{outcome}`
- `nostrmash_trust_fetch_pubkeys_total{outcome}`
- `nostrmash_trust_fetch_pubkeys_selected_total`

### Invalid events

Invalid relay payloads are not dropped silently. They are written to `invalid_events` with error code, message, optional payload, and relay source when available.

If payload trimming is enabled, older rows may keep metadata (`error_code`, `error_message`, relay, timestamps) while `raw_payload` is intentionally cleared to reduce storage pressure.

Primary inspection endpoint:

- `GET /admin/v1/invalid-events`

## What to inspect first

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
- `GET /admin/v1/relays/suggestions`
- `GET /admin/v1/jobs`
- `GET /admin/v1/invalid-events`
- `GET /admin/v1/derivation-versions`
- `GET /admin/v1/rebuilds`

## Troubleshooting flow

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

If this is a performance regression rather than a correctness regression, check [performance.md](performance.md) for the current evidence-backed tuning log and benchmark commands used for before/after comparison.

### Invalid event volume spikes

- Inspect `GET /admin/v1/invalid-events`.
- Look for relay-specific issues, malformed client traffic, or a validator change.
- Treat spikes carefully before loosening validation, since invalid storage is part of the safety boundary.

## SLO-driven triage

Use [slo.md](slo.md) as the reliability contract, and this page for action-oriented checks.

- API availability/latency SLOs:
  - Start with `nostrmash_api_requests_total` and `nostrmash_api_request_duration_seconds`.
  - Use `http.request -> query.* -> store.*` traces to localize slow/failing segments.
- Ingest freshness SLO:
  - Start with `GET /admin/v1/relays` checkpoint freshness (`updated_at`) and status.
  - Correlate with ingest counters and `ingest.live.handle_event`/`ingest.backfill.*` traces.
- Worker/queue SLO:
  - Start with `nostrmash_worker_jobs_total`, queue operation metrics, and `GET /admin/v1/jobs`.
  - Use `worker.job.execute` and queue/store spans to separate derivation vs queue-state-transition issues.
- Rebuild/replay SLO:
  - Start with `GET /admin/v1/rebuilds` and `GET /admin/v1/derivation-versions`.
  - Correlate with worker queue/store telemetry before changing concurrency or retry policy.

## Alert response playbook

Alert rules are defined in `observability/alerts/core_workflow_alerts.yml`. Use this map for first response:

- `NostrMashAPIHighErrorRate`
  - Meaning: sustained API `5xx` ratio above SLO early-warning level.
  - Check next: per-route `status_code` mix, then `http.request -> query.* -> store.*` traces.
- `NostrMashAPICriticalPathLatencyHigh`
  - Meaning: core read-route p95 latency is above target.
  - Check next: DB pool pressure, then dominant query/store spans by `trace_id`.
- `NostrMashDBPoolSaturation`
  - Meaning: pool is near max-open and callers are waiting.
  - Check next: slow query paths, pool sizing, and concurrent worker/API load.
- `NostrMashWorkerDeadLetterRateHigh`
  - Meaning: dead-letter ratio exceeds worker SLO threshold.
  - Check next: `job_type` with highest dead outcomes, derivation errors, queue transition errors.
- `NostrMashWorkerRetryStorm`
  - Meaning: retries dominate recent job outcomes.
  - Check next: dependency errors, derivation regressions, queue contention.
- `NostrMashQueueClaimErrorsHigh`
  - Meaning: workers are failing to claim jobs reliably.
  - Check next: DB health, lock contention, and queue table pressure.
- `NostrMashIngestLikelyStalled`
  - Meaning: durable live checkpoint freshness is stale for sustained period.
  - Check next: relay checkpoint freshness (`GET /admin/v1/relays`), relay reachability, ingest traces.
- `NostrMashRebuildProcessingSlow`
  - Meaning: one or more active rebuilds are aging beyond expected scoped duration.
  - Check next: `GET /admin/v1/rebuilds`, rebuild active-age gauge, worker queue/store bottlenecks.
- `NostrMashRebuildJobsDead`
  - Meaning: rebuild jobs are dead-lettering; rebuild recovery is failing.
  - Check next: rebuild run status (`GET /admin/v1/rebuilds`), dead-job error payloads, and worker/store traces.
- `NostrMashTrustRunStuck`
  - Meaning: at least one trust run has remained active beyond the expected phase window.
  - Check next: `GET /admin/v1/trust/runs`, current phase metadata/error fields, trust queue health, and `trust_worker` logs.
- `NostrMashTrustSnapshotStale`
  - Meaning: the most recent successful trust snapshot is older than the freshness target.
  - Check next: trigger or inspect `POST /admin/v1/trust/runs`, verify `trust_worker` liveness, and confirm Redis/Postgres trust phases are completing.
- `NostrMashTrustRetryStorm`
  - Meaning: trust sync/compute/promote phases are retrying at an unhealthy rate.
  - Check next: retry-heavy `job_type` values, trust phase error fields, Redis reachability, and backing store latency.
- `NostrMashFallbackFailureRatioHigh`
  - Meaning: per-surface fallback miss/failure ratio is elevated.
  - Check next: relay availability, timeout/fanout tuning, and local miss distribution by lookup surface.
- `NostrMashFallbackLatencyHigh`
  - Meaning: per-surface fallback p95 latency is elevated.
  - Check next: relay response speed, network path, and fallback timeout budget.
- `NostrMashLocalMissRatioHigh`
  - Meaning: per-surface local cache/index hit ratio degraded for fallback-enabled lookups.
  - Check next: ingest freshness/checkpoints and recent data locality.
- `NostrMashRetentionPurgeFailing`
  - Meaning: worker retention loops are repeatedly erroring.
  - Check next: worker logs for purge target (`jobs` vs `invalid_events`), DB connectivity, and retention env configuration.
- `NostrMashDatabaseGrowthSustained`
  - Meaning: global database growth rate is elevated for hours.
  - Check next: heavy-table growth (`nostrmash_storage_table_bytes{table}`), retention throughput, and expected ingest/rebuild workload.
- `NostrMashInvalidEventsGrowthHigh`
  - Meaning: `invalid_events` is growing faster than expected.
  - Check next: malformed relay/client sources, validator behavior changes, and invalid-event retention purge effectiveness.

## Debug and build identity

Debug listener configuration:

- `DEBUG_ADDR` enables an auxiliary pprof HTTP server.
- API debug server requires `ADMIN_BEARER_TOKEN`; if token is not configured, API debug server remains disabled even when `DEBUG_ADDR` is set.
- Worker and trust-worker debug servers are unauthenticated; bind only to localhost/private addresses.

Common incident commands:

- CPU profile (30s): `go tool pprof http://<addr>/debug/pprof/profile?seconds=30`
- Heap profile: `go tool pprof http://<addr>/debug/pprof/heap`
- Goroutines dump: `curl http://<addr>/debug/pprof/goroutine?debug=2`

Security/access caveats:

- Never expose debug endpoints on public interfaces without network controls.
- Prefer temporary enablement during incidents.
- Treat profile data as sensitive operational data.

## Backup and restore cautions

Postgres holds more than application data. It also holds queue state, relay checkpoints, invalid payloads, and derivation version metadata.

Operationally that means:

- take consistent Postgres backups
- do not treat projections as the only thing worth preserving
- after restore, prefer rebuilding projections rather than patching tables by hand
- never modify old migration files in place after a database has seen them

If projections are suspect after restore, rebuild them. If canonical raw storage is suspect, treat that as a data integrity incident.

For release-time schema change risk decisions, follow [migrations.md](migrations.md) and [../RELEASE.md](../RELEASE.md) together.

## Curated data operations

Curated compatibility surfaces are operator-seeded. The runtime does not auto-ingest external curation feeds.

Tables backing curated responses:

- `curated_recommended_reads`
- `curated_reads_topics`
- `curated_featured_authors`
- `curated_creator_paid_tiers`

Operational expectations:

- Seed and update curated rows using controlled SQL migrations or operator runbooks.
- Keep curated updates transactional to avoid partial list states.
- Treat empty curated tables as valid empty-state behavior, not incidents.
- Curated cache responses for reads/topics/authors are exposed as kind-wrapped WS payloads (`10000145`/`146`/`148`) with JSON content derived from curated tables.
- `creator_paid_tiers` first tries event-native output (latest kind `17000` plus referenced `e` events) and falls back to curated normalized output (`10000147`) when source events are absent.
- `user_of_ln_address` resolves from `profiles_latest.nip05` and emits kind `10000138`; ensure metadata ingestion is healthy when troubleshooting LN-address lookup gaps.

For deeper telemetry detail, use [observability.md](observability.md). For rollout safety and policy, use [migrations.md](migrations.md), [compatibility.md](compatibility.md), and [../RELEASE.md](../RELEASE.md).
