# Observability

Use this page when you need the telemetry catalog. Metrics, tracing, debug endpoints, rule files, and signal interpretation all live here. Use [operations.md](operations.md) for first-response playbooks and [slo.md](slo.md) for the service-level objective layer built on top of these signals.

Quick orientation:

- `operations.md` answers "what should I check first?"
- this page answers "what exactly is emitted, and how should I read it?"
- `slo.md` answers "which signals define reliability targets?"

## Reading guide

| If you need to... | Start here |
| --- | --- |
| find the right metrics endpoint | [Metrics endpoints](#metrics-endpoints) |
| inspect runtime pressure or DB saturation | [Runtime and process signals](#runtime-and-process-signals) and [DB pool saturation signals (API and worker)](#db-pool-saturation-signals-api-and-worker) |
| understand queue, fallback, or storage signals | [Critical DB and queue/job operations](#critical-db-and-queuejob-operations) |
| follow latency or failure across layers | [Tracing](#tracing) |
| understand alert intent and recording rules | [Alerting and recording rules](#alerting-and-recording-rules) |

## Contributor observability expectations

When changing high-risk paths (API contracts, queue/job flow, replay/rebuild, compatibility WS behavior):

- preserve existing metric label cardinality constraints
- ensure traces/log fields still allow request/job correlation
- document any new or changed operator interpretation guidance in this file and `operations.md`

If behavior changes but observability semantics do not, operators lose incident-response context.

Prometheus rule files (first pass) live in:

- `observability/recording_rules/slo_and_workflow_rules.yml`
- `observability/alerts/core_workflow_alerts.yml`

To run Prometheus + Alertmanager + Grafana on the Coolify host and scrape the
app stack, use [observability-stack.md](observability-stack.md).

Build and deployment metadata now also expose:

- `nostrmash_build_info{binary_role,version,commit,build_time}`
- `nostrmash_deployment_info{binary_role,service_name,environment}`

## Metrics endpoints

| Runtime | Endpoint |
| --- | --- |
| API | `GET /metrics` on the API address |
| Worker | `GET /metrics` on `METRICS_ADDR` when configured |
| Ingestor | `GET /metrics` on `METRICS_ADDR` when configured |
| Trust worker | `GET /metrics` on `METRICS_ADDR` when configured |

## Debug and incident tooling

Optional pprof debug surfaces are available:

- API: `DEBUG_ADDR` (requires configured `ADMIN_BEARER_TOKEN`; otherwise API debug server stays disabled)
- Worker: `DEBUG_ADDR` (no auth layer; bind to private/localhost address only)
- Trust worker: `DEBUG_ADDR` (no auth layer; bind to private/localhost address only)

Exposed endpoints (when debug server is enabled):

- `GET /debug/pprof/`
- `GET /debug/pprof/profile`
- `GET /debug/pprof/heap`
- `GET /debug/pprof/goroutine`
- `GET /debug/pprof/trace`

Operational caveats:

- Do not expose debug listeners publicly.
- Prefer `127.0.0.1:<port>` with SSH tunnel or private network access.
- CPU/profile endpoints can add overhead during capture; use targeted windows during incidents.

## Runtime and process signals

Each process exports standard Go runtime and process collectors.

Representative runtime signals:

- `go_goroutines`
- `go_gc_duration_seconds`
- `go_memstats_heap_alloc_bytes`

Representative process signals:

- `process_cpu_seconds_total`
- `process_resident_memory_bytes`
- `process_open_fds` (when supported by platform)

Operator guidance:

- Increasing `go_goroutines` with stable throughput can indicate blocked work or downstream latency.
- Rising `go_memstats_heap_alloc_bytes` and `process_resident_memory_bytes` without recovery after GC may indicate memory pressure.
- A sharp increase in `process_cpu_seconds_total` slope with flat throughput can indicate inefficient queries or retries.

## DB pool saturation signals (API and worker)

API and worker register DB pool metrics from the shared pgx pool.

Capacity and occupancy:

- `nostrmash_db_pool_open_connections`
- `nostrmash_db_pool_in_use_connections`
- `nostrmash_db_pool_idle_connections`
- `nostrmash_db_pool_max_open_connections`
- `nostrmash_db_pool_max_open_usage_ratio`

Acquire/wait pressure:

- `nostrmash_db_pool_acquire_count_total`
- `nostrmash_db_pool_acquire_duration_seconds_total`
- `nostrmash_db_pool_wait_count_total`
- `nostrmash_db_pool_canceled_acquire_count_total`
- `nostrmash_db_pool_constructing_connections`

Operator guidance:

- Treat sustained `nostrmash_db_pool_max_open_usage_ratio > 0.8` as early saturation pressure.
- If `nostrmash_db_pool_wait_count_total` accelerates, the process is waiting on pool capacity.
- If waits rise while `nostrmash_db_pool_in_use_connections` sits near `nostrmash_db_pool_max_open_connections`, investigate slow queries and pool size.
- If waits rise while `nostrmash_db_pool_in_use_connections` is low, investigate connection churn, transient DB reachability, or acquire cancellations.

### Example: slow API request with DB pressure

When a read path feels slow in production:

1. Start with request latency on the API route template.
2. Check whether `nostrmash_db_pool_max_open_usage_ratio` and `nostrmash_db_pool_wait_count_total` are rising at the same time.
3. If they are, follow the matching `trace_id` through `http.request -> query.* -> store.*`.
4. If store spans dominate, treat it as a DB/query-path problem first.
5. If store spans stay short while request time stays high, move up a layer and inspect query orchestration or transport-specific shaping.

## Critical DB and queue/job operations

Focused latency and error telemetry is emitted at stable operation boundaries.

DB operation metrics:

- `nostrmash_db_operation_duration_seconds{operation,result}`
- `nostrmash_db_operation_errors_total{operation}`

Current DB operations instrumented:

- `insert_canonical_event`
- `get_event_raw_by_id`
- `get_profile_by_pubkey`
- `get_event_replies`
- `get_event_ancestors`

Fallback lookup metrics:

- `nostrmash_lookup_local_total{surface,result}` where `result` is `hit` or `miss`
- `nostrmash_lookup_fallback_total{entity,result}` where `entity` is one of `event`, `profile` and `result` is one of `attempt`, `hit`, `miss`, `error`
- `nostrmash_lookup_fallback_latency_seconds{entity}`
- fallback sub-step spans under query orchestration:
  - `query.get_event_by_id.fallback`
  - `query.get_event_batch.fallback`
  - `query.get_profile.fallback`
  - `query.get_profile_batch.fallback`

Current fallback-enabled surfaces:

- `event_by_id`
- `event_batch`
- `profile_by_pubkey`
- `profile_batch`

Fallback infra/system failures are logged as `query_fallback_lookup_failed` with low-cardinality metric labels and structured log context:

- `entity_type`
- `entity_key`/`entity_keys`
- `error_class`
- `degraded_to_not_found`

Storage growth metrics:

- `nostrmash_storage_database_bytes`
- `nostrmash_storage_table_bytes{table}`
- `nostrmash_storage_table_rows{table}`
- emitted by the API process via periodic Postgres catalog snapshots

Queue/job operation metrics:

- `nostrmash_queue_operation_duration_seconds{operation,result}`
- `nostrmash_queue_operation_errors_total{operation}`
- `nostrmash_retention_purge_runs_total{target,result}`
- `nostrmash_retention_purged_rows_total{target}`
- retention counters are emitted by the worker process retention loops
- current retention targets include `jobs_terminal`, `invalid_events`, optional `invalid_events_payload`, `engagement_events`, `replaceable_events`, `deletion_events`, `untrusted_author_events`, `author_recent_events`, `search_documents_body_trim`, `search_documents_orphans`, `event_relays`, `trusted_discovery_candidates`, and `account_states_idle`

Current queue/job operations instrumented:

- `enqueue`
- `claim_available`
- `complete_job`
- `fail_job`
- `enqueue_event_job_tx`

Worker execution metrics:

- `nostrmash_worker_job_execution_duration_seconds{job_type,outcome}`

Result classes used:

- DB: `ok`, `not_found`, `error`
- Queue/job: `ok`, `not_owned`, `not_found`, `error`
- Worker outcome: `succeeded`, `retry`, `dead`, `complete_error`, `fail_mark_error`

Operator guidance:

- Rising `*_errors_total` on one `operation` localizes breakage to that boundary without relying on path-level labels.
- If `get_event_replies`/`get_event_ancestors` latency grows while DB pool pressure also grows, prioritize query-plan/index and pool-capacity checks.
- If `claim_available` or `fail_job` latency grows with rising worker retries/dead outcomes, inspect Postgres lock/wait pressure and job backlog.
- If worker `complete_error` or `fail_mark_error` outcomes appear, worker derivation execution may succeed/fail but queue state transitions are failing and need immediate DB path inspection.

Ingest gate metrics (ingestor):

- `nostrmash_ingest_gate_decisions_total{kind,decision}` — bounded labels: `kind` ∈ `1`, `4`, `5`, `6`, `7`, `9735`, `9802`, `10000`, `10003`, `30023`, `open_kind`, `other`; `decision` ∈ `accept`, `reject_untrusted_author`, `reject_missing_target`, `shadow_reject`, `fail_closed`
- `nostrmash_ingest_trusted_set_size`
- `nostrmash_ingest_trusted_set_loaded` — `1` after first successful load; author-gated kinds (`1`/`4`/`5`/`9802`/`10000`/`10003`/`30023`) fail-close in `trusted_only` when `0`
- `nostrmash_ingest_trusted_set_age_seconds`
- `nostrmash_ingest_events_total{outcome="gated"}`

Gate operator guidance:

- Shadow rollout: compare `shadow_reject` vs `accept` before setting `INGESTOR_TRUST_GATE_MODE=trusted_only`.
- Sustained `fail_closed` after enforce → trusted set never loaded; check `trust_worker` snapshot refresh.
- Alert when `nostrmash_ingest_trusted_set_age_seconds` exceeds `2 × INGESTOR_TRUST_GATE_REFRESH_INTERVAL`.

## Tracing

NostrMash uses OpenTelemetry tracing with W3C trace-context propagation. Traces are no-op by default and become export-capable when OTLP env is configured (`OTEL_TRACES_EXPORTER=otlp` and/or `OTEL_EXPORTER_OTLP_ENDPOINT`).

Span naming conventions:

- API entry: `http.request`
- Query service orchestration: `query.*` (for example `query.get_thread`, `query.search`)
- Store boundaries: `store.*` (for example `store.get_event_raw_by_id`, `store.insert_canonical_event`)
- Worker execution: `worker.queue.claim_available`, `worker.job.execute`
- Ingest paths: `ingest.live.handle_event`, `ingest.backfill.run`, `ingest.backfill.run_relay`, `ingest.backfill.fetch_page`

Stable trace/log correlation fields:

- `trace_id` in request and error logs
- span names (`http.request`, `query.*`, `store.*`, `worker.*`, `ingest.*`)
- selected low-cardinality attributes like `http.method`, `http.route`, `job.type`, and bounded relay identifiers

Trace context propagation:

- Incoming API trace context is accepted from `Traceparent` and propagated through OTel.
- API responses include `X-Trace-ID` for operator correlation.
- Query and store spans inherit request context, preserving `trace_id` across layers.
- Worker and ingest flows create root spans per execution unit and propagate context to internal DB/queue/store calls.

How to use traces operationally:

- Start at `http.request` for a slow/error request, then follow child `query.*` and `store.*` spans by `trace_id`.
- For job incidents, inspect `worker.job.execute` and related queue/store spans to separate handler failures from queue state-transition failures.
- For ingest incidents, inspect `ingest.live.handle_event` and `ingest.backfill.*` spans to see whether failures are in fetch, validation, checkpointing, or canonical writes.

## Failure taxonomy

NostrMash logs now classify operational failures into a small shared taxonomy:

- `client_input`: invalid/malformed client requests (`4xx` paths).
- `dependency_transient`: timeouts/cancellation/unavailable dependencies.
- `storage`: Postgres/storage path failures.
- `queue_job`: queue claim/complete/fail and worker job lifecycle failures.
- `internal_bug`: panics or unexpected internal failures.

Classification fields appear in structured logs as `failure_class` and `failure_reason` on key HTTP and worker failure paths.

## Panic recovery behavior

- HTTP path: `WithPanicRecovery` catches panics, logs `http_panic_recovered` with failure classification, and returns a standard `500` `internal_error` envelope.
- Worker path: job execution panics are recovered and converted into job failures/retries/dead-letter transitions instead of crashing the worker goroutine.
- Panic details are kept in logs/traces while API responses remain generic and safe.

## Alerting and recording rules

This first pass is intentionally conservative and SLO-aligned. The intent is actionable operator paging, not vanity alert volume.

Recording rules provide reusable series for SLO/dashboards, including:

- API error ratio (`nostrmash:api:error_ratio5m`)
- Critical path latency p95, Postgres metadata routes only (`nostrmash:api:critical_path_latency_p95_seconds:5m`)
- Search latency p95, tracked separately since it is Meilisearch-bound rather than Postgres-bound (`nostrmash:api:search_latency_p95_seconds:5m`)
- DB pool saturation helpers (`nostrmash:db_pool:max_open_usage_ratio`, `nostrmash:db_pool:wait_rate5m`)
- Worker dead/retry ratios (`nostrmash:worker:dead_ratio30m`, `nostrmash:worker:retry_ratio15m`)
- Ingest/checkpoint/backlog/rebuild helpers (`nostrmash:ingest:checkpoint_freshness_seconds:max`, `nostrmash:worker:queue_backlog_oldest_pending_age_seconds`, `nostrmash:rebuild:active_oldest_age_seconds`)
- Fallback health helpers (`nostrmash:lookup:fallback_error_ratio5m_by_entity`, `nostrmash:lookup:fallback_miss_ratio5m_by_entity`, `nostrmash:lookup:fallback_latency_p95_seconds:5m_by_entity`, `nostrmash:lookup:local_miss_ratio5m_by_surface`)
- Storage and retention helpers (`nostrmash:storage:database_growth_bytes_per_hour:6h`, `nostrmash:storage:heavy_table_growth_bytes_per_hour:6h`, `nostrmash:retention:purge_error_rate5m`, `nostrmash:retention:purged_rows_rate1h_by_target`, `nostrmash:retention:batch_duration_p95_seconds:6h`)

Alert rules cover:

- API error-rate and latency degradation
- DB pool saturation pressure
- Worker dead-letter/retry storms and queue-claim errors
- Ingest likely stall (checkpoint freshness stale)
- Rebuild slowdown (active rebuild age) and rebuild dead-letter failures
- Fallback error ratio/latency and elevated local miss ratio on fallback-enabled lookup surfaces
- Retention purge failures, elevated retention batch p95 latency, sustained `event_tags` junk catchup, and sustained storage growth (global DB and `invalid_events` early warning)

Interpreting storage growth safely:

- Step increases after migrations/rebuilds can be one-off changes; confirm whether growth slope returns toward baseline.
- Sustained positive growth rates over multiple hours indicate ongoing write pressure and should drive retention/capacity action.
- Use global DB growth first, then table-level growth on fixed heavy-table allowlist to avoid high-cardinality alerting.

Operator response guidance for each alert is in [operations.md](operations.md#alert-response-playbook).

## Existing application metrics

HTTP API:

- `nostrmash_api_requests_total{method,path_template,status_code}`
- `nostrmash_api_request_duration_seconds{method,path_template}`

Worker:

- `nostrmash_worker_jobs_total{job_type,outcome}`

Ingest:

- `nostrmash_ingest_events_total{outcome}`
- `nostrmash_ingest_snapshot_total{outcome}`

Primal compatibility gateway:

- `nostrmash_primal_ws_connections`
- `nostrmash_primal_ws_frames_total{frame_type}`
- `nostrmash_primal_ws_requests_total{request_kind,outcome}`
- `nostrmash_primal_ws_request_duration_seconds{request_kind}`

Cardinality policy:

- Route labels use path templates (not raw URLs).
- Labels should remain bounded and deployment-stable.
- Avoid per-user, per-event, or other unbounded labels.
