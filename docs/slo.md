# Service-Level Objectives

This page defines the initial SLO model for NostrMash using telemetry that already exists today.

These are practical starter objectives, not final long-term promises.

## How To Use This Page

- Treat each SLO as an operator-facing reliability contract.
- Use `30d` windows for formal objective tracking unless noted.
- Use traces for localization when an SLO is breached; use metrics/admin endpoints for detection.
- Use recording rules in `observability/recording_rules/slo_and_workflow_rules.yml` for consistent SLO queries.
- Use alert rules in `observability/alerts/core_workflow_alerts.yml` for first-pass detection.

## SLO 1: API Availability

- **Objective**: `>= 99.5%` successful API requests over `30d` for public read routes.
- **Why it matters**: API availability is the primary user-facing reliability signal.
- **Telemetry mapping**:
  - `nostrmash_api_requests_total{path_template,status_code}`
  - Error classes from `status_code` (treat `5xx` as availability failures).
  - `trace_span` for `http.request` when localizing failures.
- **Initial target/threshold**:
  - Availability target: `99.5%` (error budget `0.5%` / 30d).
  - Early warning: sustained `5xx` ratio `> 1%` for 15m.
  - Rule mapping: `nostrmash:api:error_ratio5m`, alert `NostrMashAPIHighErrorRate`.
- **Breach interpretation**:
  - Identify failing `path_template` first.
  - Then follow trace path `http.request -> query.* -> store.*` to isolate where failures propagate.

## SLO 2: API Latency

- **Objective**: `p95 <= 750ms` over `30d` for key read routes (`/api/v1/events/{id}`, `/api/v1/threads/{eventId}`, `/api/v1/profiles/{pubkey}`, `/api/v1/search`).
- **Why it matters**: These routes represent core interactive read performance.
- **Telemetry mapping**:
  - `nostrmash_api_request_duration_seconds{method,path_template}`
  - `trace_span` (`http.request`, `query.*`, `store.*`) for decomposition.
- **Initial target/threshold**:
  - Long-window target: `p95 <= 750ms`.
  - Fast-burn warning: `p95 > 1.5s` for 10m.
  - Rule mapping: `nostrmash:api:critical_path_latency_p95_seconds:5m`, alert `NostrMashAPICriticalPathLatencyHigh`.
- **Breach interpretation**:
  - If API latency rises with DB pool pressure (`nostrmash_db_pool_max_open_usage_ratio`, `nostrmash_db_pool_wait_count_total`), prioritize DB/pool tuning.
  - If API latency rises without DB pressure, inspect query orchestration and worker-side dependency load.

## SLO 3: Ingest Freshness

- **Objective**: enabled relays should show fresh progress (`updated_at`) within `<= 5m` for `99%` of 10m windows.
- **Why it matters**: stale ingest means user-facing reads lag behind relay reality.
- **Telemetry mapping**:
  - `GET /admin/v1/relays` (`updated_at`, checkpoint status per relay/filter group).
  - `nostrmash_ingest_checkpoint_freshness_seconds{mode,filter_group}`.
  - `nostrmash_ingest_events_total{outcome}` for ingest flow health.
  - Traces: `ingest.live.handle_event`, `ingest.backfill.*`.
- **Initial target/threshold**:
  - Freshness target: checkpoint age `< 5m`.
  - Early warning: any critical relay stale `> 10m`.
  - Rule mapping: `nostrmash:ingest:checkpoint_freshness_seconds:max`, alert `NostrMashIngestLikelyStalled`.
- **Breach interpretation**:
  - If invalid events spike and freshness drops, inspect payload/validation regressions.
  - If fetch spans dominate (`ingest.backfill.fetch_page`), prioritize relay/network issues.
  - If store spans dominate, prioritize DB write path and pool saturation.

## SLO 4: Worker Throughput And Queue Health

- **Objective**: keep derived-work pipeline stable:
  - dead-letter rate `< 1%` of terminal outcomes over `30d`
  - no sustained backlog growth (oldest pending job age should normally stay `< 10m`)
- **Why it matters**: projection correctness/freshness depends on worker drain rate.
- **Telemetry mapping**:
  - `nostrmash_worker_jobs_total{job_type,outcome}`
  - `nostrmash_worker_job_execution_duration_seconds{job_type,outcome}`
  - `nostrmash_queue_operation_duration_seconds{operation,result}`
  - `nostrmash_queue_operation_errors_total{operation}`
  - `nostrmash_worker_queue_backlog_oldest_pending_age_seconds`
  - `GET /admin/v1/jobs` for backlog/oldest pending inspection.
- **Initial target/threshold**:
  - Dead-letter target: `< 1%`.
  - Early warning: retry-heavy mix (`retry` dominating) for 15m, or oldest pending age `> 10m`.
  - Rule mapping: `nostrmash:worker:dead_ratio30m`, `nostrmash:worker:retry_ratio15m`, alerts `NostrMashWorkerDeadLetterRateHigh` and `NostrMashWorkerRetryStorm`.
- **Breach interpretation**:
  - If `worker.job.execute` spans are slow but queue ops are healthy, focus derivation logic.
  - If queue operation errors rise (`claim_available`, `complete_job`, `fail_job`), focus DB/locking and queue table path.

## SLO 5: Rebuild/Replay Completion

- **Objective**: operational rebuild actions should complete successfully and predictably:
  - scoped rebuilds: normally `< 30m`
  - full rebuilds: normally `< 4h`
- **Why it matters**: rebuild/replay is the recovery path after logic/version changes.
- **Telemetry mapping**:
  - `GET /admin/v1/rebuilds` (status, started/finished times, attempts, errors).
  - `nostrmash_rebuild_runs_active`, `nostrmash_rebuild_active_oldest_age_seconds`
  - `GET /admin/v1/derivation-versions` (pending/active/target state).
  - Worker + queue telemetry above for bottleneck attribution.
- **Initial target/threshold**:
  - Success target: `>= 95%` rebuild runs succeed without manual intervention.
  - Early warning: any scoped rebuild crossing `30m`, or repeated failed attempts.
  - Rule mapping: `nostrmash:rebuild:active_oldest_age_seconds`, alerts `NostrMashRebuildProcessingSlow` and `NostrMashRebuildJobsDead`.
- **Breach interpretation**:
  - Determine whether slowdown is queue acquisition, worker execution, or store writes via traces and queue/store metrics.

## Deferred SLOs (Not Yet Backed Well Enough)

- End-to-end ingest-to-query freshness percentile (needs stronger event timestamp correlation in telemetry).
- Per-relay or per-tenant SLOs (would risk high-cardinality and require richer segmentation).
- Fine-grained replay SLIs beyond rebuild run state (needs dedicated replay metrics).
