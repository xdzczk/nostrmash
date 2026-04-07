# Performance Hot Paths

Use this page to decide where performance work belongs and what evidence should accompany it. The goal is not generic tuning; it is repeatable measurement on the paths that actually matter.

No hard numeric latency or throughput budgets are set here yet. Where baselines are missing, the immediate target is repeatable evidence first, then tighter budgets later.

Contributor flow:

1. Map your change to a hot path below.
2. Run targeted benchmark/load scenarios before and after.
3. Include evidence summary in PR/release notes for perf-sensitive changes.

For command details, see [testing.md](testing.md). For rollout expectations when regressions are found, see [../RELEASE.md](../RELEASE.md).

## Hot-path map

| Hot path | Why it matters most | First evidence to reach for |
| --- | --- | --- |
| Thread views | core interactive read path across native HTTP, Primal HTTP, and WS | route latency, thread benchmarks, query/store traces |
| Batch reads | bursty fan-in path that can dominate tail latency | batch benchmarks, API latency, DB pressure |
| User-info/profile aggregation | high-frequency fan-out path with shaping overhead | profile benchmarks, request latency, metadata-enrichment traces |
| Replay/rebuild execution | recovery speed and rollout safety | worker queue metrics, rebuild timings, replay benchmarks |
| Derivation fan-out | sustained background throughput and staleness control | worker execution metrics, queue pressure, derivation benchmarks |

## Performance principles

- Benchmark before tuning.
- Profile before changing query or index shape.
- Keep perf telemetry low-cardinality and deployment-stable.
- Prefer representative scenarios (realistic thread depth, batch sizes, replay volume) over synthetic trivia.

## Top hot paths

### 1) Thread views (native + Primal HTTP + Primal WS)

Why it matters:
- Thread reads are a user-facing critical path and combine multiple dependent reads (focal event, ancestors, replies, pagination).
- This path is exercised across three surfaces and regressions are visible quickly in product UX.

Entrypoints and responsible packages:
- Routes: `GET /api/v1/threads/{eventId}`, `GET /primal/v1/threads/{eventId}`, Primal WS `thread_view`.
- Transport handlers/gateway: `internal/api/handlers.go`, `internal/api_primal/handlers.go`, `internal/api_primal/ws_gateway.go`.
- Shared orchestration: `internal/query/service.go` (`GetThread`, `GetThreadWindow`).
- Store reads: `internal/store` thread/event fetch paths.

Primary resource sensitivities:
- DB query latency and index shape for ancestor/reply lookups.
- DB pool pressure under concurrent thread windows.
- CPU and allocations from thread assembly, ordering, and transport-specific shaping.
- Network payload size for deep threads, especially WS stream variants.

Current observability/benchmark coverage:
- Metrics: API request duration by route template; Primal WS request duration by `request_kind`; DB op latency/error signals for thread primitives (`get_event_replies`, `get_event_ancestors`, `get_event_raw_by_id`).
- Tracing: `http.request -> query.* -> store.*` and WS request spans.
- Benchmarks: `internal/query/service_benchmark_test.go` (`GetThreadWindow`) and `internal/api_primal/ws_gateway_test.go` (`thread_view` benchmark coverage).

Desired future benchmark/load-test coverage:
- End-to-end thread-view load tests across shallow/medium/deep threads.
- Mixed-surface concurrency tests (native HTTP + Primal HTTP + WS together).
- Memory-allocation tracking for large descendant windows and metadata enrichment.

### 2) Batch reads (event + profile batch endpoints)

Why it matters:
- Batch reads amortize client round trips but can create large bursty DB and allocation pressure.
- They are common API fan-in points and can dominate tail latency under heavy clients.

Entrypoints and responsible packages:
- Routes: `POST /api/v1/events/batch`, `POST /primal/v1/events/batch`, `POST /api/v1/profiles/batch`, `POST /primal/v1/user_infos`.
- Transport handlers: `internal/api/handlers.go`, `internal/api_primal/handlers.go`.
- Shared read orchestration (partial): `internal/query/service.go` (`GetEventBatch`, `GetUserInfos`).
- Store batch reads: `internal/store` event/profile batch query paths.

Primary resource sensitivities:
- DB read amplification from large `IN` sets and ordering requirements.
- CPU/allocations for input normalization, deduplication, stable-order rebuild, and missing-id/missing-pubkey computation.
- Network and request-body pressure for large batched payloads.
- Queue-free but DB-heavy contention with other API/worker workloads.

Current observability/benchmark coverage:
- Metrics: API request duration and rate-limit controls for batch routes; Primal WS request metrics for compatibility batch-like calls.
- Tracing: `query.*` and `store.*` spans where routed through shared query service.
- Benchmarks: `internal/query/service_benchmark_test.go` (`GetUserInfos`) and WS `user_infos` benchmark coverage in `internal/api_primal/ws_gateway_test.go`.
- Gap: explicit benchmark coverage for large event-batch ordering/missing behavior is still limited.

Desired future benchmark/load-test coverage:
- Batch-size scaling tests (small/medium/large) for both event and profile/user-info paths.
- Mixed hit/miss ratio scenarios to capture missing-id handling costs.
- Sustained load tests with realistic client burst patterns and body-size ceilings.

### 3) User-info/profile aggregation (including metadata enrichment)

Why it matters:
- Profile and user-info reads are high-frequency and frequently chained into other responses (threads, feeds, compatibility payloads).
- Aggregation quality directly affects perceived product responsiveness and fan-out behavior.

Entrypoints and responsible packages:
- Routes: `GET /api/v1/profiles/{pubkey}`, `GET /primal/v1/profiles/{pubkey}`, `POST /primal/v1/user_infos`.
- WS compatibility paths: `user_profile`, `user_infos`, and metadata enrichment helpers in `internal/api_primal/ws_gateway.go`.
- Query layer: `internal/query/service.go` user/profile assembly methods.
- Store layer: profile projection reads in `internal/store`.

Primary resource sensitivities:
- DB latency for profile projection lookups and metadata-event fetch follow-ups.
- CPU/allocations for pubkey normalization, ordered joins, and compatibility response shaping.
- Network expansion when profile metadata is attached to larger result sets.

Current observability/benchmark coverage:
- Metrics: DB operation telemetry for `get_profile_by_pubkey`; API and WS request duration metrics on profile-related routes/request kinds.
- Tracing: profile-related `query.*`/`store.*` spans.
- Benchmarks: `GetUserInfos` in `internal/query/service_benchmark_test.go` and WS `user_infos` handling benchmarks.
- Gap: limited direct benchmark coverage of metadata-enrichment fan-out attached to non-profile requests (for example thread and feed shaping).

Desired future benchmark/load-test coverage:
- Fan-out tests where one response requires metadata for many distinct pubkeys.
- Cache-hit vs cache-miss style scenarios (when projection rows are present vs sparse).
- Transport-specific profile shaping cost comparison (native vs Primal HTTP vs WS).

### 4) Replay/rebuild execution

Why it matters:
- Rebuildability is a core system contract; replay/rebuild speed determines recovery time and rollout safety for projection changes.
- This path can contend with live ingest/API traffic and stress DB + worker coordination.

Entrypoints and responsible packages:
- Admin rebuild control: `GET/POST /admin/v1/rebuilds`, `GET /admin/v1/derivation-versions`.
- Replay tooling: `internal/replay` execution path.
- Rebuild orchestration: `internal/derivation/rebuilds.go` and related worker execution flow.
- Queue/storage interaction: `internal/jobs`, `internal/store`.

Primary resource sensitivities:
- DB read/write throughput and transaction contention during large rebuild waves.
- Worker queue claim/complete/fail throughput and retry/dead-letter behavior.
- CPU for projection recomputation and JSON/event transformation steps.
- Potential I/O and memory pressure when replay fixtures or large datasets are loaded.

Current observability/benchmark coverage:
- Metrics: queue operation metrics, worker execution latency by `job_type`/`outcome`, rebuild-oriented alerts and recording rules.
- Tracing: `worker.queue.claim_available`, `worker.job.execute`, and store spans.
- Benchmarks: targeted replay fixture benchmark coverage (`internal/replay/fixture_benchmark_test.go`).
- Gap: limited end-to-end replay/rebuild throughput benchmarks under concurrent load.

Desired future benchmark/load-test coverage:
- End-to-end rebuild load tests with representative dataset sizes and mixed job types.
- Replay+worker concurrency tests to expose contention between rebuild and normal processing.
- Repeatable timing baselines for rebuild phases (queueing, execution, completion).

### 5) Derivation fan-out (worker-heavy canonical-event path)

Why it matters:
- Canonical ingest can fan one event into multiple downstream derivation actions; this defines sustained background throughput.
- Regressions here increase queue lag, retry storms, and projection staleness.

Entrypoints and responsible packages:
- Ingest write/publish boundary: canonical event insert and enqueue in `internal/store/events.go`.
- Queue lifecycle: `internal/jobs/queue.go`.
- Worker execution: `cmd/worker/main.go`, derivation handlers in `internal/derivation/handlers.go`.
- Upstream event flow: `internal/ingestor/live/processor.go` and backfill runner paths.

Primary resource sensitivities:
- Queue and DB contention (`claim_available`, completion/failure transitions).
- Worker CPU and allocations for derivation/reference extraction and projection writes.
- Concurrency tuning (`WORKER_CONCURRENCY`) and backpressure behavior.

Current observability/benchmark coverage:
- Metrics: queue op duration/errors and worker execution duration by `job_type`/`outcome`.
- Tracing: worker queue claim and job execution spans.
- Benchmarks: derivation hot function benchmarks (`internal/derivation/handlers_benchmark_test.go`).
- Gap: limited integrated fan-out throughput benchmarking across queue + derivation + store writes.

Desired future benchmark/load-test coverage:
- Throughput and tail-latency testing across multiple `WORKER_CONCURRENCY` levels.
- Mixed job-type load with controlled failure/retry injections.
- Queue-depth growth/recovery characterization under sustained ingest pressure.

## Measurement ownership notes

- `internal/query` owns shared read-orchestration benchmark scenarios (thread window, batch profile assembly).
- `internal/api_primal` owns WS compatibility shaping benchmarks where transport-specific fan-out is significant.
- `internal/jobs` + `internal/derivation` own worker/queue throughput and retry/dead behavior characterization.
- `internal/replay` owns replay fixture and replay-runner performance baselines.

Future performance changes should cite which hot path is being optimized and include before/after evidence from the relevant benchmark or load-test scenario listed above.

### Example: choosing evidence for a thread-path change

If a change touches thread assembly or reply pagination:

1. Start with the thread hot path section on this page.
2. Run the relevant thread benchmark before and after.
3. Check API route latency or WS request timing if the change affects transport shaping.
4. Use traces to separate store time from query assembly time before concluding where the regression lives.

## Repeatable load-test suite

The repo now includes a small repeatable load-test suite in [`../loadtest`](../loadtest) with scenario wrappers in `make`:

- `make loadtest-api`
- `make loadtest-worker`
- `make loadtest-ingest`
- `make loadtest-replay-rebuild`

Use [`../loadtest/README.md`](../loadtest/README.md) for prerequisites, environment knobs, captured outputs, and high-level interpretation guidance.

## Baseline storage over time

Performance outputs are now preservable as lightweight snapshots under `benchmarks/history/<run-id>/`.

Collection path:

- Local/manual: `make perf-collect`
- Local/manual protected-only: `make perf-protect-collect`
- Optional with load-test outputs: `PERF_COLLECT_INCLUDE_LOADTEST=1 make perf-collect`
- GitHub Actions manual workflow: `Performance Snapshot` (`.github/workflows/perf.yml`)

What is collected:

- Raw benchmark outputs (`benchmarks/*.txt`) from the hot benchmark suites.
- Snapshot `collect_scope` metadata (`full` vs `protected`).
- Snapshot metadata (`metadata.json`) with timestamp/ref/SHA and run knobs.
- Optional copied load-test outputs (`loadtest/results`) when explicitly requested.

How comparisons are intended to work:

- Compare equivalent benchmark files across two snapshots with `benchstat`.
- Compare protected snapshots with `make perf-protect-compare BASELINE_DIR=... CURRENT_DIR=...` (median `ns/op` report for protected benchmarks).
- Compare load-test scenario `summary.txt` files between snapshots for throughput/latency/retry trends.
- Use the same bench count and similar environment settings to reduce noise before concluding regressions.

Why this is not a hard gate yet:

- Signal quality is environment-sensitive (host contention, local DB state, fixture shape).
- Load-test scenarios are currently operator-driven and not yet normalized enough for strict pass/fail thresholds.
- The current design is evidence-preserving first; gating can be added later once variance and baseline stability are proven.

## Regression protection

Protected surfaces:

- thread assembly (`BenchmarkServiceGetThreadWindow`)
- batch/profile read assembly (`BenchmarkServiceGetUserInfos`, `BenchmarkServiceGetEventBatch`)
- compatibility-heavy WS dispatch (`BenchmarkWSGatewayDispatchCacheCallThreadView`, `BenchmarkWSGatewayDispatchCacheCallUserInfos`)
- replay/rebuild hot paths (`BenchmarkLoadFixtureFile`, `BenchmarkDeriveEventReferences`)

Mechanism:

- Local:
  - `make benchmark-protected` for focused repeatable signal (`-count=5`).
  - `make perf-protect-collect` to snapshot protected benchmark outputs.
  - `make perf-protect-compare BASELINE_DIR=... CURRENT_DIR=...` for median `ns/op` comparison on protected benchmarks.
- CI/manual workflow (`.github/workflows/perf.yml`):
  - manual `workflow_dispatch` run
  - optional baseline artifact comparison (`baseline_run_id` + `baseline_artifact_name`)
  - emits a protected benchmark comparison artifact with benchmark deltas
  - optional strict mode only when `enforcement_mode=release_candidate` and `fail_on_regression=true`

Release vs investigation policy:

- **Default/advisory**: regressions trigger investigation and owner sign-off; run does not fail automatically.
- **Release candidate**: run with `enforcement_mode=release_candidate`; use `fail_on_regression=true` when signal quality/environment are controlled enough for stricter enforcement.
- Missing baseline artifacts are treated as non-blocking evidence gaps, not silent success.

Why this level of strictness now:

- Benchmark noise still exists across machines/DB state.
- Compatibility/load surfaces are valuable as directional signals but not universally deterministic gates yet.
- This mechanism preserves evidence and introduces optional strictness only when a release manager explicitly opts in.

Keep historical tuning narratives in PRs, perf artifacts, or issue threads rather than in this doc. This page is the standing reference for how to measure and evaluate performance work, not a changelog of past optimizations.
