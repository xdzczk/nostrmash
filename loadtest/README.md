# Load Testing

This directory contains a small, repeatable load-test suite for the core NostrMash pressure modes:

- API read pressure
- worker/job throughput pressure
- ingest throughput pressure
- replay + rebuild pressure
- WS + API latency pressure (Primal cache protocol + native reads, p50/p95/p99)

The Bash scenarios are intentionally boring: wrappers around existing binaries/endpoints, `curl`, and small `python3` reducers for summary stats. The WS+API latency scenario is a small Go harness (`loadtest/harness/`) that drives concurrent WebSocket and HTTP clients and reports latency percentiles.

## Prerequisites

- Repository root as working directory.
- Running services for the scenario you execute:
  - API on `http://localhost:8080` by default
  - worker metrics on `http://localhost:9091/metrics` by default when worker metrics are exposed
- `go`, `bash`, `curl`, and `python3` available on your machine.
- For admin/rebuild scenarios:
  - `ADMIN_BEARER_TOKEN` set
  - valid `DERIVATION_NAME` from `GET /admin/v1/derivation-versions`

## Quick Start

List targets:

```bash
make loadtest
```

Run a scenario:

```bash
make loadtest-api
make loadtest-worker
make loadtest-ingest
make loadtest-replay-rebuild
make loadtest-ws-api
```

All scenarios write timestamped output under `loadtest/results/`.

## Scenario 1: API Read Pressure

Script: `loadtest/scenarios/api_read_pressure.sh`

What it does:
- Runs a mixed request stream over:
  - `GET /api/v1/events/{id}`
  - `GET /api/v1/profiles/{pubkey}`
  - `GET /api/v1/threads/{eventId}`
  - `POST /api/v1/events/batch`
  - `POST /api/v1/profiles/batch`
- Uses configurable concurrency and request volume.

Run:

```bash
CONCURRENCY=12 REQUESTS_TOTAL=600 make loadtest-api
```

Useful env vars:
- `BASE_URL` (default `http://localhost:8080`)
- `CONCURRENCY` (default `8`)
- `REQUESTS_TOTAL` (default `240`)
- `REQUEST_TIMEOUT_SECONDS` (default `5`)
- `EVENT_ID`, `PUBKEY` (default fixture IDs)
- `EVENT_BATCH_IDS`, `PROFILE_BATCH_PUBKEYS` (comma-separated lists)

Outputs to capture:
- `requests.csv` (endpoint, status, per-request latency)
- `summary.txt` (throughput RPS, p50/p95, 2xx/429/5xx counts, per-endpoint stats)

Interpretation:
- Rising `p95` with flat `p50` usually means tail-path contention.
- Non-trivial `429` indicates rate limiting before backend saturation.
- Any `5xx` under moderate load is a regression candidate.

## Scenario 2: Worker Throughput Pressure

Script: `loadtest/scenarios/worker_throughput_pressure.sh`

What it does:
- Samples `nostrmash_worker_jobs_total{job_type,outcome}` over time.
- Optionally triggers a full rebuild for one derivation to create worker pressure.

Run (sampling only):

```bash
WORKER_METRICS_URL=http://localhost:9091/metrics make loadtest-worker
```

Run (sample + trigger rebuild):

```bash
ADMIN_BEARER_TOKEN=... \
DERIVATION_NAME=update_thread_projection \
TRIGGER_REBUILD=1 \
make loadtest-worker
```

Useful env vars:
- `API_BASE_URL` (default `http://localhost:8080`)
- `WORKER_METRICS_URL` (default `http://localhost:9091/metrics`)
- `SAMPLE_SECONDS` (default `120`)
- `SAMPLE_EVERY_SECONDS` (default `5`)
- `TRIGGER_REBUILD` (`0`/`1`, default `0`)
- `ADMIN_BEARER_TOKEN`, `DERIVATION_NAME` (required when `TRIGGER_REBUILD=1`)

Outputs to capture:
- `worker_jobs_samples.csv` (outcome counters over time)
- `summary.txt` (counter deltas per outcome)
- `rebuild_trigger_response.json` when rebuild trigger is enabled

Interpretation:
- Positive `succeeded` delta with controlled `retry`/`dead` is healthy throughput.
- Growing `retry`/`dead` deltas indicate handler failures, queue contention, or DB pressure.

## Scenario 3: Ingest Throughput Pressure

Script: `loadtest/scenarios/ingest_throughput_pressure.sh`

What it does:
- Amplifies a fixture by repeating NDJSON payloads.
- Runs ingestor in replay mode against that amplified fixture.
- Reports effective input-line throughput and elapsed time.

Run:

```bash
FIXTURE_REPEAT_COUNT=2000 make loadtest-ingest
```

Useful env vars:
- `FIXTURE_SOURCE_PATH` (default `internal/replay/testdata/relay_payloads/basic_flow.ndjson`)
- `FIXTURE_REPEAT_COUNT` (default `1000`)

Outputs to capture:
- `amplified_fixture.ndjson` (scenario input)
- `ingestor_replay.log` (ingestor run output)
- `summary.txt` (elapsed time + effective lines/sec)

Interpretation:
- This is ingest-front-door pressure (decode/validate/persist/dedupe path), not a substitute for a large unique-event production fixture.
- Use it for repeatable local comparisons first; confirm with larger real fixtures before tuning.

## Scenario 4: Replay + Rebuild Pressure

Script: `loadtest/scenarios/replay_rebuild_pressure.sh`

What it does:
- Replays an amplified fixture (ingest replay mode).
- Triggers a full rebuild for a chosen derivation.
- Polls rebuild snapshots and worker outcome counters during the pressure window.

Run:

```bash
ADMIN_BEARER_TOKEN=... \
DERIVATION_NAME=update_thread_projection \
FIXTURE_REPEAT_COUNT=1000 \
POLL_SECONDS=180 \
make loadtest-replay-rebuild
```

Useful env vars:
- `API_BASE_URL`, `WORKER_METRICS_URL`
- `ADMIN_BEARER_TOKEN`, `DERIVATION_NAME` (required)
- `FIXTURE_SOURCE_PATH`, `FIXTURE_REPEAT_COUNT`
- `POLL_SECONDS`, `POLL_EVERY_SECONDS`

Outputs to capture:
- `replay.log`
- `rebuild_response.json`
- `rebuilds-<timestamp>.json` snapshots + `rebuilds_snapshots.ndjson` index
- `worker_jobs_samples.csv`
- `summary.txt` (replay duration, worker outcome deltas)

Interpretation:
- Use this scenario to observe recovery behavior when replay ingestion and rebuild activity overlap.
- Rising worker retries/dead outcomes under overlap load is an early signal to investigate queue/store/handler bottlenecks before tuning.

## Scenario 5: WS + API Latency Pressure

Harness: `loadtest/harness/` (Go); wrapper: `loadtest/scenarios/ws_api_pressure.sh`.

What it does:
- Opens `WS_CLIENTS` concurrent WebSocket connections that speak the Primal
  cache protocol (`["REQ", subID, {"cache": [verb, params]}]`) against
  `/primal/ws`, measuring time-to-`EOSE` per request.
- Opens `API_CLIENTS` concurrent HTTP clients hitting native `/api/v1` read
  endpoints.
- Reports p50/p95/p99 latency, throughput, and error rates per channel; writes
  a machine-readable `summary.json`.

Request shapes mirror the golden contract fixtures in
`internal/api_primal/testdata/primal_contracts` and the WS dispatch registry in
`internal/api_primal/primal_cache_dispatch.go`. Only 5xx and transport/timeout
failures count as errors; 2xx and 404 are both healthy read outcomes against
sparse datasets.

Run:

```bash
WS_CLIENTS=32 API_CLIENTS=32 DURATION=30s make loadtest-ws-api
```

Run the harness directly (bypassing the wrapper):

```bash
go run ./loadtest/harness -base-url http://localhost:8080 \
  -ws-clients 32 -api-clients 32 -duration 30s \
  -out loadtest/results/ws-api.json
```

Useful env vars (wrapper) / flags (harness):
- `BASE_URL` / `-base-url` (default `http://localhost:8080`)
- `WS_URL` / `-ws-url` (default derived: `ws://<host>/primal/ws`)
- `WS_CLIENTS` / `-ws-clients` (default `16`; `0` disables the WS channel)
- `API_CLIENTS` / `-api-clients` (default `16`; `0` disables the API channel)
- `DURATION` / `-duration` (default `30s`)
- `WARMUP` / `-warmup` (default `2s`, excluded from stats)
- `REQUEST_TIMEOUT` / `-timeout` (default `10s`)
- `PUBKEY`, `EVENT_ID`, `QUERY`, `HASHTAG` fixtures substituted into shapes
- `SCENARIO_FILE` / `-scenario` optional JSON file overriding request shapes

Outputs to capture:
- `summary.txt` (harness stdout)
- `summary.json` (per-channel percentiles, throughput, error rates)

Interpretation:
- Rising `p99` with flat `p50` indicates tail-path contention (single API
  instance, DB pool saturation, or Meili lag).
- Non-trivial `5xx` counts under moderate load are regression candidates;
  `timeout` counts point at a saturated request path.
- `notice` outcomes are protocol-level rejections (rate limits, unsupported /
  at-capacity cache verbs, or fixture mismatches). They are counted separately
  from transport errors; latency is still measured for them.
- `4xx` (including `404`) is treated as a healthy read outcome against sparse
  datasets, not an error.

Server tuning for meaningful numbers: the pgx pool defaults to **4
connections**, which deadlocks under mixed WS+API load. Start the API under
test with `DATABASE_MAX_CONNS` comfortably above the total client count (the CI
job uses `64`) and raise `PRIMAL_WS_MAX_REQ_PER_MINUTE` / `HTTP_RATE_LIMIT_RPM`
so results reflect backend latency rather than pool-acquire queueing or rate
limiting.

## Result Recording Guidance

For each run, keep:
- scenario name + git commit SHA
- environment knobs (concurrency, repeat count, worker concurrency, fixture path)
- key summary metrics (`p95`, throughput, retry/dead deltas, rebuild completion behavior)
- links to relevant metrics/traces dashboards for that time window

This allows apples-to-apples comparisons across branches and config changes.
