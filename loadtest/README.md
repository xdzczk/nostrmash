# Load Testing

This directory contains a small, repeatable load-test suite for the core NostrMash pressure modes:

- API read pressure
- worker/job throughput pressure
- ingest throughput pressure
- replay + rebuild pressure

The suite is intentionally boring: Bash wrappers around existing binaries/endpoints, `curl`, and small `python3` reducers for summary stats.

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

## Result Recording Guidance

For each run, keep:
- scenario name + git commit SHA
- environment knobs (concurrency, repeat count, worker concurrency, fixture path)
- key summary metrics (`p95`, throughput, retry/dead deltas, rebuild completion behavior)
- links to relevant metrics/traces dashboards for that time window

This allows apples-to-apples comparisons across branches and config changes.
