# Testing

This repository uses a pragmatic coverage policy focused on high-risk packages instead of a vanity repo-wide percentage.

## Quick Path For Contributors

If you are unsure what to run:

1. Run `make ci` for full local parity with core CI checks.
2. Add targeted checks from this page based on your change type (race, fuzz, benchmark, contract drift).
3. For schema/compatibility/release-sensitive changes, pair this page with:
   - `migrations.md`
   - `compatibility.md`
   - `../RELEASE.md`

## Race Test Policy (Enforced In CI)

CI runs `make test-race-policy`, which executes:

- `./internal/jobs`
- `./internal/store`
- `./internal/ingestor/...`
- `./internal/api_primal`

### Why this scope

- `internal/jobs` and `internal/ingestor` coordinate concurrent workers, relay sessions, and lifecycle flows.
- `internal/store` is shared by concurrent writers/readers and sits on the correctness boundary for persisted state.
- `internal/api_primal` includes WebSocket compatibility paths where concurrent request/session behavior is easy to regress.
- `internal/api` and `internal/query` are intentionally excluded from the blocking race suite today to keep CI cost bounded; they are still covered by unit tests and can be race-tested ad hoc when needed.

This is intentionally targeted instead of full-repo `-race` to keep CI cost reasonable while still covering concurrency-sensitive paths.

## Coverage Policy (Enforced In CI)

CI runs `make coverage-policy`, which executes `scripts/coverage_check.sh` and enforces minimum coverage for:

- `./internal/query` >= `25%`
- `./internal/store` >= `20%`
- `./internal/api_primal` >= `60%`

## Why This Policy

- `internal/query` owns read orchestration logic and is a high-leverage failure point.
- `internal/store` owns data correctness and persistence behavior.
- `internal/api_primal` is a compatibility-heavy transport surface where regressions are easy to introduce.

This keeps the signal high by enforcing coverage where regressions are costly, without pushing contributors to write low-value tests just to raise a global number.

## Fuzz Testing (Targeted Input Surfaces)

Fuzzing is currently focused on high-risk input handling rather than broad, low-value coverage:

- cursor/token decoding in `internal/api` and `internal/api_primal`
- batch request body decoding for native + compatibility HTTP handlers
- WebSocket compatibility frame and request parsing helpers in `internal/api_primal`

Run targeted fuzzers locally with a bounded fuzz time:

```bash
go test ./internal/api -run=^$ -fuzz=FuzzDecodeEventCursor$ -fuzztime=20s
go test ./internal/api -run=^$ -fuzz=FuzzBatchGetEventsRequestDecoder$ -fuzztime=20s
go test ./internal/api -run=^$ -fuzz=FuzzBatchGetProfilesRequestDecoder$ -fuzztime=20s
go test ./internal/api_primal -run=^$ -fuzz=FuzzPrimalDecodeEventCursor$ -fuzztime=20s
go test ./internal/api_primal -run=^$ -fuzz=FuzzPrimalBatchGetEventsRequestDecoder$ -fuzztime=20s
go test ./internal/api_primal -run=^$ -fuzz=FuzzPrimalBatchGetUserInfosRequestDecoder$ -fuzztime=20s
go test ./internal/api_primal -run=^$ -fuzz=FuzzDecodeFrame$ -fuzztime=20s
go test ./internal/api_primal -run=^$ -fuzz=FuzzParseParameterizedReplaceableRefs$ -fuzztime=20s
go test ./internal/api_primal -run=^$ -fuzz=FuzzParseAndValidateDMResetAuth$ -fuzztime=20s
```

`go test -fuzz` runs one fuzz target at a time; pick the specific `Fuzz...` function you want to run.

If a fuzzer finds a crash or decoder regression, keep the fix narrow and add the minimized reproducer to the seed corpus in the fuzz test.

## Benchmarks (Hot Paths)

Benchmarks are targeted at representative hot paths:

- query assembly (`GetThreadWindow`, `GetUserInfos`)
- store-side tag expansion (`ExpandEventTags`)
- replay fixture loading (`loadFixtureFile`)
- derivation reference extraction (`deriveEventReferences`)
- derivation ID normalization (`normalizeUniqueIDs`)
- Primal WS cache-call handling (`thread_view`, `user_infos`)

Run the focused suite:

```bash
make benchmark-hot
```

Run selected groups:

```bash
make benchmark-query
make benchmark-ws
make benchmark-replay-derivation
```

For comparisons across branches, prefer repeatable runs such as:

```bash
go test -run=^$ -bench=BenchmarkServiceGetThreadWindow -benchmem -count=5 ./internal/query
```

Query service tests now prefer capability-shaped constructors (`NewThreadService`, `NewEventService`, `NewProfileService`) so test doubles only implement methods needed for that slice, instead of a large all-purpose reader mock.

Persist benchmark and optional load-test outputs for later comparison:

```bash
make perf-collect
```

See [`../benchmarks/README.md`](../benchmarks/README.md) for snapshot layout and `benchstat` comparison examples.
Use [`performance.md`](performance.md) to choose the hot-path benchmark/load scenario to run for your change.

Regression-protection helpers for expensive/fragile paths:

```bash
make benchmark-protected
make perf-protect-collect
make perf-protect-compare BASELINE_DIR=benchmarks/history/protection/<older> CURRENT_DIR=benchmarks/history/protection/<newer>
```

Notes:

- `perf-protect-collect` stores a protected-only benchmark snapshot (`PERF_COLLECT_SCOPE=protected`).
- `perf-protect-compare` computes median `ns/op` deltas for the protected benchmark set and writes `benchmarks/compare/protected-summary.{md,json}`.
- local compare is advisory by default; set `FAIL_ON_REGRESSION=1` to make local compare exit non-zero when threshold is exceeded.

Protected benchmark set currently focuses on:

- thread assembly (`BenchmarkServiceGetThreadWindow`)
- batch/profile read assembly (`BenchmarkServiceGetUserInfos`, `BenchmarkServiceGetEventBatch`)
- compatibility-heavy WS cache handling (`BenchmarkWSGatewayDispatchCacheCallThreadView`, `BenchmarkWSGatewayDispatchCacheCallUserInfos`)
- replay/rebuild hot functions (`BenchmarkLoadFixtureFile`, `BenchmarkDeriveEventReferences`)
- derivation normalization (`BenchmarkNormalizeUniqueIDs`)

## Load Tests (Repeatable Scenarios)

Repeatable load-test scenarios live under [`../loadtest`](../loadtest):

- API read pressure (`thread` / `event` / `profile` + batch paths)
- worker throughput pressure (metrics sampling, optional rebuild trigger)
- ingest throughput pressure (replay fixture amplification)
- replay + rebuild overlap pressure

Run scenario wrappers via Makefile:

```bash
make loadtest
make loadtest-api
make loadtest-worker
make loadtest-ingest
make loadtest-replay-rebuild
```

The authoritative runbook for prerequisites, environment knobs, outputs, and interpretation is [`../loadtest/README.md`](../loadtest/README.md).

To preserve load-test outputs with a benchmark snapshot in one run:

```bash
PERF_COLLECT_INCLUDE_LOADTEST=1 make perf-collect
```

## Contract Drift (Routes vs OpenAPI)

Route ownership is centralized in `cmd/api/routes.go`. The API server and the contract-drift test both consume this same route declaration set.

Run the drift check:

```bash
make contract-drift
```

This verifies each contract-owned route pattern (`METHOD /path`) is represented in `docs/openapi.yaml` with the matching HTTP method.
The policy is intentionally one-way: route declarations own the runtime surface, and OpenAPI must cover that surface.

## Lint Policy (Blocking)

Linting is intentionally high-signal and blocking in CI.

Run locally:

```bash
make lint
```

The lint stack focuses on correctness and maintainability (`govet`, `staticcheck`, `errcheck`, `ineffassign`, `unused`, `revive`, `gosec`, `misspell`) plus dependency-policy checks (`depguard`) for key architecture boundaries.

## CI-Enforced Gates

The current CI workflow enforces:

- `make fmt-check`
- `make imports-check`
- `make lint`
- `go mod verify`
- `make vulncheck`
- `make cover`
- `make coverage-policy`
- `make test-race-policy`
- `make contract-drift`
- `make rules-check`
- `make configdoc-check`
- `make build`

Contributor workflow and PR expectations are in [`../CONTRIBUTING.md`](../CONTRIBUTING.md).

## Local Commands

Run race policy checks (same scope as CI):

```bash
make test-race-policy
```

You should run this before opening/updating a PR when touching:

- worker/job execution logic
- relay ingest/session code
- store read/write paths
- Primal compatibility and WebSocket gateway behavior

Run the same policy as CI:

```bash
make coverage-policy
```

For full CI parity on the `internal/store` threshold, point tests at a running Postgres:

```bash
export TEST_DATABASE_URL=postgres://nostrmash:nostrmash@localhost:5432/nostrmash?sslmode=disable
make coverage-policy
```

Run full quality checks:

```bash
make ci
```

If a package fails the threshold, add or improve tests in that package around meaningful behaviors (error paths, branching logic, and compatibility-sensitive flows).

Policy references:

- contributor workflow and PR expectations: [`../CONTRIBUTING.md`](../CONTRIBUTING.md)
- release validation expectations: [`../RELEASE.md`](../RELEASE.md)
