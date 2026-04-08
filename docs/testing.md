# Testing

Use this page when you need to decide what to run for a change. It is the execution guide for CI parity, race testing, coverage policy, fuzzing, benchmarks, load tests, and contract drift.

## Quick path for contributors

If you are unsure what to run:

Prerequisite toolchain: Go `1.26+` (CI/Docker pin `1.26.2`; local recommendation is `1.26.2`).

1. Run `make ci` for full local parity with core CI checks.
2. Add targeted checks from this page based on your change type (race, fuzz, benchmark, contract drift).
3. For schema/compatibility/release-sensitive changes, pair this page with:
   - `migrations.md`
   - `compatibility.md`
   - `../RELEASE.md`

## Decide what to run

| If your change touches... | Start with | Then add |
| --- | --- | --- |
| General product code | `make ci` | targeted package tests if you need faster iteration |
| Concurrency-sensitive paths | `make test-race-policy` | `make ci` before review |
| API or compatibility contracts | `make contract-drift` | relevant handler and compatibility tests |
| Performance-sensitive paths | relevant benchmark or load-test command | `make perf-collect` when you need evidence |
| Coverage-sensitive packages | `make cover` or package tests | `make coverage-policy` |

## Common validation recipes

| Scenario | Recommended path |
| --- | --- |
| Small handler or query change | run focused package tests first, then `make ci` |
| Route or contract change | update docs/contracts, run `make contract-drift`, then relevant tests and `make ci` |
| Concurrency-sensitive change | run `make test-race-policy`, then finish with `make ci` |
| Perf-sensitive change | run the relevant benchmark/load scenario before and after, then store evidence with `make perf-collect` if needed |
| Schema or rollout-sensitive change | pair this page with `migrations.md` and `RELEASE.md`, then run the highest-signal checks plus `make ci` |

## Race test policy (enforced in CI)

CI runs `make test-race-policy`, which executes:

- `go test -race ./...`

### Why this scope

- concurrency bugs are often package-boundary bugs, not just "hot spot" bugs
- blocking `-race` now covers `internal/query`, `internal/trust`, `internal/replay`, `cmd/api`, and other packages that previously relied on advisory coverage only
- the full-repo race suite remains fast enough to justify using it as a merge gate on this repository

For local parity on integration-backed packages such as `internal/store`, point tests at a running Postgres:

```bash
export TEST_DATABASE_URL=postgres://nostrmash:nostrmash@localhost:5432/nostrmash?sslmode=disable
make test-race-policy
```

## Coverage policy (enforced in CI)

CI runs `make coverage-policy`, which executes `scripts/coverage_check.sh` and enforces minimum coverage for:

- `./internal/api` >= `35%`
- `./internal/query` >= `25%`
- `./internal/store` >= `20%`
- `./internal/api_primal` >= `60%`

When `coverage.out` is present (for example after `make cover` or in CI), the policy check consumes that profile directly instead of re-running package tests.

## Why this policy

- `internal/query` owns read orchestration logic and is a high-leverage failure point.
- `internal/store` owns data correctness and persistence behavior.
- `internal/api_primal` is a compatibility-heavy transport surface where regressions are easy to introduce.
- `internal/api` owns the native HTTP boundary and central request validation/error mapping.

This keeps the signal high by enforcing coverage where regressions are costly, without pushing contributors to write low-value tests just to raise a global number.

## Fuzz testing (targeted input surfaces)

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

## Benchmarks (hot paths)

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

## Load tests (repeatable scenarios)

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

## Contract drift (routes vs OpenAPI)

Route ownership is centralized in `cmd/api/routes.go`. The API server and the contract-drift test both consume this same route declaration set.

Run the drift check:

```bash
make contract-drift
```

This verifies each contract-owned route pattern (`METHOD /path`) is represented in `docs/openapi.yaml` with the matching HTTP method.
The policy is intentionally one-way: route declarations own the runtime surface, and OpenAPI must cover that surface.

## Lint policy (blocking)

Linting is intentionally high-signal and blocking in CI.

Run locally:

```bash
make lint
```

The lint stack focuses on correctness and maintainability (`govet`, `staticcheck`, `errcheck`, `ineffassign`, `unused`, `revive`, `gosec`, `misspell`) plus dependency-policy checks (`depguard`) for key architecture boundaries.

## CI-enforced gates

The current CI workflow enforces:

- `make fmt-check`
- `make imports-check`
- `make lint`
- `go mod verify`
- `make vulncheck`
- coverage profile generation (`go test -covermode=atomic -coverprofile=coverage.out ./...`, equivalent to the coverage-producing part of `make cover`)
- `make coverage-policy`
- `make test-race-policy`
- `make contract-drift`
- `make rules-check`
- `make configdoc-check`
- `make build`

## Advisory Deep Confidence workflow

For broader, higher-cost confidence checks that should not block normal PR flow, use the `Deep Confidence` GitHub workflow.

It runs a scheduled advisory pass with a full coverage profile + policy check and uploads artifacts for inspection.

Contributor workflow and PR expectations are in [`../CONTRIBUTING.md`](../CONTRIBUTING.md).

## Local commands

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

Example quick path for an API contract change:

```bash
go test ./internal/api ./internal/api_primal
make contract-drift
make ci
```

If a package fails the threshold, add or improve tests in that package around meaningful behaviors (error paths, branching logic, and compatibility-sensitive flows).

Policy references:

- contributor workflow and PR expectations: [`../CONTRIBUTING.md`](../CONTRIBUTING.md)
- release validation expectations: [`../RELEASE.md`](../RELEASE.md)
