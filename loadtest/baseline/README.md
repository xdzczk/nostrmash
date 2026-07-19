# WS + API load-harness baseline

`main-latest/summary.json` is the committed baseline for the continuous WS+API
load-harness trend line. It is **machine-generated** — do not edit by hand.

- The `loadtest-baseline` job in [../../.github/workflows/perf.yml](../../.github/workflows/perf.yml)
  seeds Postgres via `INGESTOR_MODE=replay`, runs
  [the harness](../harness), and commits the resulting `summary.json` on every
  push to `main` (same commit-bot pattern as the protected benchmark baseline).
- The `loadtest-compare` job runs the identical harness on pull requests and
  compares against this file via [`scripts/loadtest_compare.sh`](../../scripts/loadtest_compare.sh).

Gating is **error-rate only** (5xx + transport regressions): a channel fails the
PR when its error rate is both above an absolute floor (1%) and more than doubles
versus the baseline. Latency p95/p99 deltas are reported as advisory only, to
avoid runner-variance flakes.

Until the first post-merge push to `main` regenerates it, `main-latest/` may not
exist; the compare job then runs advisory-only.
