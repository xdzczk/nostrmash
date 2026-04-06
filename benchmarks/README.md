# Benchmark Baselines

This directory stores lightweight historical performance snapshots so results are preserved across runs/releases instead of living only in terminal output.

## What Gets Stored

Snapshots are written under `benchmarks/history/<run-id>/` and include:

- `metadata.json` (timestamp, ref, git SHA, bench count, collection flags)
- `benchmarks/*.txt` raw Go benchmark output files
- optional copied load-test output (`loadtest/results`) when requested
- `README.txt` with quick compare commands

## Collect A Snapshot

From repo root:

```bash
make perf-collect
```

Optional knobs:

```bash
PERF_BENCH_COUNT=5 make perf-collect
PERF_COLLECT_INCLUDE_LOADTEST=1 make perf-collect
PERF_COLLECT_LOADTEST_DIR=loadtest/results make perf-collect
PERF_SKIP_BENCHMARKS=1 PERF_COLLECT_INCLUDE_LOADTEST=1 make perf-collect
```

Protected-only snapshot (used for regression protection):

```bash
make perf-protect-collect
```

## Compare Two Snapshot Runs

Install `benchstat` once:

```bash
go install golang.org/x/perf/cmd/benchstat@latest
```

Compare representative files:

```bash
benchstat benchmarks/history/<older>/benchmarks/benchmark-hot.txt benchmarks/history/<newer>/benchmarks/benchmark-hot.txt
benchstat benchmarks/history/<older>/benchmarks/benchmark-query.txt benchmarks/history/<newer>/benchmarks/benchmark-query.txt
```

Compare protected snapshots with built-in median `ns/op` report:

```bash
make perf-protect-compare BASELINE_DIR=benchmarks/history/protection/<older> CURRENT_DIR=benchmarks/history/protection/<newer>
```

This writes:

- `benchmarks/compare/protected-summary.md`
- `benchmarks/compare/protected-summary.json`

For load-test snapshots, compare each scenario's `summary.txt` between runs (for example API `p95`, throughput, worker retry/dead deltas).

## CI/Actions Storage

Manual workflow runs (see `.github/workflows/perf.yml`) upload the collected snapshot directory as a GitHub Actions artifact so baseline outputs can be downloaded and compared later.
