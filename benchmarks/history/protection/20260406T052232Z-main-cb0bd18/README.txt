Performance snapshot: 20260406T052232Z-main-cb0bd18

Collected:
- benchmark outputs under benchmarks/*.txt (unless PERF_SKIP_BENCHMARKS=1)
- optional load-test results under loadtest/results (when PERF_COLLECT_INCLUDE_LOADTEST=1)

Comparison examples:
- benchstat benchmarks/history/protection/<older-run>/benchmarks/benchmark-hot.txt benchmarks/history/protection/20260406T052232Z-main-cb0bd18/benchmarks/benchmark-hot.txt
- benchstat benchmarks/history/protection/<older-run>/benchmarks/benchmark-query.txt benchmarks/history/protection/20260406T052232Z-main-cb0bd18/benchmarks/benchmark-query.txt

If benchstat is missing:
go install golang.org/x/perf/cmd/benchstat@latest
