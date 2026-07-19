# Refactor Hotspots Baseline

Generated: 2026-07-19

## Scope
- Language: Go (`*.go`)
- Repository: `nostrmash`
- Window for churn: last 12 months

## Aggregate metrics
- Go files: `571`
- Total Go LOC: `98,606`
- Production LOC: `62,147`
- Test LOC: `36,459`
- Test/Prod ratio: `0.59`
- Median file size: `128 LOC`
- Average file size: `172.7 LOC`

## Size thresholds
- Files >= 300 LOC: `86`
- Files >= 400 LOC: `47`
- Files >= 500 LOC: `28`
- Files >= 600 LOC: `15`
- Files >= 800 LOC: `4`
- Files >= 1000 LOC: `2`

## Largest Go files (sample)
- `internal/api/handlers_discovery_test.go` (`1254`)
- `internal/worker/runtime/runtime.go` (`1140`)
- `internal/query/event_service.go` (`904`)
- `internal/config/config_test.go` (`867`)
- `internal/query/fallback_test.go` (`759`)
- `internal/jobs/queue_test.go` (`751`)
- `internal/archtest/boundaries_test.go` (`734`)
- `internal/api/handlers_helpers_test.go` (`722`)
- `internal/derivation/handlers_projection_rebuild_coverage_test.go` (`711`)
- `internal/api/admin_test.go` (`701`)
- `internal/meili/sync.go` (`648`)

## Highest-churn Go files (12 months, sample)
- `cmd/worker/main.go` (`6`)
- `internal/api_primal/handlers.go` (`6`)
- `cmd/api/main.go` (`6`)
- `internal/api/admin.go` (`6`)
- `internal/query/service.go` (`6`)
- `internal/jobs/queue.go` (`5`)
- `cmd/ingestor/main.go` (`5`)
- `internal/api/middleware.go` (`5`)
- `internal/config/config.go` (`5`)

## Top priority candidates (current)
1. `internal/worker/runtime/runtime.go` (`1140` LOC — largest production file; loop/lifecycle wiring)
2. `internal/query/event_service.go` (`904` LOC)
3. `internal/store` package (176-method `PostgresStore`; bounded-context sub-package split pending — see Phase 4c)
4. `internal/meili/sync.go` (`648` LOC)
5. `cmd/api/main.go` / `cmd/worker/main.go` / `cmd/trust_worker/main.go` (binary composition; align on `internal/runtimebootstrap`)
6. `internal/api_primal/primal_cache_dispatch.go`
7. `internal/metrics/registry.go`
8. `internal/config/config.go`
