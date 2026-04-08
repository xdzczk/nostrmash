# Refactor Hotspots Baseline

Generated: 2026-04-08

## Scope
- Language: Go (`*.go`)
- Repository: `nostrmash`
- Window for churn: last 12 months

## Aggregate metrics
- Go files: `289`
- Total Go LOC: `43,219`
- Production LOC: `25,478`
- Test LOC: `17,741`
- Test/Prod ratio: `0.70`
- Median file size: `110 LOC`
- Average file size: `149.5 LOC`

## Size thresholds
- Files >= 300 LOC: `31`
- Files >= 400 LOC: `21`
- Files >= 500 LOC: `11`
- Files >= 600 LOC: `2`
- Files >= 800 LOC: `0`

## Function length thresholds (non-test)
- Functions >= 80 LOC: `48`
- Functions >= 120 LOC: `12`
- Functions >= 160 LOC: `3`

## Largest Go files (sample)
- `internal/archtest/boundaries_test.go` (`638`)
- `internal/config/doc.go` (`634`)
- `internal/query/fallback_test.go` (`591`)
- `internal/jobs/queue_test.go` (`586`)
- `cmd/worker/main.go` (`584`)
- `internal/jobs/queue.go` (`563`)
- `cmd/trust_worker/main.go` (`469`)
- `internal/metrics/registry.go` (`448`)
- `internal/api_primal/handlers.go` (`433`)
- `internal/query/event_service.go` (`409`)

## Longest non-test functions (sample)
- `internal/config/doc.go::ConfigEnvDocs` (`~570`)
- `cmd/ingestor/main.go::main` (`~241`)
- `internal/replay/snapshot.go::CaptureStateSnapshot` (`~182`)
- `internal/api_primal/ws_session.go::runWS` (`~156`)
- `cmd/api/main.go::main` (`~138`)
- `internal/metrics/registry.go::registerCoreMetrics` (`~128`)
- `cmd/trust_worker/main.go::main` (`~104`)

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

## Top priority candidates (initial)
1. `cmd/worker/main.go`
2. `cmd/trust_worker/main.go`
3. `internal/jobs/queue.go`
4. `internal/api_primal/primal_cache_dispatch.go`
5. `internal/query/event_service.go`
6. `internal/api_primal/handlers.go`
7. `cmd/ingestor/main.go`
8. `internal/metrics/registry.go`
9. `internal/config/config.go`
10. `internal/ingestor/relay/manager.go`
