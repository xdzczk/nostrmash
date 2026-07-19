.PHONY: lint lint-ci test test-race test-race-policy cover coverage-policy verify-clean verify-local verify-docker contract-drift rules-check benchmark-hot benchmark-query benchmark-ws benchmark-replay-derivation benchmark-protected perf-collect perf-protect-collect perf-protect-compare loadtest loadtest-api loadtest-worker loadtest-ingest loadtest-replay-rebuild loadtest-ws-api fuzz build mod-verify vulncheck configdoc configdoc-check sqlc sqlc-check fmt fmt-check imports imports-check format ci
BENCH_HOT_PKGS := ./internal/query ./internal/store ./internal/replay ./internal/derivation ./internal/api_primal ./internal/trust
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/cmd/golangci-lint@v1.64.8
GOVULNCHECK := go run golang.org/x/vuln/cmd/govulncheck@v1.1.4
GOIMPORTS := go run golang.org/x/tools/cmd/goimports@latest
SQLC := go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.31.1
VERSION ?= dev
GIT_COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_TIME ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS_API := -X 'main.buildVersion=$(VERSION)' -X 'main.buildCommit=$(GIT_COMMIT)' -X 'main.buildTime=$(BUILD_TIME)'
LDFLAGS_WORKER := -X 'main.buildVersion=$(VERSION)' -X 'main.buildCommit=$(GIT_COMMIT)' -X 'main.buildTime=$(BUILD_TIME)'
LDFLAGS_INGESTOR := -X 'main.buildVersion=$(VERSION)' -X 'main.buildCommit=$(GIT_COMMIT)' -X 'main.buildTime=$(BUILD_TIME)'
LDFLAGS_TRUST_WORKER := -X 'main.buildVersion=$(VERSION)' -X 'main.buildCommit=$(GIT_COMMIT)' -X 'main.buildTime=$(BUILD_TIME)'
BUILD_OUTPUT_DIR ?= bin
COVERAGE_PROFILE ?= coverage.out
VERIFY_COVERAGE_PROFILE ?= .tmp/coverage.verify.out

lint:
	$(GOLANGCI_LINT) run --config .golangci.yml

lint-ci:
	$(MAKE) lint

test:
	go test ./...

test-race:
	$(MAKE) test-race-policy

test-race-policy:
	go test -race ./...

cover:
	@mkdir -p "$(dir $(COVERAGE_PROFILE))"
	go test -covermode=atomic -coverprofile="$(COVERAGE_PROFILE)" ./...
	go tool cover -func="$(COVERAGE_PROFILE)"

coverage-policy:
	COVERAGE_PROFILE="$(COVERAGE_PROFILE)" bash ./scripts/coverage_check.sh

verify-clean:
	rm -f coverage.out coverage-summary.txt
	rm -rf .tmp

verify-local:
	@set -e; \
	trap '$(MAKE) verify-clean' EXIT; \
	$(MAKE) verify-clean; \
	COVERAGE_PROFILE="$(VERIFY_COVERAGE_PROFILE)" $(MAKE) ci

verify-docker:
	@set -e; \
	trap 'docker compose -p nostrmash-verify -f docker-compose.verify.yml down --volumes --remove-orphans >/dev/null 2>&1 || true' EXIT; \
	docker compose -p nostrmash-verify -f docker-compose.verify.yml up --build --abort-on-container-exit --exit-code-from verify verify

contract-drift:
	go test ./cmd/api -run TestOpenAPIContainsAllContractOwnedRoutes_OneWayPolicy -count=1

rules-check:
	go run ./cmd/rulecheck

benchmark-hot:
	go test -run=^$$ -bench=. -benchmem $(BENCH_HOT_PKGS)

benchmark-query:
	go test -run=^$$ -bench=BenchmarkService -benchmem ./internal/query

benchmark-ws:
	go test -run=^$$ -bench=BenchmarkWSGateway -benchmem ./internal/api_primal

benchmark-replay-derivation:
	go test -run=^$$ -bench=BenchmarkLoadFixtureFile -benchmem ./internal/replay && \
	go test -run=^$$ -bench='Benchmark(DeriveEventReferences|NormalizeUniqueIDs)$$' -benchmem ./internal/derivation

benchmark-protected:
	go test -run=^$$ -bench='BenchmarkService(GetThreadWindow|GetUserInfos|GetEventBatch)$$' -benchmem -count=5 ./internal/query && \
	go test -run=^$$ -bench='BenchmarkWSGatewayDispatchCacheCall(ThreadView|UserInfos)$$' -benchmem -count=5 ./internal/api_primal && \
	go test -run=^$$ -bench='Benchmark(LoadFixtureFile|DeriveEventReferences|NormalizeUniqueIDs)$$' -benchmem -count=5 ./internal/replay ./internal/derivation && \
	go test -run=^$$ -bench='BenchmarkComputeIterativeGlobalRankLarge$$' -benchmem -count=5 ./internal/trust

perf-collect:
	bash ./scripts/perf_collect.sh

perf-protect-collect:
	PERF_BENCH_COUNT=5 PERF_COLLECT_SCOPE=protected PERF_OUTPUT_BASE=benchmarks/history/protection bash ./scripts/perf_collect.sh

perf-protect-compare:
	@test -n "$(BASELINE_DIR)" || (echo "BASELINE_DIR is required, e.g. benchmarks/history/protection/<run-id>" && exit 1)
	@test -n "$(CURRENT_DIR)" || (echo "CURRENT_DIR is required, e.g. benchmarks/history/protection/<run-id>" && exit 1)
	REGRESSION_THRESHOLD_PCT=$(or $(REGRESSION_THRESHOLD_PCT),15) \
	ENFORCEMENT_MODE=$(or $(ENFORCEMENT_MODE),advisory) \
	FAIL_ON_REGRESSION=$(or $(FAIL_ON_REGRESSION),0) \
	bash ./scripts/perf_protect_compare.sh "$(BASELINE_DIR)" "$(CURRENT_DIR)" benchmarks/compare

loadtest:
	@echo "Available load-test targets:"
	@echo "  make loadtest-api"
	@echo "  make loadtest-worker"
	@echo "  make loadtest-ingest"
	@echo "  make loadtest-replay-rebuild"
	@echo "  make loadtest-ws-api"
	@echo ""
	@echo "Environment knobs are documented in loadtest/README.md."

loadtest-api:
	bash ./loadtest/run.sh api-read-pressure

loadtest-worker:
	bash ./loadtest/run.sh worker-throughput-pressure

loadtest-ingest:
	bash ./loadtest/run.sh ingest-throughput-pressure

loadtest-replay-rebuild:
	bash ./loadtest/run.sh replay-rebuild-pressure

loadtest-ws-api:
	bash ./loadtest/run.sh ws-api-pressure

# Run every Fuzz* target in the WS/API packages for a short bounded budget.
# Override the per-target budget with FUZZTIME (e.g. FUZZTIME=60s make fuzz).
fuzz:
	FUZZTIME=$(or $(FUZZTIME),20s) bash ./scripts/fuzz_all.sh

build:
	mkdir -p "$(BUILD_OUTPUT_DIR)"
	go build -ldflags "$(LDFLAGS_API)" -o "$(BUILD_OUTPUT_DIR)/api" ./cmd/api
	go build -ldflags "$(LDFLAGS_INGESTOR)" -o "$(BUILD_OUTPUT_DIR)/ingestor" ./cmd/ingestor
	go build -ldflags "$(LDFLAGS_WORKER)" -o "$(BUILD_OUTPUT_DIR)/worker" ./cmd/worker
	go build -ldflags "$(LDFLAGS_TRUST_WORKER)" -o "$(BUILD_OUTPUT_DIR)/trust_worker" ./cmd/trust_worker

mod-verify:
	go mod verify

vulncheck:
	$(GOVULNCHECK) ./...

configdoc:
	go run ./cmd/configdoc

configdoc-check:
	go run ./cmd/configdoc -check

# sqlc pilot (internal/store/account). `sqlc` regenerates the accountdb package
# from queries + schema; `sqlc-check` fails CI when the committed generated code
# has drifted from the queries/schema (mirrors configdoc-check).
sqlc:
	$(SQLC) generate

sqlc-check:
	$(SQLC) diff

fmt:
	gofmt -w .

fmt-check:
	@files=$$(gofmt -l .); \
	if [ -n "$$files" ]; then \
		echo "gofmt changes needed:"; \
		echo "$$files"; \
		exit 1; \
	fi

imports:
	$(GOIMPORTS) -w .

imports-check:
	@files=$$($(GOIMPORTS) -l .); \
	if [ -n "$$files" ]; then \
		echo "goimports changes needed:"; \
		echo "$$files"; \
		exit 1; \
	fi

format: fmt imports

ci: fmt-check imports-check lint-ci mod-verify vulncheck test-race-policy cover coverage-policy contract-drift rules-check configdoc-check sqlc-check build
