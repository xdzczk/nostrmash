.PHONY: lint test test-race cover build mod-verify vulncheck ci

RACE_PKGS := ./internal/jobs ./internal/store ./internal/ingestor/...
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/cmd/golangci-lint@v1.64.8
GOVULNCHECK := go run golang.org/x/vuln/cmd/govulncheck@v1.1.4

lint:
	$(GOLANGCI_LINT) run --config .golangci.yml

test:
	go test ./...

test-race:
	go test -race $(RACE_PKGS)

cover:
	go test -covermode=atomic -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

build:
	go build ./cmd/api ./cmd/ingestor ./cmd/worker

mod-verify:
	go mod verify

vulncheck:
	$(GOVULNCHECK) ./...

ci: lint mod-verify vulncheck test-race cover build
