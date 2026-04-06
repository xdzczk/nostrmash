# Security Dependency Hygiene

This repository uses dependency automation for the dependency surfaces that are first-class and machine-updatable in this codebase.

## Automatically Updated

- **Go modules (`go.mod`, `go.sum`)** via Dependabot `gomod` updates.
- **GitHub Actions** versions referenced in workflow `uses:` steps via Dependabot `github-actions` updates.
- **Container base images in Dockerfiles** (for example the root `Dockerfile`) via Dependabot `docker` updates.

Dependabot runs weekly and opens grouped PRs so update volume stays reviewable.

## Intentionally Manual (By Design)

- **Workflow service/container images** declared directly in workflow YAML `services.image` (for example CI Postgres) are reviewed manually.
- **Compose service images** (for example `postgres:16-alpine` in `docker-compose.yml`) are reviewed manually.
- **Pinned security tooling versions outside `go.mod`** (for example `govulncheck` pinned in `Makefile`) are reviewed and bumped manually.
- **Risk-significant major version upgrades** are never auto-merged; they require explicit reviewer sign-off.
- **Transitive runtime behavior changes** (database engine behavior, libc/openssl changes in base images, runner environment drift) still require human validation even when the version bump is automated.

## Validation Expectations For Dependency PRs

For every dependency update PR:

1. Run CI and require green status before merge.
2. Review changelog/release notes for security fixes and breaking changes.
3. For **Go module** changes, ensure tests and vuln checks pass.
4. For **GitHub Action** changes, verify workflow behavior and permissions still match least-privilege expectations.
5. For **container base image** changes, rebuild images and run smoke checks (`/health`, `/ready`, critical API paths) before merge.

For higher-risk updates (major versions, base image family changes, runtime/toolchain changes), perform a staged rollout and keep rollback ready before production promotion.
