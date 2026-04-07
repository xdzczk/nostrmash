# Security Policy

Use this page for security reporting and handling posture. It is intentionally short: where to report, what belongs in a report, and how the project responds.

## Reporting a security issue

Please do **not** open public GitHub issues for suspected vulnerabilities.

Report privately by emailing: `security@nostrmash.dev`.

If you do not receive acknowledgement within 3 business days, use GitHub's private vulnerability reporting flow for this repository ("Security" -> "Report a vulnerability") and include the original report timestamp.

Include:

- affected component(s) and version/commit
- reproduction steps or proof-of-concept
- impact assessment (confidentiality/integrity/availability)
- any suggested mitigation

We will acknowledge receipt and triage as quickly as possible, then coordinate disclosure timing with the reporter.

Maintainer confirmation item:

- confirm `security@nostrmash.dev` is actively monitored before each release; if ownership changes, update this policy in the same release cycle.

## What belongs in security reporting

Examples:

- auth/admin bypass or privilege escalation
- sensitive data exposure (tokens, private material, unsafe debug exposure)
- injection issues (SQL, command, template, etc.)
- denial-of-service vectors with realistic abuse potential
- dependency vulnerabilities with practical impact on this repo

Non-security bugs (correctness/performance regressions without security impact) should go through normal issue/PR flow.

## Handling posture

- prioritize fixes that reduce exploitability quickly
- prefer minimally risky patches for initial response
- backport/patch supported release lines when feasible
- document operational mitigations when a full fix needs extra time

When relevant, release notes should include remediation guidance and upgrade urgency.

## Dependency hygiene

Dependency update posture is security-relevant, but it does not need a separate top-level doc.

Automatically updated:

- Go modules (`go.mod`, `go.sum`) via Dependabot `gomod` updates
- GitHub Actions referenced in workflow `uses:` steps via Dependabot `github-actions` updates
- Container base images in Dockerfiles via Dependabot `docker` updates

Intentionally manual:

- workflow service/container images declared inside workflow YAML
- Compose service images such as `postgres:16-alpine`
- pinned security tooling versions outside `go.mod`
- risk-significant major version upgrades that need explicit reviewer sign-off

Validation expectations for dependency PRs:

1. Run CI and require green status before merge.
2. Review changelog or release notes for security fixes and breaking changes.
3. For Go module changes, ensure tests and vuln checks pass.
4. For GitHub Action changes, verify workflow behavior and permissions still match least-privilege expectations.
5. For container base image changes, rebuild images and run smoke checks (`/health`, `/ready`, critical API paths) before merge.
