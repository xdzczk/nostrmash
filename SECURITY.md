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
