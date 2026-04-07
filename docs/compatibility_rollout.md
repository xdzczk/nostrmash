# Compatibility Rollout Plan

Use this page when you are planning adoption, not when you are checking current support. For current availability, use [primal_compatibility_matrix.md](primal_compatibility_matrix.md). This document is the staged rollout runbook for replacing Primal Cache with NostrMash compatibility surfaces.

## Stage summary

| Stage | Purpose | Main question |
| --- | --- | --- |
| Stage 1 | establish HTTP subset | do the core routes behave correctly and predictably? |
| Stage 2 | establish WS gateway subset | is protocol behavior stable under realistic load? |
| Stage 3 | mirror real traffic | do supported flows still match production expectations? |
| Stage 4 | migrate selected clients | can we hold SLOs with live user traffic? |
| Stage 5 | make it the default | are the remaining gaps either closed or explicitly non-blocking? |

## Stage 1: HTTP compatibility subset

Enable the first compatibility subset over HTTP:

- events by id and batch
- profile by pubkey and batch infos
- thread view
- author events/replies
- event action counts

Exit criteria:

- golden contract tests pass for every supported endpoint
- native API behavior remains unchanged
- compatibility error budget is defined and monitored

## Stage 2: WebSocket gateway subset

Enable `GET /primal/ws` gateway for:

- `REQ` / `CLOSE` protocol handling
- cache dispatch for the initial cutover subset first, while allowing the repository to carry broader compatibility handlers behind the same boundary
- `EVENT`, `EOSE`, and `NOTICE` responses

Exit criteria:

- stable connection lifecycle under test load
- bounded subscriptions per connection
- per-function latency and error metrics visible

## Stage 3: Shadow traffic

Mirror a representative slice of cache traffic:

- compare payload shape and ordering against legacy service
- track unsupported-function notices
- tune query timeouts and limits

Exit criteria:

- no unknown regressions in supported functions
- unsupported surface is explicitly acknowledged by clients

## Stage 4: Partial production migration

Move selected clients or routes to NostrMash compatibility:

- start with read-only and low-risk features
- gate on SLOs and fallback readiness

Exit criteria:

- sustained SLO compliance
- no recurring protocol failures
- operational runbooks validated by on-call

## Stage 5: Full migration

Promote NostrMash compatibility as the default path after:

- all must-have functions are implemented
- deferred functions are either shipped or intentionally retired
- rollback and incident handling are documented and tested

Implementation note:

- This plan describes rollout sequencing, not a strict list of everything already implemented in the repository.
- When rollout sequencing and the compatibility matrix differ, treat `primal_compatibility_matrix.md` as the source of truth for current feature availability and this document as the operational adoption plan.
