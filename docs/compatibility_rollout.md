# Compatibility Rollout Plan

This runbook defines staged rollout and exit criteria for replacing Primal Cache with NostrMash compatibility surfaces.

## Stage 1: HTTP Compatibility Subset

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

## Stage 2: WebSocket Gateway (Subset)

Enable `GET /primal/ws` gateway for:

- `REQ` / `CLOSE` protocol handling
- cache dispatch for the Stage 1 compatibility subset
- `EVENT`, `EOSE`, and `NOTICE` responses

Exit criteria:

- stable connection lifecycle under test load
- bounded subscriptions per connection
- per-function latency and error metrics visible

## Stage 3: Shadow Traffic

Mirror a representative slice of cache traffic:

- compare payload shape and ordering against legacy service
- track unsupported-function notices
- tune query timeouts and limits

Exit criteria:

- no unknown regressions in supported functions
- unsupported surface is explicitly acknowledged by clients

## Stage 4: Partial Production Migration

Move selected clients or routes to NostrMash compatibility:

- start with read-only and low-risk features
- gate on SLOs and fallback readiness

Exit criteria:

- sustained SLO compliance
- no recurring protocol failures
- operational runbooks validated by on-call

## Stage 5: Full Migration

Promote NostrMash compatibility as the default path after:

- all must-have functions are implemented
- deferred functions are either shipped or intentionally retired
- rollback and incident handling are documented and tested
