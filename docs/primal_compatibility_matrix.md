# Primal Compatibility Matrix

This document defines the explicit compatibility target for replacing Primal Cache in phases.

The matrix below is the contract for what NostrMash supports today, what is planned in the next migration phases, and what is intentionally deferred.

## Classification Legend

- `supported_now`: implemented and covered by tests in this repository.
- `phase1_http`: targeted first via HTTP compatibility endpoints.
- `phase2_ws`: delivered via WebSocket gateway protocol compatibility.
- `deferred`: planned, but not required for initial cutover.
- `unsupported`: intentionally out of scope for now.

## Function-Level Matrix

| Primal capability | Status | NostrMash surface | Notes |
| --- | --- | --- | --- |
| `events` by id | supported_now | `GET /primal/v1/events/{id}` | Canonical payload passthrough. |
| `events` batch | supported_now | `POST /primal/v1/events/batch` | Returns found and missing IDs. |
| `user_profile` | supported_now | `GET /primal/v1/profiles/{pubkey}` | Uses projected profile state. |
| `user_infos` | phase1_http | `POST /primal/v1/user_infos` | Maps to projected profiles batch lookup. |
| `thread_view` | phase1_http | `GET /primal/v1/threads/{event_id}` | Maps to native thread/reply/ancestor reads. |
| author events feed | phase1_http | `GET /primal/v1/authors/{pubkey}/events` | Maps to `author_recent_events`. |
| author replies | phase1_http | `GET /primal/v1/authors/{pubkey}/replies` | Maps to derived reply references. |
| event action counts | phase1_http | `GET /primal/v1/events/{id}/actions` | Derived from reply/reaction/repost counts. |
| contact list | phase1_http | `GET /primal/v1/contact-lists/{pubkey}` | Uses `contact_lists_latest`. |
| relay list | phase1_http | `GET /primal/v1/relay-lists/{pubkey}` | Uses `relay_lists_latest`. |
| protocol `REQ`/`CLOSE` | phase2_ws | `GET /primal/ws` | Gateway handles subscription and dispatch. |
| cache func dispatch | phase2_ws | `REQ` with `cache` payload | Supported funcs map to shared query facade. |
| social graph enrichment | deferred | future | Followers/mutuals require additional projections. |
| search | deferred | future | Requires explicit ranking/indexing decisions. |
| zaps | deferred | future | Requires dedicated derivations and event links. |
| moderation helpers | deferred | future | Policy and data model not finalized. |
| bookmarks/highlights | deferred | future | Requires dedicated storage/query semantics. |
| long-form reads | deferred | future | Requires topic/read model semantics. |
| DMs | unsupported | none | Separate security/privacy scope from public reads. |

## Required Behavior Contract

For all `supported_now`, `phase1_http`, and `phase2_ws` capabilities:

- deterministic ordering for list responses
- explicit pagination limits and cursor behavior
- stable error envelope with machine-readable `error.code`
- request-id propagation in all error responses
- fixture/golden test coverage for response shape

## Initial Cutover Gate

Initial Primal cutover can proceed only when all are true:

1. All `supported_now` and `phase1_http` rows are implemented and contract-tested.
2. WebSocket gateway supports at least event/profile/thread/author/count dispatch.
3. Compatibility metrics and error rates are observable in production.
4. Unknown cache functions fail with explicit `unsupported` notices, not silent drops.

## Frozen Cutover Scope (V1)

The first production cutover is explicitly limited to:

- `events` by id and batch
- `user_profile` and `user_infos`
- `thread_view`
- author events and replies
- event action counts
- contact list and relay list reads
- WebSocket compatibility for the request types above

Everything else in `deferred` or `unsupported` is out of scope for V1 and must not block cutover.
