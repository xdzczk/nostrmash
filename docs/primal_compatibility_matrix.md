# Primal Compatibility Matrix

Use this page as the current compatibility inventory. If you need rollout sequencing, use [compatibility_rollout.md](compatibility_rollout.md); if you need policy, use [compatibility.md](compatibility.md).

The matrix below describes what NostrMash implements today, what is planned next, and what is intentionally deferred. The later "Frozen cutover scope (V1)" section is narrower on purpose: it captures the first production migration bundle, not the full currently implemented surface.

## How to read this matrix

- Use `supported_now` as the present-tense repository truth.
- Use the "Frozen cutover scope (V1)" section when deciding what must block the first production migration.
- Use [compatibility_rollout.md](compatibility_rollout.md) when the question is sequencing rather than availability.

## Classification legend

- `supported_now`: implemented and covered by tests in this repository.
- `phase1_http`: targeted first via HTTP compatibility endpoints.
- `phase2_ws`: delivered via WebSocket gateway protocol compatibility.
- `deferred`: planned, but not required for initial cutover.
- `unsupported`: intentionally out of scope for now.

## Function-level matrix

| Primal capability | Status | NostrMash surface | Notes |
| --- | --- | --- | --- |
| `events` by id | supported_now | `GET /primal/v1/events/{id}` | Canonical payload passthrough. |
| `events` batch | supported_now | `POST /primal/v1/events/batch` | Returns found and missing IDs. |
| `user_profile` | supported_now | `GET /primal/v1/profiles/{pubkey}` | Uses projected profile state. |
| `user_infos` | supported_now | `POST /primal/v1/user_infos` | Batch profile lookup on projected state. |
| `thread_view` | supported_now | `GET /primal/v1/threads/{eventId}` | Includes cursor pagination via `cursor`/`next_cursor`. |
| author events feed | supported_now | `GET /primal/v1/authors/{pubkey}/events` | Maps to `author_recent_events`. |
| author replies | supported_now | `GET /primal/v1/authors/{pubkey}/replies` | Maps to derived reply references. |
| event action counts | supported_now | `GET /primal/v1/events/{id}/actions` | Derived from reply/reaction/repost counts. |
| contact list | supported_now | `GET /primal/v1/contact-lists/{pubkey}` | Uses `contact_lists_latest`. |
| relay list | supported_now | `GET /primal/v1/relay-lists/{pubkey}` | Uses `relay_lists_latest`. |
| protocol `REQ`/`CLOSE` | supported_now | `GET /primal/ws` | Gateway handles subscription and dispatch. |
| cache func dispatch | supported_now | `REQ` with `cache` payload | Unsupported calls return explicit `unsupported`. |
| direct messages (compat) | supported_now | `REQ cache:get_directmsgs` | Compatibility-only WS method with DM peer/count/reset helpers. |
| mentions (compat) | supported_now | `REQ cache:user_mentions` | Reference-based p-tag mentions. |
| followers (compat) | supported_now | `REQ cache:user_followers` | Backed by `follower_edges` derived from latest kind:3 contact lists. |
| search | supported_now | `REQ search` and `REQ cache:search` | Ranked Postgres-first search with unified payload shape. |
| zaps | supported_now | `REQ cache:user_zaps*` and `REQ cache:event_zaps_by_satszapped` | Sender/receiver/event zap query paths with sats ranking. |
| social graph helpers | supported_now | `REQ cache:is_user_following` and `REQ cache:mutual_follows` | Uses durable follower-edge projection. |
| moderation helpers | supported_now | `REQ cache:mutelist` / `allowlist` / `is_hidden_by_content_moderation` / `search_filterlist` | Replaceable-list backed semantics with explanation payloads. |
| bookmarks/highlights | supported_now | native + compatibility WS cache calls | Replaceable and kind-backed product reads. |
| long-form reads | supported_now | native + compatibility WS cache calls | Long-form and parameterized replaceable query surfaces. |
| curated/external parity subset | supported_now | `network_stats`, `server_name`, `get_recommended_reads`, `get_reads_topics`, `get_featured_authors`, `creator_paid_tiers`, `user_of_ln_address` | Reads/topics/authors/LN lookup use compatibility kind envelopes (`10000145`/`146`/`148`/`138`); `creator_paid_tiers` prefers event-native kind-`17000` + referenced tier events with curated (`10000147`) fallback; featured-authors and LN lookup include profile metadata events when available. |
| DMs over native HTTP | unsupported | none | Separate security/privacy scope from public reads. |

## Required behavior contract

For all `supported_now`, `phase1_http`, and `phase2_ws` capabilities:

- deterministic ordering for list responses
- explicit pagination limits and cursor behavior
- stable error envelope with machine-readable `error.code`
- request-id propagation in all error responses
- fixture/golden test coverage for response shape

## Initial cutover gate

Initial Primal cutover can proceed only when all are true:

1. All `supported_now` and `phase1_http` rows are implemented and contract-tested.
2. WebSocket gateway supports event/profile/thread/author/count plus DM/social/moderation/search/zap dispatch.
3. Compatibility metrics and error rates are observable in production.
4. Unknown cache functions fail with explicit `unsupported` notices, not silent drops.

## Frozen cutover scope (V1)

The first production cutover is explicitly limited to this smaller deployment bundle, even though the repository implements additional compatibility functions today:

- `events` by id and batch
- `user_profile` and `user_infos`
- `thread_view`
- author events and replies
- event action counts
- contact list and relay list reads
- WebSocket compatibility for the request types above

Everything else outside this list must not block the first cutover, even if it is already implemented behind the compatibility boundary.
