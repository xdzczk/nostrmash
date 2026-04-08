# Primal Compatibility Matrix

Use this page as the current compatibility inventory. If you need operational guidance, use [compatibility_rollout.md](compatibility_rollout.md); if you need policy, use [compatibility.md](compatibility.md).

The matrix below describes what NostrMash implements today. Use it as the source of truth for the currently supported legacy-shaped compatibility surface in this repository.

## How to read this matrix

- Use `supported_now` as the present-tense repository truth.
- Use [compatibility_rollout.md](compatibility_rollout.md) for operational guidance rather than feature availability.

## Function-level matrix

| Primal capability | Status | NostrMash surface | Notes |
| --- | --- | --- | --- |
| `events` by id | supported_now | `GET /primal/v1/events/{id}` | Canonical payload passthrough. |
| `events` batch | supported_now | `POST /primal/v1/events/batch` | Returns found and missing IDs. |
| `user_profile` | supported_now | `GET /primal/v1/profiles/{pubkey}` | Uses projected profile state. |
| `user_infos` | supported_now | `POST /primal/v1/user_infos` | Batch profile lookup on projected state. |
| `thread_view` | supported_now | `GET /primal/v1/threads/{eventId}` | Includes cursor pagination via `cursor`/`next_cursor`. |
| feed | supported_now | `REQ cache:feed` | Compatibility cache dispatch supports feed-style reads on the WebSocket boundary. |
| author events feed | supported_now | `GET /primal/v1/authors/{pubkey}/events` | Maps to `author_recent_events`. |
| author replies | supported_now | `GET /primal/v1/authors/{pubkey}/replies` | Maps to derived reply references. |
| event action counts | supported_now | `GET /primal/v1/events/{id}/actions` | Derived from reply/reaction/repost counts. |
| contact list | supported_now | `GET /primal/v1/contact-lists/{pubkey}` | Uses `contact_lists_latest`. |
| relay list | supported_now | `GET /primal/v1/relay-lists/{pubkey}` | Uses `relay_lists_latest`. |
| protocol `REQ`/`CLOSE` | supported_now | `GET /primal/ws` | Gateway handles subscription and dispatch. |
| cache func dispatch | supported_now | `REQ` with `cache` payload | Unsupported calls return explicit `unsupported`. |
| direct messages (compat) | supported_now | `REQ cache:get_directmsgs` and `POST /primal/v1/dms/*` | WS cache calls plus strict-parity HTTP wrappers for DM message/contact/count/reset helpers. |
| mentions (compat) | supported_now | `REQ cache:user_mentions` | Reference-based p-tag mentions. |
| followers (compat) | supported_now | `REQ cache:user_followers` | Backed by `follower_edges` derived from latest kind:3 contact lists. |
| search | supported_now | `REQ search` and `REQ cache:search` | Ranked Postgres-first search with unified payload shape. |
| zaps | supported_now | `REQ cache:user_zaps*` and `REQ cache:event_zaps_by_satszapped` | Sender/receiver/event zap query paths with sats ranking. |
| social graph helpers | supported_now | `REQ cache:is_user_following` and `REQ cache:mutual_follows` | Uses durable follower-edge projection. |
| moderation helpers | supported_now | `REQ cache:mutelist` / `mutelists` / `allowlist` / `is_hidden_by_content_moderation` / `search_filterlist` | Replaceable-list backed semantics with explanation payloads. |
| bookmarks/highlights | supported_now | native + compatibility WS cache calls | Replaceable and kind-backed product reads. |
| long-form reads | supported_now | native + compatibility WS cache calls | Long-form and parameterized replaceable query surfaces. |
| curated/external parity subset | supported_now | `network_stats`, `server_name`, `get_recommended_reads`, `get_reads_topics`, `get_featured_authors`, `creator_paid_tiers`, `user_of_ln_address` | Reads/topics/authors/LN lookup use compatibility kind envelopes (`10000145`/`146`/`148`/`138`); `creator_paid_tiers` prefers event-native kind-`17000` + referenced tier events with curated (`10000147`) fallback; featured-authors and LN lookup include profile metadata events when available. |
| DMs over native HTTP | supported_now | `POST /primal/v1/dms/messages`, `/contacts`, `/count`, `/count2`, `/reset-count`, `/reset-counts` | Strict-parity POST wrappers around legacy cache DM semantics; intended for compatibility, not as a new native DM product API. |

## Required behavior contract

For all `supported_now` capabilities:

- deterministic ordering for list responses
- explicit pagination limits and cursor behavior
- stable error envelope with machine-readable `error.code`
- request-id propagation in all error responses
- fixture/golden coverage for contract-owned HTTP routes and targeted WebSocket/cache flows, with the checked-in fixtures representing the currently audited compatibility cases
