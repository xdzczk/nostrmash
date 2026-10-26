# Primal Compatibility Matrix

Use this page as the current compatibility inventory. If you need operational guidance, use [compatibility_rollout.md](compatibility_rollout.md); if you need policy, use [compatibility.md](compatibility.md).

The matrix below describes what NostrMash implements today. `GET /primal/ws` and `/primal/v1/*` keep the legacy shapes. `GET /primal/ws/v1` is the Primal iOS cache protocol.

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

## `/primal/ws/v1`

Point the iOS app at Settings → Network → caching service → `wss://<host>/primal/ws/v1`. The legacy socket at `/primal/ws` is unchanged.

Statuses: `supported_now` answers in the Primal kind shape, `stubbed` returns an empty EOSE (or a free-tier payload where the app needs one) so the screen degrades quietly, `deferred` is not registered.

| Funcall | Status | Notes |
| --- | --- | --- |
| `set_primal_protocol` | supported_now | `compression: zlib` switches that connection's EVENT batches to one binary `zlib(["EVENTS", sub, [msgs]])` frame. NOTICE and EOSE stay text. |
| `client_config`, `get_default_app_settings`, `get_default_relays`, `server_name` | supported_now | Kinds `10000162`, `10000103`, `10000124`. Settings JSON defaults ship in-repo; override with `PRIMAL_DEFAULT_SETTINGS_PATH`, `PRIMAL_CLIENT_CONFIG_PATH`, `PRIMAL_DEFAULT_RELAYS`. |
| `get_app_settings`, `get_app_settings_2`, `set_app_settings`, subsettings | supported_now | Signed kind `30078`. Stored in `primal_app_settings` / `primal_app_subsettings`. |
| `user_profile` | supported_now | Raw kind `0` plus kind `10000105`. |
| `user_infos`, `user_search`, `get_recommended_users` | supported_now | Kind `0` events plus follower maps `10000108` and `10000133`. |
| `is_user_following` | supported_now | Kind `10000125`, content `true` or `false`. `pubkey` is the follower and `user_pubkey` is the followed account. |
| `contact_list` | supported_now | Raw kind `3`, kind `0`s of follows, follower maps. `extended_response` defaults to true. |
| `get_user_relays`, `get_user_relays_2` | supported_now | Kind `10000139` with `r` tags, including read/write markers. |
| `replaceable_event`, `follow_list`, `parametrized_replaceable_event` | supported_now | Raw replaceable event. Follow lists add user infos. |
| `mega_feed_directive`, `multi_kind_mega_feed_directive`, `feed` | supported_now | `latest` and `feed` notes modes `follows`, `authored`, `replies`, `bookmarks`, `user_media_thumbnails`. `global-trending`, `all-notes`, and `most-zapped` use discovery scores. `advsearch` uses note search. Reads specs use long-form. |
| `explore*`, `video-reels`, `wide-net-notes`, `strange-but-popular` | stubbed | Empty page plus a kind `10000113` RANGE. |
| `thread_view`, `multi_kind_thread_view` | supported_now | Replies, then root and ancestors. Each pass ends with its own RANGE whose `elements` are the ids to render, newest first. |
| `events` | supported_now | `extended_response` runs the feed enrichment tail (kind `0`, `10000100`, `10000107`, `10000129`, `10000108`, `10000133`, `10000141`, RANGE). |
| `get_notifications`, seen, `notification_counts`, `notification_counts_2` | supported_now | Kinds `10000110`, `10000111`, `10000112`, `10000132`. Counts are a 1s plaintext push. Types 1, 3, 4, 5, 6, 7, 8, and 601 are derived with each event. Types 101–204, 301, 302, 401, and 501 are deferred. |
| `user_search`, `search`, `advanced_search` | supported_now | Search results go through enrichment and end with a RANGE. |
| `user_followers`, `user_mentions`, `user_zaps*` | supported_now | Events, metadata, zap summaries (`10000129`), and a RANGE. |
| `get_directmsg_contacts`, `get_directmsgs` | supported_now | Accepts `user_pubkey` and `receiver`/`sender`. RANGE `elements` preserve order. |
| `get_media_metadata` | supported_now | Kind `10000119` from `imeta` tags and stored rows. Network fetch is off unless `PRIMAL_MEDIA_FETCH_ENABLED=true`. |
| `broadcast_events`, `broadcast_reply` | supported_now | Validates, stores locally, publishes to the requested relays, returns kind `10000149`. |
| `nip19_decode`, `trending_hashtags*` | supported_now | Kinds `10000173` and `10000160`. |
| `membership_status` | stubbed | Kind `100001603` with `{"premium":false,"tier":"free"}`. |
| `membership_*`, push tokens, NIP-46 queue, DVM feeds, live feeds, `poll_votes`, explore lists, `import_events`, reports | stubbed | Empty EOSE. No wallet socket, premium purchase, or APNs delivery. |
| TrustRank humaness scoring | deferred | Trust-tier filtering stays where the native API already has it. |

Feed and thread answers end with kind `10000113` `{"since","until","order_by","elements"}`. Clients render only the ids in `elements`, in that order.

`PRIMAL_WS_V1_ENABLED` defaults to true. `PRIMAL_WS_V1_SERVER_PUBKEY` is the viewer used when a mega-feed or thread call omits `user_pubkey`. `PRIMAL_WS_MAX_BROADCAST_PER_MINUTE` defaults to 30.

## Required behavior contract

For all `supported_now` capabilities:

- deterministic ordering for list responses
- explicit pagination limits and cursor behavior
- stable error envelope with machine-readable `error.code`
- request-id propagation in all error responses
- fixture/golden coverage for contract-owned HTTP routes and targeted WebSocket/cache flows, with the checked-in fixtures representing the currently audited compatibility cases
