# Orchestration Surfaces

Use this page when a change crosses transport boundaries. It maps the current read-side assembly paths exposed through native HTTP, Primal-compatible HTTP, and Primal-compatible WebSocket surfaces so you can see where orchestration actually lives today.

For performance prioritization and benchmark/load-test targets tied to these surfaces, see [../performance.md](../performance.md).
For compatibility/deprecation expectations on these external surfaces, see [../compatibility.md](../compatibility.md).
For validation expectations before shipping changes on these paths, see [../testing.md](../testing.md) and [../../RELEASE.md](../../RELEASE.md).

Scope is limited to the code currently wired from `cmd/api/main.go` into:

- `internal/api/handlers.go`
- `internal/api_primal/handlers.go`
- `internal/api_primal/ws_gateway.go`
- `internal/query/service.go`

No redesign is proposed here. The goal is to describe what runs today.

## How contributors should use this document

Use this page as a map before changing handlers/gateway/query code:

1. Identify your entry surface (native HTTP, Primal HTTP, or Primal WS).
2. Confirm where orchestration actually lives for that route/request kind.
3. Keep transport shaping in transport packages and shared read assembly in `internal/query` where applicable.
4. If you move ownership boundaries, update this document in the same PR.

High-risk edits are those that change shared thread/event/profile assembly semantics across multiple surfaces. Treat those as compatibility-sensitive changes and stage validation accordingly.

## Route-level entry points

`cmd/api/routes.go` is the route source of truth. `cmd/api/main.go` registers from it, and `cmd/api/contract_drift_test.go` validates contract-owned routes against `docs/openapi.yaml`:

- Native HTTP:
  - `GET /api/v1/events/{id}` -> `internal/api.Handlers.GetEventByID`
  - `POST /api/v1/events/batch` -> `internal/api.Handlers.BatchGetEvents`
  - `GET /api/v1/profiles/{pubkey}` -> `internal/api.Handlers.GetProfileByPubkey`
  - `POST /api/v1/profiles/batch` -> `internal/api.Handlers.BatchGetProfiles`
  - `GET /api/v1/events/{id}/replies` -> `internal/api.Handlers.GetEventReplies`
  - `GET /api/v1/events/{id}/ancestors` -> `internal/api.Handlers.GetEventAncestors`
  - `GET /api/v1/threads/{eventId}` -> `internal/api.Handlers.GetThread`
  - `GET /api/v1/search` -> `internal/api.Handlers.Search`
- Primal HTTP:
  - `GET /primal/v1/events/{id}` -> `internal/api_primal.Handlers.GetEventByID`
  - `POST /primal/v1/events/batch` -> `internal/api_primal.Handlers.BatchGetEvents`
  - `GET /primal/v1/profiles/{pubkey}` -> `internal/api_primal.Handlers.GetProfileByPubkey`
  - `POST /primal/v1/user_infos` -> `internal/api_primal.Handlers.BatchGetUserInfos`
  - `GET /primal/v1/threads/{eventId}` -> `internal/api_primal.Handlers.GetThreadView`
- Primal WebSocket:
  - `GET /primal/ws` -> `internal/api_primal.WSGateway.Handle`
  - Request dispatch inside the socket is driven by `internal/api_primal.WSGateway.resolveFilter` and `internal/api_primal.WSGateway.dispatchCacheCall`

## Current authority boundary

- `internal/query/service.go` is the shared application-service layer for migrated read orchestration paths.
- Native HTTP and Primal HTTP are mixed:
  - migrated handlers delegate through `h.service` (`GetThread`, batch event/profile reads, profile reads, author/contact/relay reads)
  - non-migrated handlers still call store methods directly when behavior remains transport-specific
- Primal WebSocket constructs `query.Service` in `NewWSGateway` and delegates cache-call reads through `g.query`.
- WebSocket still owns compatibility-only stream shaping (metadata injection, synthetic range markers, moderation/DM response shaping), so transport-specific composition remains in gateway code.

## Thread assembly

### Native HTTP thread

- Transport entrypoint:
  - `GET /api/v1/threads/{eventId}` -> `internal/api.Handlers.GetThread`
- Downstream calls:
  - `query.NewService(h.store).GetThread`
  - local `encodeEventCursor`
- Where orchestration/business assembly happens:
  - In `internal/query.Service.GetThread`, which assembles focal event, ancestor chain, missing ancestor ids, and paged replies.
- Where transport-specific shaping happens:
  - In `internal/api/handlers.go` `GetThread`; request parsing, cursor decode/encode, status mapping, and final JSON envelope remain transport-side.
- Duplication status:
  - Native thread no longer does direct store orchestration in handler code.
  - Primal HTTP `GetThreadView` uses the same shared query thread orchestration path.

### Primal HTTP thread

- Transport entrypoint:
  - `GET /primal/v1/threads/{eventId}` -> `internal/api_primal.Handlers.GetThreadView`
- Downstream calls:
  - `query.NewService(h.store).GetThread`
  - local `encodeEventCursor`
  - `buildThreadViewResponse`
- Where orchestration/business assembly happens:
  - In `internal/query.Service.GetThread`, shared with native thread flow.
- Where transport-specific shaping happens:
  - In `internal/api_primal/handlers.go` `GetThreadView`; cursor parse/encode, HTTP status mapping, and Primal envelope (`buildThreadViewResponse`) remain transport-side.
- Duplication status:
  - No longer duplicates native thread business assembly; both HTTP surfaces now use `query.Service.GetThread`.
  - Primal-specific response shaping remains local to Primal transport.

### Primal WebSocket thread

- Transport entrypoint:
  - `GET /primal/ws` -> `WSGateway.Handle`
  - `REQ` frame with `cache: ["thread_view", kwargs]` -> `dispatchCacheCall("thread_view", ...)`
- Downstream calls:
  - `g.query.GetThreadWindow`
  - `buildThreadViewStream`
  - inside stream shaping: `buildMetadataEvents`, `rangeFromEvents`, `buildThreadRangeEvent`
- Where orchestration/business assembly happens:
  - Shared thread window assembly happens in `query.Service.GetThreadWindow`; it collects ascending reply pages, derives descending order, and applies cursor/offset windowing.
- Where transport-specific shaping happens:
  - `buildThreadViewStream` converts the assembled thread into a stream of WS events: replies first, then metadata events for participant profiles, then a synthetic range event, then ancestors and focal event.
- Duplication status:
  - This path now shares descending thread window orchestration via `query.Service.GetThreadWindow`.
  - It does not share the native/primal HTTP response builders.
  - It is the only thread path that injects profile metadata events and a synthetic range marker.

### Thread conclusion

- Native thread now uses a shared application-service method: `query.Service.GetThread`.
- That service is **not** the canonical thread path for all surfaces:
  - native HTTP uses it for `GET /api/v1/threads/{eventId}`
  - Primal HTTP uses it for `GET /primal/v1/threads/{eventId}`
  - Primal WebSocket uses `GetThreadWindow` for descending lookup assembly, but still owns stream framing/metadata/range shaping

## Event lookup and event assembly

### Single event lookup: native HTTP

- Transport entrypoint:
  - `GET /api/v1/events/{id}` -> `internal/api.Handlers.GetEventByID`
- Downstream calls:
  - `h.store.GetEventWithProvenance`
  - `json.Unmarshal` of stored event bytes
- Where orchestration/business assembly happens:
  - In `GetEventByID`; this is more than a raw fetch because it also assembles provenance relay data into a structured envelope.
- Where transport-specific shaping happens:
  - Same handler; it returns `{event, provenance, consistency}` for the native API.
- Duplication status:
  - No equivalent provenance-aware path exists in `internal/api_primal/handlers.go` or `query.Service`.
  - This path is native-only logic.

### Single event lookup: Primal HTTP

- Transport entrypoint:
  - `GET /primal/v1/events/{id}` -> `internal/api_primal.Handlers.GetEventByID`
- Downstream calls:
  - `h.store.GetEventRawByID`
- Where orchestration/business assembly happens:
  - Minimal orchestration in the handler: validate id, fetch raw event, wrap in `primalEventResponse`.
- Where transport-specific shaping happens:
  - In the handler itself; response shape is `{event: <raw event>}`.
- Duplication status:
  - Similar purpose to native `GetEventByID`, but not the same behavior because provenance is dropped and the store call is different.
  - `query.Service.GetEventByID` exists but is not used by either HTTP handler.

### Batch event lookup: native HTTP

- Transport entrypoint:
  - `POST /api/v1/events/batch` -> `internal/api.Handlers.BatchGetEvents`
- Downstream calls:
  - local request decode and de-duplication
  - `query.Service.GetEvents`
- Where orchestration/business assembly happens:
  - In `BatchGetEvents`; the handler normalizes ids, preserves request order, and computes the missing-id list.
- Where transport-specific shaping happens:
  - Same handler; response shape is `{events, missing}`.
- Duplication status:
  - Duplicated by `internal/api_primal.Handlers.BatchGetEvents` with only field-name differences.
  - Both HTTP handlers now delegate the fetch step to `query.Service.GetEvents`, while keeping normalization/order/missing shaping in transport.

### Batch event lookup: Primal HTTP

- Transport entrypoint:
  - `POST /primal/v1/events/batch` -> `internal/api_primal.Handlers.BatchGetEvents`
- Downstream calls:
  - local `sourceIDs` alias handling for `event_ids` vs `ids`
  - local `normalizeUniqueValues`
  - `query.Service.GetEvents`
- Where orchestration/business assembly happens:
  - In the handler; it performs the same normalize, fetch, preserve-order, and missing-list work as native HTTP.
- Where transport-specific shaping happens:
  - Same handler; response shape is `{events, missing_event_ids}`.
- Duplication status:
  - Direct duplicate of native batch orchestration with compatibility-specific request/response field names.

### Event lookup: Primal WebSocket

- Transport entrypoint:
  - `REQ` frame with top-level `ids`
  - `REQ` frame with `cache: ["events", {"event_ids": ...}]`
- Downstream calls:
  - `g.query.GetEventBatch`
- Where orchestration/business assembly happens:
  - Very little service-side orchestration; `query.Service.GetEventBatch` is effectively a pass-through to `reader.GetEventRawsByIDs`.
  - The gateway preserves client-requested id order before emitting events.
- Where transport-specific shaping happens:
  - `resolveFilter` and `dispatchCacheCall` convert results into `["EVENT", subID, rawEvent]` frames.
- Duplication status:
  - Duplicates the same underlying batch lookup capability, but in stream form.
  - Unlike HTTP, there is no missing-id payload; absent ids simply do not emit an event frame.

## Profile and user-info assembly

### Single profile: native HTTP

- Transport entrypoint:
  - `GET /api/v1/profiles/{pubkey}` -> `internal/api.Handlers.GetProfileByPubkey`
- Downstream calls:
  - `query.Service.GetProfile`
- Where orchestration/business assembly happens:
  - Minimal assembly in the handler; the returned `store.ProfileProjection` is copied into `profileResponse`.
- Where transport-specific shaping happens:
  - Same handler; response shape is `{pubkey, metadata_event_id, metadata_created_at, profile}`.
- Duplication status:
  - Duplicated by the Primal HTTP profile handler with only type naming differences.
  - Both HTTP profile handlers now use `query.Service.GetProfile`.

### Batch profiles: native HTTP

- Transport entrypoint:
  - `POST /api/v1/profiles/batch` -> `internal/api.Handlers.BatchGetProfiles`
- Downstream calls:
  - local normalization and de-duplication
  - `query.Service.GetProfiles`
- Where orchestration/business assembly happens:
  - In the handler; it normalizes input order, computes `missing_pubkeys`, and converts store projections to response structs.
- Where transport-specific shaping happens:
  - Same handler; response shape is `{profiles, missing_pubkeys}`.
- Duplication status:
  - Same orchestration is duplicated by Primal HTTP `BatchGetUserInfos`.
  - Both HTTP handlers now use `query.Service.GetProfiles` and keep only transport-specific request/response handling.

### Single profile: Primal HTTP

- Transport entrypoint:
  - `GET /primal/v1/profiles/{pubkey}` -> `internal/api_primal.Handlers.GetProfileByPubkey`
- Downstream calls:
  - `h.service.GetProfile`
- Where orchestration/business assembly happens:
  - Minimal handler-level mapping into `primalProfileResponse`.
- Where transport-specific shaping happens:
  - Same handler.
- Duplication status:
  - Mirrors native `GetProfileByPubkey` transport shaping while sharing profile lookup through `query.Service`.

### Batch user infos: Primal HTTP

- Transport entrypoint:
  - `POST /primal/v1/user_infos` -> `internal/api_primal.Handlers.BatchGetUserInfos`
- Downstream calls:
  - local `normalizeUniqueValues`
  - `query.Service.GetProfiles`
- Where orchestration/business assembly happens:
  - In the handler; same ordered profile assembly and missing-pubkey computation as native batch profiles.
- Where transport-specific shaping happens:
  - Same handler; response field names match Primal compatibility.
- Duplication status:
  - Duplicates native `BatchGetProfiles`.
  - Shared profile fetch/missing assembly now comes from `query.Service.GetProfiles`.

### Profile and metadata in Primal WebSocket

- Transport entrypoints:
  - `cache: ["user_profile", {"pubkey": ...}]`
  - `cache: ["user_infos", {"pubkeys": ...}]`
  - implicit metadata enrichment inside `buildMetadataEvents`
- Downstream calls:
  - `g.query.GetProfile`
  - `g.query.GetUserInfos`
  - `g.query.GetEventBatch` for metadata event ids
- Where orchestration/business assembly happens:
  - `query.Service.GetProfile` is just a direct reader pass-through.
  - `query.Service.GetUserInfos` performs ordered batch profile assembly.
  - `buildMetadataEvents` then performs an additional compatibility-specific second-stage assembly: it resolves metadata event ids from profile projections and fetches the raw metadata events themselves.
- Where transport-specific shaping happens:
  - `dispatchCacheCall` wraps single-profile and batch-profile results as Primal WS payload objects.
  - `buildMetadataEvents` emits raw kind-0 metadata events into other WS responses even when the original request was for threads, highlights, long-form items, DM contacts, or featured authors.
- Duplication status:
  - `user_profile` and `user_infos` reuse `query.Service`, but the metadata-event enrichment path is WS-only and bypasses any native/HTTP shared representation.

## Compatibility-specific response shaping paths

These paths are not just alternate transports; they reshape data specifically for Primal-compatible clients.

### Primal HTTP shaping

- `internal/api_primal.Handlers.BatchGetEvents`
  - accepts `event_ids` and transitional `ids`
  - emits `missing_event_ids` instead of native `missing`
- `internal/api_primal.Handlers.BatchGetUserInfos`
  - renames native batch profile semantics to Primal `user_infos`
- `internal/api_primal.Handlers.GetThreadView`
  - now delegates thread assembly to `query.Service.GetThread` and applies Primal envelope shaping via `buildThreadViewResponse`

Most Primal HTTP handlers still do their own orchestration instead of delegating to `query.Service`; `GetThreadView` is now the migrated exception.

### Primal WebSocket shaping

The WebSocket gateway contains the densest compatibility-only assembly:

- `resolveFilter`
  - maps top-level filter forms (`ids`, `search`, `cache`) to compatibility handlers
- `dispatchCacheCall`
  - switches on Primal request names such as `thread_view`, `user_profile`, `user_infos`, `feed`, `author_replies`, `event_actions`, `get_highlights`, `long_form_content_feed`, `get_directmsgs`, moderation requests, curated-list requests, and others
- `buildThreadViewStream`
  - rewrites thread responses into an event stream with reply-first ordering, metadata events, a synthetic range event, ancestor events, then the focal event
- `buildEventsWithMetadataAndRange`
  - appends metadata events and synthetic range events to event collections
- `buildDirectMessageContactsPayload`, `buildDirectMessagesPayload`
  - synthesize compatibility event payloads and include metadata/range events
- `buildSearchFilterlistResponse`, `buildHiddenByContentModerationResponse`
  - synthesize compatibility moderation events that do not have native HTTP equivalents
- `resolveUnifiedSearch`
  - combines search events with profile payload objects shaped as kind-0-like compatibility items

## Explicit duplication between `internal/api` and `internal/api_primal`

The clearest duplicated orchestration today is in HTTP handlers:

- Thread assembly:
  - `internal/api.Handlers.GetThread` now delegates to `query.Service.GetThread`
  - `internal/api_primal.Handlers.GetThreadView` now also delegates to `query.Service.GetThread`
  - duplication is reduced for this slice; remaining differences are transport-level parsing and response shape
- Batch event assembly:
  - `internal/api.Handlers.BatchGetEvents`
  - `internal/api_primal.Handlers.BatchGetEvents`
  - both normalize ids, call `query.Service.GetEvents`, preserve requested order, and compute missing ids
- Single profile assembly:
  - `internal/api.Handlers.GetProfileByPubkey`
  - `internal/api_primal.Handlers.GetProfileByPubkey`
  - both call `query.Service.GetProfile` and map to a thin response struct
- Batch profile/user-info assembly:
  - `internal/api.Handlers.BatchGetProfiles`
  - `internal/api_primal.Handlers.BatchGetUserInfos`
  - both normalize pubkeys, call `query.Service.GetProfiles`, preserve input order, and compute missing pubkeys
- Author event/reply and contact/relay list reads also mirror each other structurally and now call shared `query.Service` methods.

## WebSocket paths that bypass shared service logic

The main bypasses in `internal/api_primal/ws_gateway.go` are:

- `thread_view`
  - now uses `g.query.GetThreadWindow` for lookup assembly; transport-specific stream composition still happens in `buildThreadViewStream`
- metadata enrichment
  - `buildMetadataEvents` independently resolves profile projections to metadata event ids and then fetches raw events; no shared HTTP path performs this as a reusable service step
- event collection shaping
  - `buildEventsWithMetadataAndRange` appends synthetic range events and metadata events in the gateway, not in `query.Service`
- moderation shaping
  - `buildSearchFilterlistResponse` and `buildHiddenByContentModerationResponse` assemble compatibility payloads directly in the gateway
- long-form thread compatibility
  - `resolveLongFormContentThreadView` does not use `query.Service.GetLongFormThreadView`; it fetches the parameterized event, collects base replies, fetches extra a-tag replies via `g.query.GetLongFormThreadATagReplies`, then applies shared `query.WindowDescendingReplies` before WS stream shaping

## What is canonical today

- There is **no single canonical read assembly path across all surfaces** today.
- For migrated HTTP reads, the canonical pattern is handler -> `query.Service` -> transport response shaping.
- For non-migrated HTTP reads, handlers still call store methods directly.
- For WebSocket compatibility, the canonical pattern is gateway -> `query.Service` for shared lookup/assembly primitives -> gateway-owned compatibility stream shaping.
- `query.Service` is best described as the shared read-orchestration layer for migrated slices, not a universal assembler for every response path.

## Observations

- The native HTTP and Primal HTTP layers still duplicate orchestration for event batches, profile lookups, and batch profile assembly.
- The WebSocket gateway is the heaviest orchestration surface. It both reuses and bypasses `query.Service`, depending on the request kind.
- Thread handling is shared for the primary HTTP and WS thread_view slices:
  - HTTP handlers delegate to `query.Service.GetThread`
  - WS `thread_view` delegates to `query.Service.GetThreadWindow`
  - WS still owns compatibility stream shaping (metadata/range events and ordering)

## Shared HTTP transport helper

- Generic HTTP JSON body limiting/decoding logic is now centralized in `internal/transport/httpx/body_limit.go`.
- Both `internal/api/body_limit.go` and `internal/api_primal/body_limit.go` now use the shared helper and only keep transport-surface-specific error envelope mapping.
- Shared helper responsibilities:
  - max-bytes enforcement via `http.MaxBytesReader`
  - JSON decode with optional unknown-field rejection
  - single-object enforcement (reject trailing JSON payloads)
- API/Primal wrappers remain intentionally thin so business/query code is not moved into transport helpers.

## Store to job vocabulary boundary

- Canonical event persistence in `internal/store/events.go` no longer imports derivation-owned job constants.
- Job type vocabulary used for queue publication is now owned by `internal/jobs` (`internal/jobs/types.go`).
- Canonical-event downstream publication now routes through `internal/jobs.QueuePublisher` (via `internal/jobs.CanonicalEventPublisher`) and remains invoked only when a new event row is inserted.
- `internal/derivation` still consumes the same job types, but now aliases `internal/jobs` constants, so dependency direction is `derivation -> jobs` rather than `store -> derivation`.

## Target application-service shape

The `internal/query` package now exposes focused, transport-agnostic interfaces intended to become the common read-orchestration boundary:

- `ThreadService`
  - `GetThread(ctx, ThreadRequest) (ThreadView, error)`
  - `GetThreadWindow(ctx, ThreadWindowRequest) (ThreadView, error)`
- `EventService`
  - `GetEvent(ctx, id)`
  - `GetEvents(ctx, ids)`
  - `GetEventActionCounts(ctx, eventID)`
- `ProfileService`
  - `GetProfile(ctx, pubkey)`
  - `GetProfiles(ctx, pubkeys) (UserInfosResult, error)`

`query.Service` implements these interfaces and, for already-migrated slices, acts as the shared read-orchestration layer while non-migrated handlers continue to call reader methods directly.
