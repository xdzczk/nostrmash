# API surfaces

Use this page to decide which NostrMash surface you should integrate with. It is intentionally the map, not the full contract: start here for surface selection and consistency expectations, then move to [openapi.yaml](openapi.yaml) or the compatibility matrix for exact shapes.

## On this page

- [Surface selection](#surface-selection)
- [Native API](#native-api)
- [Compatibility API](#compatibility-api)
- [Admin API](#admin-api)
- [Consistency model](#consistency-model)
- [OpenAPI](#openapi)
- [Pagination, cursors, and errors](#pagination-cursors-and-errors)

NostrMash currently exposes three API surfaces with different purposes.

## Surface selection

| Use case | Primary surface | Why |
| --- | --- | --- |
| Durable product-facing reads owned by this repo | `/api/v1` | This is the native contract and the preferred home for new first-class capabilities |
| Existing Primal-oriented clients | `/primal/v1` and `/primal/ws` | Compatibility behavior stays at the boundary without reshaping core storage and derivation layers |
| Operator inspection and control | `/admin/v1` | Admin routes expose system state, operational controls, and runtime visibility |

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"-apple-system, BlinkMacSystemFont, Segoe UI, sans-serif","primaryColor":"#eff6ff","primaryTextColor":"#0f172a","primaryBorderColor":"#93c5fd","lineColor":"#2563eb","secondaryColor":"#ecfeff","secondaryTextColor":"#0f172a","secondaryBorderColor":"#67e8f9","tertiaryColor":"#f0fdf4","tertiaryTextColor":"#0f172a","tertiaryBorderColor":"#86efac","clusterBkg":"#ffffff","clusterBorder":"#dbe7f5","mainBkg":"#ffffff","edgeLabelBackground":"#ffffff"},"flowchart":{"curve":"basis","nodeSpacing":30,"rankSpacing":44,"htmlLabels":false}}}%%
flowchart LR
    ReaderNeed[Reader need] --> SurfaceChoice{Choose the surface}
    SurfaceChoice -->|"Native product reads"| NativeAPI[/api/v1]
    SurfaceChoice -->|"Primal compatibility"| CompatAPI[/primal/v1 or /primal/ws]
    SurfaceChoice -->|"Operational control"| AdminAPI[/admin/v1]

    classDef support fill:#f8fafc,stroke:#cbd5e1,color:#0f172a;
    classDef decision fill:#eff6ff,stroke:#93c5fd,color:#0f172a;
    classDef api fill:#ecfeff,stroke:#67e8f9,color:#0f172a;
    classDef compat fill:#f5f3ff,stroke:#c4b5fd,color:#0f172a;
    classDef trust fill:#f0fdf4,stroke:#86efac,color:#0f172a;

    class ReaderNeed support;
    class SurfaceChoice decision;
    class NativeAPI api;
    class CompatAPI compat;
    class AdminAPI trust;
```

### Example: choosing a surface

Use this quick rule of thumb:

1. If you are building a NostrMash-native client, start with `/api/v1`.
2. If you are replacing or mirroring a Primal-oriented integration, start with `/primal/v1` or `/primal/ws`.
3. If you need system inspection, rebuild control, or operator visibility, use `/admin/v1`.
4. If you are still unsure, choose the surface whose response model you want to preserve rather than the one that merely looks familiar.

## Native API

The native API lives under `/api/v1` and is the primary contract for this repository.

It serves:

- canonical raw event reads
- relay provenance
- projected profile reads
- batch event/profile lookups
- author event/reply views
- thread and ancestor/reply views
- relay health summaries
- event provenance reads such as `seen-on`
- projected interaction counts
- kind-scoped user event reads such as `bookmarks`, `highlights`, `long-form`, and `zaps`
- `mentions` (p-tag reference events)
- `followers` (derived follower edges from latest kind:3 contact lists)
- mute-list reads (`mute-list` for who a user mutes, `muted-by` for the inverse)
- author activity reads (`zaps` and `reactions` sent by an author)
- author analytics rollups (summary, topics, grouped notes, media mix, activity windows, posting patterns, top notes, performance summary, recycle candidates)
- note-level summaries and related-note discovery
- public profile summaries
- trust score reads for a single pubkey or the current top-ranked set
- account status and hydration triggers (`/api/v1/accounts/{pubkey}/...`)

Discovery-oriented native reads have an explicit namespace under `/api/v1/discovery/...`.

- keep discovery surfaces grouped under `/api/v1/discovery/...` (for example notes/profiles/hashtags trending and network/content stats)
- keep discovery ranking endpoints projection-backed (for example note and hashtag trending windows) instead of on-demand joins over raw event graphs
- keep search under `/api/v1/search` so full-text querying remains a dedicated surface
- add new discovery routes through the central declared route list and OpenAPI contract so drift checks protect them

Representative implemented native routes include:

- `GET /api/v1/events/{id}`
- `POST /api/v1/events/batch`
- `GET /api/v1/events/{id}/seen-on`
- `GET /api/v1/events/{id}/counts`
- `GET /api/v1/events/{id}/replies`
- `GET /api/v1/profiles/{pubkey}`
- `GET /api/v1/profiles/{pubkey}/topics`
- `POST /api/v1/profiles/batch`
- `GET /api/v1/authors/{pubkey}/events`
- `GET /api/v1/authors/{pubkey}/replies`
- `GET /api/v1/authors/{pubkey}/zaps`
- `GET /api/v1/authors/{pubkey}/reactions`
- `GET /api/v1/authors/{pubkey}/analytics/summary` (plus `topics`, `grouped-notes`, `media-mix`, `activity-windows`, `posting-patterns`, `top-notes`, `performance-summary`, `recycle-candidates`)
- `GET /api/v1/threads/{eventId}`
- `GET /api/v1/threads/{root_event_id}/summary`
- `GET /api/v1/threads/{root_event_id}/activity`
- `GET /api/v1/notes/{event_id}/summary`
- `GET /api/v1/notes/{event_id}/related`
- `GET /api/v1/users/{pubkey}/summary`
- `GET /api/v1/users/{pubkey}/mute-list`
- `GET /api/v1/users/{pubkey}/muted-by`
- `GET /api/v1/search`
- `GET /api/v1/search/notes`
- `GET /api/v1/search/profiles`
- `GET /api/v1/search/suggest`
- `GET /api/v1/discovery/home`
- `GET /api/v1/discovery/notes/trending`
- `GET /api/v1/discovery/long-form/trending`
- `GET /api/v1/discovery/conversations/hot`
- `GET /api/v1/discovery/profiles/trending` (plus `rising` and `{pubkey}/related`)
- `GET /api/v1/discovery/hashtags/trending` (plus `{hashtag}`, `{hashtag}/notes`, `{hashtag}/related`)
- `GET /api/v1/discovery/stats/network`
- `GET /api/v1/discovery/stats/content`
- `GET /api/v1/discovery/stats/relays`
- `GET /api/v1/discovery/domains/trending`
- `GET /api/v1/discovery/domains/{domain}`
- `GET /api/v1/discovery/domains/{domain}/notes`
- `GET /api/v1/trust/scores` and `GET /api/v1/trust/scores/{pubkey}`
- `GET /api/v1/accounts/{pubkey}/status` and `POST /api/v1/accounts/{pubkey}/hydrate`

Compatibility aliases also exist for `GET /api/v1/discovery/network/stats` and `GET /api/v1/discovery/content/stats`.

The authoritative route inventory is the declared route list in `internal/app/api/routes.go`; contract-owned routes are additionally covered by [openapi.yaml](openapi.yaml) and guarded by the one-way drift test (`make contract-drift`).

This is the surface to extend when NostrMash gains new first-class read capabilities.

### Profile explorer summary

`GET /api/v1/users/{pubkey}/summary` is now shaped for a public profile explorer flow rather than dashboard-style sections.

Response organization (top-to-bottom page composition) is:

- `hero` (single profile identity surface: avatar, display identity, compact counters, metadata strip, action links)
- `recent_notes` (authored notes list)
- `related_discovery` (`related_profiles` plus `rising_profiles`)
- `identity_details` (lower-level metadata, truncation- and copy-friendly)

Legacy compatibility fields remain available (`pubkey`, `metadata_event_id`, `metadata_created_at`, `profile`, `stats`) so existing consumers can migrate incrementally.

### Topic affinity primitives

Topic affinity is exposed as deterministic rollups over explicit content signals (primarily hashtags) and intentionally avoids opaque semantic labeling.

- `GET /api/v1/authors/{pubkey}/analytics/topics`
- `GET /api/v1/profiles/{pubkey}/topics`

Both endpoints support:

- `window`: `7d`, `30d`, or `90d` (default `30d`)
- `limit`: bounded item count (default `20`, max `100`)

Example response shape:

```json
{
  "pubkey": "npub1...",
  "window": "30d",
  "items": [
    { "hashtag": "nostr", "usage_count": 12, "active_days": 8 },
    { "hashtag": "bitcoin", "usage_count": 4, "active_days": 3 }
  ]
}
```

## Compatibility API

The compatibility surface currently lives under `/primal/v1` and `/primal/ws`.

Today it includes a substantial HTTP subset plus a WebSocket gateway:

- `GET /primal/v1/events/{id}`
- `POST /primal/v1/events/batch`
- `GET /primal/v1/profiles/{pubkey}`
- `POST /primal/v1/user_infos`
- `GET /primal/v1/threads/{eventId}`
- `GET /primal/v1/authors/{pubkey}/events`
- `GET /primal/v1/authors/{pubkey}/replies`
- `GET /primal/v1/events/{id}/actions`
- `GET /primal/v1/contact-lists/{pubkey}`
- `GET /primal/v1/relay-lists/{pubkey}`
- `POST /primal/v1/dms/messages`
- `POST /primal/v1/dms/contacts`
- `POST /primal/v1/dms/count`
- `POST /primal/v1/dms/count2`
- `POST /primal/v1/dms/reset-count`
- `POST /primal/v1/dms/reset-counts`
- `GET /primal/ws` (WebSocket `REQ`/`CLOSE` compatibility gateway)

How to think about compatibility:

- use it when you need Primal-oriented shapes and request names, not when you are designing new NostrMash-native capabilities
- expect boundary-specific response shaping, especially on WebSocket request kinds
- expect the supported surface to preserve the legacy-shaped compatibility behavior documented in this repository
- treat [primal_compatibility_matrix.md](primal_compatibility_matrix.md) as the current feature inventory
- treat [compatibility_rollout.md](compatibility_rollout.md) as the operational guide for running the already-supported surface

Current compatibility highlights:

- `thread_view` supports opaque cursor pagination (`cursor` input, `next_cursor` output)
- DM compatibility now exists on both transports: WebSocket cache calls plus strict-parity HTTP wrappers under `/primal/v1/dms/*`
- search behavior is intentionally unified between top-level WS `search` filters and `cache:search`
- compatibility cache groups also cover social graph, moderation, zaps, parameterized replaceables, and curated parity reads
- curated reads/topics/authors and LN lookup use Primal-like kind envelopes
- `creator_paid_tiers` prefers event-native output and falls back to curated normalized output when source events are absent

Use compatibility when preserving an external client contract matters more than exposing the cleanest native shape.

This implements the currently supported legacy-shaped compatibility surface documented in this repository. Compatibility logic remains boundary-only so protocol-specific models do not leak into core storage and derivation code.

HTTP compatibility contract coverage remains fixture-driven for compatibility routes including:

- `GET /primal/v1/events/{id}`
- `POST /primal/v1/events/batch`
- `GET /primal/v1/profiles/{pubkey}`
- `POST /primal/v1/dms/messages`
- `POST /primal/v1/dms/contacts`
- `POST /primal/v1/dms/count`
- `POST /primal/v1/dms/count2`

Contract fixtures and golden responses live under [`../internal/api_primal/testdata/primal_contracts`](../internal/api_primal/testdata/primal_contracts).
Run with strict comparison by default and update intentionally only with:

```bash
go test ./internal/api_primal -update
```

Current inventory and operational notes live in:

- [primal_compatibility_matrix.md](primal_compatibility_matrix.md)
- [compatibility_rollout.md](compatibility_rollout.md)

## Admin API

The admin surface lives under `/admin/v1` and is for operators, not public clients.

It exposes inspection and control for:

| Area | Routes |
| --- | --- |
| Relay state | `GET /admin/v1/relays`, `GET /admin/v1/relays/suggestions` |
| Relay registry | `GET /admin/v1/relay-registry`, `GET /admin/v1/relay-registry/desired`, `POST /admin/v1/relay-registry/policy`, `GET /admin/v1/relay-registry/diagnostics`, `GET /admin/v1/relay-registry/admission-dry-run` |
| Job backlog and failures | `GET /admin/v1/jobs` |
| Invalid events | `GET /admin/v1/invalid-events` |
| Projection/discovery/search status | `GET /admin/v1/status/projections`, `GET /admin/v1/status/discovery`, `GET /admin/v1/status/search` |
| Search sync | `POST /admin/v1/search/meilisearch/sync` |
| Rebuilds | `GET /admin/v1/rebuilds`, `POST /admin/v1/rebuilds` |
| Storage footprint | `GET /admin/v1/storage`, `GET /admin/v1/storage/indexes` |
| Account control | `POST /admin/v1/accounts/{pubkey}/state`, `POST /admin/v1/accounts/{pubkey}/hydrate` |
| Runtime/system status | `GET /admin/v1/system` |
| Derivation versions | `GET /admin/v1/derivation-versions` |
| Trust runs and scores | `GET /admin/v1/trust/runs`, `GET /admin/v1/trust/runs/{runID}`, `POST /admin/v1/trust/runs`, `GET /admin/v1/trust/scores` |

Admin endpoints require `ADMIN_BEARER_TOKEN`. If the token is unset, the admin surface is unavailable by design.

## Consistency model

- Raw event reads can be strong. Canonical event payloads and relay provenance come straight from durable storage.
- Projection reads may be eventually consistent. Counts, thread views, profiles, and similar higher-level reads depend on asynchronous worker jobs.

That split is intentional. Clients should treat canonical reads and projection reads differently.

## OpenAPI

Detailed request and response contracts live in [openapi.yaml](openapi.yaml).

This document is the map. The OpenAPI file is the schema.

## Pagination, cursors, and errors

Pagination in the current native API is simple and explicit:

- list endpoints use bounded `limit` query parameters
- reply/thread pagination uses opaque cursors
- cursor payloads are encoded ordering state, not stable business identifiers

Practical expectations:

- treat cursors as opaque values
- do not construct them manually
- expect deterministic ordering on paginated event lists
- expect `413` for oversized JSON POST bodies on batch/admin endpoints
- expect `429` when HTTP per-client rate limits are exceeded

Error responses use a consistent envelope:

```json
{
  "error": {
    "code": "not_found",
    "message": "event not found",
    "request_id": "..."
  }
}
```

Use `request_id` for tracing across logs. Use `code` for program logic. Use `message` for operator-facing context.

For deeper detail, use [openapi.yaml](openapi.yaml) for the native contract, [primal_compatibility_matrix.md](primal_compatibility_matrix.md) for the compatibility inventory, and [operations.md](operations.md) for operator-facing API/runtime expectations.
