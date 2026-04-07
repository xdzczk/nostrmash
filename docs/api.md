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
- author event/reply views
- thread and ancestor/reply views
- relay health summaries
- projected interaction counts
- kind-scoped user event reads such as `bookmarks`, `highlights`, `long-form`, and `zaps`
- `mentions` (p-tag reference events)
- `followers` (derived follower edges from latest kind:3 contact lists)
- trust score reads for a single pubkey or the current top-ranked set

This is the surface to extend when NostrMash gains new first-class read capabilities.

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
- `GET /primal/ws` (WebSocket `REQ`/`CLOSE` compatibility gateway)

How to think about compatibility:

- use it when you need Primal-oriented shapes and request names, not when you are designing new NostrMash-native capabilities
- expect boundary-specific response shaping, especially on WebSocket request kinds
- expect partial parity rather than a frozen one-to-one mirror of every external surface
- treat [primal_compatibility_matrix.md](primal_compatibility_matrix.md) as the current feature inventory
- treat [compatibility_rollout.md](compatibility_rollout.md) as the operational adoption plan

Current compatibility highlights:

- `thread_view` supports opaque cursor pagination (`cursor` input, `next_cursor` output)
- `get_directmsgs` exists on the WebSocket gateway for compatibility only; there is no public native HTTP DM retrieval route
- search behavior is intentionally unified between top-level WS `search` filters and `cache:search`
- compatibility cache groups also cover social graph, moderation, zaps, parameterized replaceables, and curated parity reads
- curated reads/topics/authors and LN lookup use Primal-like kind envelopes
- `creator_paid_tiers` prefers event-native output and falls back to curated normalized output when source events are absent

Use compatibility when preserving an external client contract matters more than exposing the cleanest native shape.

This is still not full product parity. Compatibility logic remains boundary-only and avoids leaking protocol-specific models into core storage and derivation code.

HTTP compatibility contract coverage remains fixture-driven for selected routes:

- `GET /primal/v1/events/{id}`
- `POST /primal/v1/events/batch`
- `GET /primal/v1/profiles/{pubkey}`

Contract fixtures and golden responses live under [`../internal/api_primal/testdata/primal_contracts`](../internal/api_primal/testdata/primal_contracts).
Run with strict comparison by default and update intentionally only with:

```bash
go test ./internal/api_primal -update
```

The phased target and deferred scope are defined in:

- [primal_compatibility_matrix.md](primal_compatibility_matrix.md)
- [compatibility_rollout.md](compatibility_rollout.md)

## Admin API

The admin surface lives under `/admin/v1` and is for operators, not public clients.

It exposes inspection and control for:

- relay state
- relay suggestion state
- job backlog and failures
- invalid events
- rebuild runs
- storage footprint
- runtime/system status
- derivation versions
- trust runs and current trust phase metadata
- published trust score views

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
