# API Surfaces

This page is the high-level API map for NostrMash. Read this before diving into [openapi.yaml](openapi.yaml) if you need to understand which surface to use, what consistency to expect, and how the current compatibility layer is intentionally scoped.

## On This Page

- [Native API](#native-api)
- [Compatibility API](#compatibility-api)
- [Admin API](#admin-api)
- [Consistency Model](#consistency-model)
- [OpenAPI](#openapi)
- [Pagination, Cursors, and Errors](#pagination-cursors-and-errors)

NostrMash currently exposes three API surfaces with different purposes.

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

This is the surface to extend when the system gains new first-class read capabilities.

## Compatibility API

The compatibility surface currently lives under `/primal/v1`.

It now includes a phased subset plus a WebSocket gateway:

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

This is still not full product parity. Compatibility logic remains boundary-only and avoids leaking protocol-specific models into core storage and derivation code.

Compatibility contract coverage remains fixture-driven for HTTP compatibility routes:

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
- job backlog and failures
- invalid events
- rebuild runs
- storage footprint
- runtime/system status
- derivation versions

Admin endpoints require `ADMIN_BEARER_TOKEN`. If the token is unset, the admin surface is unavailable by design.

## Consistency Model

- Raw event reads can be strong. Canonical event payloads and relay provenance come straight from durable storage.
- Projection reads may be eventually consistent. Counts, thread views, profiles, and similar higher-level reads depend on asynchronous worker jobs.

That split is intentional. Clients should treat canonical reads and projection reads differently.

## OpenAPI

Detailed request and response contracts live in [openapi.yaml](openapi.yaml).

This document is the map. The OpenAPI file is the schema.

## Pagination, Cursors, and Errors

Pagination in the current native API is simple and explicit:

- list endpoints use bounded `limit` query parameters
- reply/thread pagination uses opaque cursors
- cursor payloads are encoded ordering state, not stable business identifiers

Practical expectations:

- treat cursors as opaque values
- do not construct them manually
- expect deterministic ordering on paginated event lists

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

## Related Docs

- [../README.md](../README.md)
- [docs/README.md](README.md)
- [architecture.md](architecture.md)
- [development.md](development.md)
- [operations.md](operations.md)
