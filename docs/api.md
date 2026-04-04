# API Surfaces

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

It is intentionally small:

- `GET /primal/v1/events/{id}`
- `POST /primal/v1/events/batch`
- `GET /primal/v1/profiles/{pubkey}`

This is not a general compatibility layer yet. The current implementation is a narrow boundary adapter and keeps compatibility logic out of core storage and derivation code.

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

Detailed request and response contracts live in `docs/openapi.yaml`.

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
