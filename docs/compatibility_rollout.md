# Compatibility Operations

Use this page for operating the compatibility layer as it exists today. For the current surface inventory, use [primal_compatibility_matrix.md](primal_compatibility_matrix.md). The repository now implements parity with the legacy `primal-caching-service-main` cache API; this document focuses on how to run and validate that supported surface in production.

## Current state

- HTTP compatibility routes are implemented and contract-tested.
- `GET /primal/ws` implements the compatibility WebSocket gateway.
- DM compatibility is available on both transports.
- Compatibility behavior is expected to match the legacy cache surface in payload shape, ordering, and error handling.

## Operational priorities

- keep fixture/golden contract tests green for contract-owned HTTP routes
- keep WebSocket lifecycle behavior stable under load, including subscription limits and request throttling
- monitor compatibility latency, error rates, and unsupported-request notices
- compare representative production traffic against legacy behavior when changing compatibility handlers
- preserve native API behavior while evolving compatibility-only boundaries

## Validation checklist

- HTTP contract tests pass for all contract-owned compatibility routes
- WebSocket compatibility tests pass for request/response lifecycle and cache dispatch behavior
- OpenAPI stays aligned with contract-owned HTTP routes
- production observability covers request volume, latency, failures, and unsupported calls
- on-call documentation reflects the currently supported compatibility surface

## Source of truth

- use [primal_compatibility_matrix.md](primal_compatibility_matrix.md) for present-tense feature availability
- use [api.md](api.md) and [openapi.yaml](openapi.yaml) for HTTP contract details
- use this document for production operation and validation guidance
