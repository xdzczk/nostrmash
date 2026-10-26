# Compatibility Operations

Use this page for operating the compatibility layer as it exists today. For the current surface inventory, use [primal_compatibility_matrix.md](primal_compatibility_matrix.md). This document focuses on how to run and validate the currently supported legacy-shaped compatibility surface in production.

## Current state

- HTTP compatibility routes are implemented and contract-tested.
- `GET /primal/ws` implements the legacy compatibility WebSocket gateway.
- `GET /primal/ws/v1` implements the Primal iOS cache protocol. Disable it with `PRIMAL_WS_V1_ENABLED=false`.
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

## iOS smoke test

1. Set the caching service to `wss://<host>/primal/ws/v1`.
2. Confirm the socket sends `set_primal_protocol` and then receives default settings (`10000103`) and client config (`10000162`).
3. Open a profile. The header needs a kind `0` and a kind `10000105`.
4. Open home. Notes render only when the page's kind `10000113` `elements` lists their ids.
5. Open a thread. The reply page and the root page each end with their own RANGE.
6. Membership, wallet, and push stay quiet: status is the free tier and the other calls return EOSE.

Watch `nostrmash_primal_ws_requests_total{dialect="primal_v1",outcome="error"}` and notices whose text is `unknown api request`. The `funcall` label is the cache name that still needs a handler.

## Source of truth

- use [primal_compatibility_matrix.md](primal_compatibility_matrix.md) for present-tense feature availability
- use [api.md](api.md) and [openapi.yaml](openapi.yaml) for HTTP contract details
- use this document for production operation and validation guidance
