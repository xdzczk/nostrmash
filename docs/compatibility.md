# Compatibility and Deprecation Policy

Use this page for policy, not feature inventory. It defines how compatibility-sensitive behavior should evolve across native HTTP, Primal-compatible HTTP, and Primal-compatible WebSocket surfaces.

- native HTTP API (`/api/v1/...`)
- Primal-compatible HTTP surface (`/primal/v1/...`)
- Primal-compatible WebSocket surface (`/primal/ws`)

It complements [../VERSIONING.md](../VERSIONING.md) and release guidance in [../RELEASE.md](../RELEASE.md).
Operational rollout/triage context lives in [operations.md](operations.md), and surface-level ownership context lives in [architecture/orchestration-surfaces.md](architecture/orchestration-surfaces.md).

## Which compatibility doc owns what

| Need | Canonical doc |
| --- | --- |
| Policy and deprecation rules | this page |
| What is implemented today | [primal_compatibility_matrix.md](primal_compatibility_matrix.md) |
| How to roll it out safely | [compatibility_rollout.md](compatibility_rollout.md) |
| Route and response contracts | [api.md](api.md) and [openapi.yaml](openapi.yaml) |

## Compatibility expectations

### Native API

- Within a major version, avoid breaking existing response contracts and route semantics.
- Additive evolution is preferred (new optional fields, new endpoints, broader filters).
- Behavior or shape changes that can break existing clients should be treated as major-version material.

### Primal-compatible surfaces

- The currently shipped compatibility surface is the legacy-shaped surface documented in this repository today.
- Future compatibility additions outside that shipped surface may still expand incrementally.
- Existing implemented compatibility routes/events should not change in breaking ways without explicit release communication.
- For `/primal/ws`, maintain frame-type and request/response semantics for currently supported flows unless a major-version change is declared.

### Operator-facing compatibility

- Changes to env var meaning/defaults, operational endpoint behavior, or admin workflows require explicit release notes and docs updates.

## Deprecation policy

When behavior needs to change incompatibly, use a deprecation path unless there is an urgent safety/security reason.

Deprecation process:

1. **Announce** in release notes what is deprecated and why.
2. **Document replacement** path (new endpoint/field/behavior).
3. **Provide overlap window** across at least one minor release when feasible.
4. **Remove** only in a later major release, or earlier only for critical risk mitigation with explicit operator/client warning.

Minimum deprecation communication should include:

- affected endpoint/event/field/config
- earliest removal target release
- migration steps for clients/operators

## Contributor checklist for compatibility-sensitive changes

When touching `/api/v1`, `/primal/v1`, or `/primal/ws` behavior:

1. Document behavior change and compatibility impact in PR/release notes.
2. Update relevant API/compatibility docs in the same PR.
3. Run contract/testing checks from `testing.md` plus targeted compatibility tests.
4. If deprecating behavior, include overlap window and removal target.

## Changes requiring staged rollout thinking

- response schema changes for high-traffic endpoints
- compatibility WS behavior changes (filtering, framing, limits, timeout semantics)
- changes affecting relay/ingest correctness visibility used by clients
- config default changes that alter externally visible behavior

Use staged rollout and verify client/operator impact before broad promotion.

## Explicitly limited guarantees

- NostrMash does not claim full, frozen parity with every external ecosystem surface.
- The compatibility guarantees above apply to the surface implemented in this repository today; future ecosystem-facing additions may still expand iteratively.
- Emergency fixes may bypass long deprecation windows when required for safety/security, with clear release communication.
