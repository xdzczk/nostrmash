# Compatibility and Deprecation Policy

This document defines practical backward-compatibility expectations for NostrMash external behavior surfaces:

- native HTTP API (`/api/v1/...`)
- Primal-compatible HTTP surface (`/primal/v1/...`)
- Primal-compatible WebSocket surface (`/primal/ws`)

It complements [../VERSIONING.md](../VERSIONING.md) and release guidance in [../RELEASE.md](../RELEASE.md).
Operational rollout/triage context lives in [operations.md](operations.md), and surface-level ownership context lives in [architecture/orchestration-surfaces.md](architecture/orchestration-surfaces.md).

## Compatibility Expectations

### Native API

- Within a major version, avoid breaking existing response contracts and route semantics.
- Additive evolution is preferred (new optional fields, new endpoints, broader filters).
- Behavior or shape changes that can break existing clients should be treated as major-version material.

### Primal-Compatible Surfaces

- Compatibility coverage is intentionally phased and may expand incrementally.
- Existing implemented compatibility routes/events should not change in breaking ways without explicit release communication.
- For `/primal/ws`, maintain frame-type and request/response semantics for currently supported flows unless a major-version change is declared.

### Operator-Facing Compatibility

- Changes to env var meaning/defaults, operational endpoint behavior, or admin workflows require explicit release notes and docs updates.

## Deprecation Policy

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

## Contributor Checklist For Compatibility-Sensitive Changes

When touching `/api/v1`, `/primal/v1`, or `/primal/ws` behavior:

1. Document behavior change and compatibility impact in PR/release notes.
2. Update relevant API/compatibility docs in the same PR.
3. Run contract/testing checks from `testing.md` plus targeted compatibility tests.
4. If deprecating behavior, include overlap window and removal target.

## Changes Requiring Staged Rollout Thinking

- response schema changes for high-traffic endpoints
- compatibility WS behavior changes (filtering, framing, limits, timeout semantics)
- changes affecting relay/ingest correctness visibility used by clients
- config default changes that alter externally visible behavior

Use staged rollout and verify client/operator impact before broad promotion.

## Explicitly Limited Guarantees

- NostrMash does not claim full, frozen parity with every external ecosystem surface.
- Compatibility routes may be partial and are expanded iteratively.
- Emergency fixes may bypass long deprecation windows when required for safety/security, with clear release communication.
