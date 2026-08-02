# Trust-Aware Policy Boundaries

This document is the contributor guardrail for trust-aware policy usage. Use it during design and code review when changing trust logic, search/discovery behavior, fallback, ingest scheduling, or retention.

For trust subsystem architecture and run phases, see [trust-subsystem.md](trust-subsystem.md).

## Why this exists

Trust policy is a shaping tool, not a cross-cutting rule engine. Without explicit boundaries, trust checks spread into handlers, store methods, and ad hoc SQL filters until behavior becomes inconsistent and hard to reason about.

## Allowed impact surface

Trust policy is allowed to affect:

- discovery candidate shaping and ranking
- search ranking and scope shaping
- relay fallback behavior (bounded, policy-aware fanout)
- ingest prioritization/scheduling (ordering, frontier focus), while preserving canonical durability
- derived/transient retention hooks where policy influences what is kept hot vs. purged

Trust policy is **not** allowed to affect:

- canonical event acceptance/writes as the primary durability path
- transport-specific handler branching in `internal/api` or `internal/api_primal`
- direct `internal/store` query semantics as a hidden policy layer
- broad unbounded relay-backed "search the network" behavior

## Architectural placement

Keep trust policy in one place per layer:

- `internal/config`: policy knobs, defaults, and validation
- `internal/query`: trust-aware orchestration and policy application
- `internal/store`: data access only (no hidden trust policy decisions)
- `internal/api` and `internal/api_primal`: transport adaptation only

If a PR adds trust conditionals in handlers or store methods, it is likely crossing boundaries and should be refactored toward query-layer policy.

## Canonical ingest boundary

Canonical ingest has two distinct trust surfaces. Do not conflate them.

### Read-side trust policy (query layer)

Discovery, search, fallback, and retention hooks use `TRUST_*_MODE` env vars (`TRUST_DISCOVERY_CANDIDATE_MODE`, `TRUST_SEARCH_RANKING_MODE`, etc.). These shape read paths and derived-state retention; they do **not** drop events at the ingest hot path.

### Trust-bounded ingest gate (ingestor)

Live canonical write enforcement is owned by the ingestor via `INGESTOR_TRUST_GATE_*`:

| Env | Default | Role |
| --- | --- | --- |
| `INGESTOR_TRUST_GATE_MODE` | `open` | `open` = shadow (metrics only); `trusted_only` = enforce author trust for kinds 1/4/5/9802/10000/10003/30023 and 6/7/9735 target-exists |
| `INGESTOR_TRUST_GATE_MAX_HOPS` | `2` | Authors within this hop distance of a seed are trusted for kind `1` |
| `INGESTOR_TRUST_GATE_REFRESH_INTERVAL` | `2m` | Reload in-memory trusted set from `trust_graph_snapshot` |

Prerequisites live in `trust_worker`: `TRUST_SEED_PUBKEYS`, `TRUST_GRAPH_SNAPSHOT_REFRESH_INTERVAL`, `TRUST_RUN_INTERVAL`.

Rules:

- Deploy with `INGESTOR_TRUST_GATE_MODE=open` first; confirm trusted-set metrics before flipping to `trusted_only`.
- Open kinds (`0`, `3`, `10002`) always pass the gate so the graph can bootstrap. Kind `5` deletions require a trusted author or a locally-stored `e`-tag target.
- Rejected gate events are counted in metrics only — not written to `invalid_events`.
- `TRUST_CANONICAL_INGEST_MODE` is **deprecated** (config placeholder, never wired). Use `INGESTOR_TRUST_GATE_MODE`.

Full design: [trust-bounded-ingest.md](trust-bounded-ingest.md). Operator setup: [../operations.md#trust-bounded-ingest-rollout](../operations.md#trust-bounded-ingest-rollout).

## Recommended public-instance modes

For public discovery/search/fallback behavior, use this baseline unless you have a specific abuse posture that requires stricter behavior:

| Surface | Recommended mode | Rationale |
| --- | --- | --- |
| `TRUST_DISCOVERY_CANDIDATE_MODE` | `open` | Preserve broad discovery coverage; avoid over-pruning early. |
| `TRUST_SEARCH_RANKING_MODE` | `prefer_trusted` | Improve relevance quality while still allowing broader recall. |
| `TRUST_FALLBACK_FETCH_MODE` | `open` | Keep direct miss recovery resilient for sparse local state. |

Additional defaults worth keeping:

- `TRUST_FALLBACK_FETCH_ALLOW_DIRECT_LOOKUP=true` to preserve explicit direct lookup recovery.
- bounded fallback knobs (`TRUST_FALLBACK_FETCH_MAX_ATTEMPTS`, `TRUST_FALLBACK_FETCH_MAX_RELAYS_PER_ATTEMPT`, `TRUST_FALLBACK_FETCH_MAX_TIME_BUDGET`) to prevent network fanout explosions.

## Retention and derived state

Derived/transient retention can be trust-shaped (`TRUST_RETENTION_POLICY_MODE`) when operators intentionally trade storage for trust-focused hot data.

Guardrails:

- apply policy to retention outputs, not canonical raw event truth
- keep retention behavior explicit and observable
- avoid implicit trust-based deletion behavior buried in unrelated jobs
- keep retention ownership explicit per transient/rebuildable class (cache, discovery candidates, enrichment state, fallback metadata)
- reject canonical durable event scopes from trust-aware retention selectors by design

## Operator tradeoffs

Mode tradeoffs by surface:

- `open`: best recall and compatibility, weakest trust filtering
- `prefer_trusted`: balanced quality/recall, good default for ranking surfaces
- `trusted_only`: strongest filtering, highest risk of false negatives and missing long-tail content

Operational implications of stricter modes:

- requires high-quality `TRUST_SEED_PUBKEYS` hygiene
- increases risk of cold-start blind spots
- can suppress legitimate but weakly connected content
- should be rolled out gradually and measured with discovery/search outcome metrics

## Explicit anti-patterns

Avoid these in reviews:

- injecting trust checks directly in HTTP/WS handlers
- adding trust-specific `WHERE` clauses in store methods as one-off fixes
- duplicating trust gating logic separately across discovery, search, and fallback codepaths
- widening fallback into broad relay-backed global search
- coupling canonical ingest durability to trust score thresholds on **open bootstrap kinds** (`0`, `3`, `10002`) — these must stay open while the graph warms up
- enabling `INGESTOR_TRUST_GATE_MODE=trusted_only` before `trust_graph_snapshot` has loaded (kind `1` fail-closes until first successful trusted-set refresh)

## Review checklist

Before approving trust-related changes:

- Does this keep canonical ingest broad and durable?
- Is policy applied in query orchestration instead of handlers/store?
- Are discovery/search/fallback mode changes intentional and documented?
- Are fallback limits still bounded and operator-safe?
- Does this avoid introducing broad relay-backed search behavior?

If the answer to any item is no, treat it as a boundary regression and request refactoring.
