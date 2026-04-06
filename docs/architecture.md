# Architecture

NostrMash separates durable ingest truth from rebuildable read models. Read this page first if you need the system shape, service boundaries, or the reasoning behind the Layer 1 / 2 / 3 model.

## On This Page

- [Purpose](#purpose)
- [Service Boundaries](#service-boundaries)
- [Data Flow](#data-flow)
- [Layer Model](#layer-model)
- [Versioned Derivations and Rebuilds](#versioned-derivations-and-rebuilds)
- [Why Postgres Is Primary](#why-postgres-is-primary)
- [Trust And Ranking Expansion](#trust-and-ranking-expansion)

## Purpose

NostrMash turns relay traffic into a durable, queryable system without collapsing raw ingest and product-facing read models into the same thing. It stores canonical event truth first, then derives higher-level state asynchronously.

That split is the point: raw history must survive schema changes and bad projection ideas; read models must be cheap to rebuild and easy to version.

## Service Boundaries

- `ingestor`: receives relay payloads in `live`, optional `backfill`, or deterministic `replay` mode; validates events; writes canonical data; records invalid payloads; persists relay checkpoints; enqueues derivation jobs.
- `worker`: claims jobs from Postgres and materializes derivations and projections.
- `api`: serves native read endpoints, a focused but substantial `/primal/v1` and `/primal/ws` compatibility surface, and admin inspection/rebuild endpoints.
- `postgres`: primary datastore for canonical storage, checkpoints, queue state, derivation metadata, and projections.

## Data Flow

```text
relays
  |
  v
ingestor
  |
  +--> validation failure --> invalid_events
  |
  +--> canonical write --> events + event_tags + event_relays
                         |
                         +--> jobs
                               |
                               v
                             worker
                               |
                               v
              derivations / projections / rebuild state
                               |
                               v
                               api
```

The canonical path is synchronous and durable. Derived state is asynchronous and may lag behind raw ingest.

## Layer Model

### Layer 1: Canonical Truth

Layer 1 is the durable ingest record:

- `events`: canonical raw event JSON and core fields
- `event_tags`: expanded tag rows
- `event_relays`: relay provenance
- `invalid_events`: quarantined invalid payloads
- `ingest_checkpoints`: per-relay progress for live/backfill

This layer is append-heavy, replayable, and treated as the foundation.

### Layer 2: Derivation State

Layer 2 captures reusable interpreted state derived from raw truth:

- `event_references`, `pubkey_references`
- `replaceable_state`
- `derivation_versions`, `derivation_active_versions`
- `projection_rebuild_runs`

This layer exists to avoid re-parsing the same semantics everywhere and to make rebuild/version control explicit.

### Layer 3: Read Models

Layer 3 is read-optimized projection state consumed by APIs:

- `profiles_latest`
- `author_recent_events`
- `thread_edges`, `unresolved_thread_references`
- `reply_counts`, `reaction_counts`, `repost_counts`
- `reaction_events`, `repost_events`, `deletion_events`
- `contact_lists_latest`, `relay_lists_latest`
- `follower_edges`
- `dm_unread_counts`, `dm_read_cursors`
- `zap_receipts`
- curated parity tables such as `curated_reads_topics`, `curated_featured_authors`, `curated_recommended_reads`, and `curated_creator_paid_tiers`

Layer 3 is disposable in principle. If a projection is wrong or changes shape, rebuild it from lower layers.

## Versioned Derivations and Rebuilds

Every projection is tied to an explicit derivation name and version. The code tracks:

- compiled version: what the binary knows how to produce
- target version: what operators want the system to converge to
- active version: what the live read path currently serves

Full rebuilds promote a derivation's active version after successful completion. The current implementation also supports narrower rebuild scopes for a single event, a pubkey, or a time range.

This is the core contract: derived state can evolve without rewriting raw history.

## Why Postgres Is Primary

Postgres is not just storage here. It is the consistency boundary:

- canonical event write, provenance write, and job enqueue happen together
- checkpoints, queue state, rebuild metadata, and projections stay in one transactional system
- operational complexity stays low for early production usage

That design favors correctness and operability over distributed-system novelty.

## Trust And Ranking Expansion

Trust and ranking are now in scope for NostrMash, but they remain an expansion area rather than a fully implemented repository surface today.

The design direction is:

- Postgres remains the canonical durability and versioning boundary for ingest truth and published derived outputs
- a trust/ranking subsystem may introduce Redis as working state for graph traversal, walk maintenance, and fast score computation
- compatibility translation remains boundary-only in `internal/api_primal`; trust outputs should be published through shared query surfaces rather than embedded into transport-specific logic

A few limits are still explicit in the current code:

- only the `default_v1` relay filter group is implemented
- compatibility support is still partial relative to full product parity and is rolled out in phases
- trust/ranking architecture is now an intended subsystem, but the full scoring pipeline and query surfaces are still being introduced

See [architecture/trust-subsystem.md](architecture/trust-subsystem.md) for the target design.

## Related Docs

- [README.md](../README.md)
- [docs/README.md](README.md)
- [development.md](development.md)
- [operations.md](operations.md)
- [api.md](api.md)
- [architecture/trust-subsystem.md](architecture/trust-subsystem.md)
