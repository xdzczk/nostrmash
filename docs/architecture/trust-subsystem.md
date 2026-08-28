# Trust Subsystem

Use this page when you are working on trust, ranking, relay suggestions, or trust-driven ingest behavior. It describes the deeper trust architecture: optional Redis-backed working state, staged score computation, and published outputs that still remain rebuildable from Postgres-backed inputs.

## Goals

- Add trust-aware ranking and filtering without creating a second canonical event pipeline
- Support graph-derived scores such as global rank now, with seeded/personalized trust deferred until a concrete product need exists
- Keep fast graph computation and walk maintenance off the critical canonical ingest path
- Publish trust outputs back into the same durable derivation and query model used elsewhere in NostrMash

## Non-Goals

- Replacing Postgres as the primary source of truth
- Running a second independent crawler that competes with `cmd/ingestor`
- Making Redis the only home of trust outputs that APIs depend on

## Design summary

- Postgres remains the system of record for canonical events, replaceable latest state, checkpoints, jobs, derivation versions, and published trust outputs.
- Redis is allowed as working state for graph adjacency, random walks, score computation, and cache-friendly trust neighborhoods.
- Trust computation should be driven from NostrMash-derived graph inputs such as `contact_lists_latest`, `follower_edges`, and `relay_lists_latest`, not from a parallel ingest path.
- Published trust/ranking results should flow back into Postgres through explicit derivations so they are rebuildable, inspectable, and versioned.

Current runtime note:

- The checked-in local Compose stack starts `redis`, but `trust_worker` defaults to `TRUST_ENABLE_REDIS_SYNC=false`, so trust sync/compute can run in a Postgres-only graph mode until Redis-backed sync is explicitly enabled.

## Existing inputs in NostrMash

NostrMash already derives graph-relevant inputs from canonical events:

- `contact_lists_latest`: latest kind `3` replaceable state per pubkey
- `follower_edges`: directed follow edges derived from latest contact lists
- `relay_lists_latest`: latest kind `10002` relay-list metadata per pubkey
- optional future enrichment from replies, reposts, zaps, mentions, and moderation state

These inputs are sufficient to build an initial WoT graph without importing an external SQLite/Redis ingest layer.

## Target data flow

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"-apple-system, BlinkMacSystemFont, Segoe UI, sans-serif","primaryColor":"#eff6ff","primaryTextColor":"#0f172a","primaryBorderColor":"#93c5fd","lineColor":"#2563eb","secondaryColor":"#ecfeff","secondaryTextColor":"#0f172a","secondaryBorderColor":"#67e8f9","tertiaryColor":"#f0fdf4","tertiaryTextColor":"#0f172a","tertiaryBorderColor":"#86efac","clusterBkg":"#ffffff","clusterBorder":"#dbe7f5","mainBkg":"#ffffff","edgeLabelBackground":"#ffffff"},"flowchart":{"curve":"basis","nodeSpacing":28,"rankSpacing":40,"htmlLabels":false}}}%%
flowchart LR
    Relays[Relays]
    Ingestor[Ingestor]
    Canonical[Canonical Postgres]
    Worker[Worker]
    GraphDerivations[Graph derivations]
    TrustPublisher[Trust publisher]
    RedisGraph[Redis graph state]
    TrustRunner[Trust runner]
    TrustOutputs[Published trust outputs]
    API[Native and Primal APIs]

    Relays --> Ingestor
    Ingestor --> Canonical
    Canonical --> Worker
    Worker --> GraphDerivations
    GraphDerivations --> TrustPublisher
    TrustPublisher --> RedisGraph
    RedisGraph --> TrustRunner
    TrustRunner --> TrustOutputs
    TrustOutputs --> API

    classDef support fill:#f8fafc,stroke:#cbd5e1,color:#0f172a;
    classDef core fill:#eff6ff,stroke:#93c5fd,color:#0f172a;
    classDef trust fill:#f0fdf4,stroke:#86efac,color:#0f172a;
    classDef api fill:#ecfeff,stroke:#67e8f9,color:#0f172a;

    class Relays support;
    class Ingestor,Canonical,Worker,GraphDerivations core;
    class TrustPublisher,RedisGraph,TrustRunner,TrustOutputs trust;
    class API api;
```

Read this diagram left to right: canonical relay data still enters through the normal ingest path, graph-oriented trust work can happen off to the side in Redis-backed working state when enabled, and only the published outputs flow back into the shared API/query world. In the default local deployment, the same phases still run but adjacency is loaded directly from Postgres instead of Redis.

## Subsystem components

### 1. Graph inputs in Postgres

These remain derived and durable:

- `contact_lists_latest`
- `follower_edges`
- `relay_lists_latest`

Optional later inputs:

- interaction edges from replies, reposts, mentions, or zaps
- moderation allow/mute lists
- relay trust or relay-quality signals

### 2. Trust Publisher (Implemented in `trust_worker`)

A dedicated trust worker can publish graph changes from Postgres into Redis. Rehydration from Postgres is first-class and expected when Redis is cold, and the worker can also skip Redis publication entirely when Redis sync is disabled.

Responsibilities:

- read current graph derivations from Postgres
- optionally project adjacency lists and reverse adjacency into Redis
- support scoped rebuilds and full rebuilds
- stamp version/run metadata so Redis state can be correlated with Postgres derivation state

Current deployment uses a dedicated `trust_worker` binary and `jobs.worker_pool='trust'` routing so trust phases are isolated from default projection workers.

### 3. Redis graph state

Redis is optional working state, not canonical truth.

Suggested contents:

- adjacency and reverse adjacency sets
- precomputed global scores
- optional personalized or neighborhood score materialization
- walk state if Monte Carlo / random-walk methods are adopted
- version and run keys tying Redis state to a trust derivation run

Redis keys should be treated as disposable and fully rebuildable from Postgres.

### 4. Trust Runner

The trust runner owns graph algorithms and score production.

Expected responsibilities:

- compute global trust/rank scores
- keep seeded trust logic deferred until explicitly required
- optionally maintain incremental walk-based state
- optionally compute per-request or cached personalized scores
- publish finalized scores back into Postgres

When Redis sync is disabled, the current implementation loads adjacency directly from Postgres and records the run snapshot as `postgres-only`.

Algorithm progression should be staged:

1. global iterative rank
2. optional seed-anchored teleport for the global rank (`TRUST_ENABLE_SEED_TELEPORT`, TrustRank-style: teleport mass lands on active `trust_seeds` instead of uniformly, so scores measure trust flowing from the seed set rather than global popularity; falls back to uniform teleport when no active seed is in the graph)
3. trust-driven operational prioritization
4. optional seeded trust neighborhoods (`TRUST_ENABLE_NEIGHBORHOODS`)
5. optional random-walk or Monte Carlo personalized rank (deferred)

### 5. Published trust outputs in Postgres

Trust outputs should be stored as Layer 3 read models or Layer 2/3 hybrid derivations, not left Redis-only.

Candidate tables:

- `trust_runs`
- `trust_scores_global`
- `ingest_pubkey_frontier` (implemented, bounded trust-targeted fetch frontier)
- `trust_relay_suggestions` (implemented, operator-facing recommendation state)
- `trust_neighborhood_members` (implemented; opt-in via `TRUST_ENABLE_NEIGHBORHOODS`)
- `trust_pubkeys_latest` (implemented)
- `relay_trust_scores` (deferred materialization; current implementation uses on-demand aggregation + suggestion state)

Candidate properties:

- tied to derivation names and versions
- rebuildable from lower layers
- optionally promoteable through active-version switching like other projections
- queryable from `internal/store` and surfaced via `internal/query`

## Query and API integration

Trust should be exposed through shared application/query surfaces, not embedded directly into `internal/api_primal`.

Recommended layering:

- `internal/store`: read/write trust tables and rebuild metadata
- `internal/query`: expose trust-aware orchestration such as:
  - `GetTrustScore(pubkey)`
  - trust-aware variants of feed/search/profile assembly when needed
- `internal/api` and `internal/api_primal`: remain transport adapters over shared trust-aware query paths

## Relay discovery and crawl prioritization

Vertex-style ideas are still useful, but should attach to NostrMash's pipeline:

- use `relay_lists_latest` + `trust_scores_global` to produce operator-visible relay suggestions
- use trust scores to prioritize bounded, author-scoped targeted fetch work
- apply smoothing/hysteresis to avoid high-churn operational reordering

Important constraint:

- trust may influence ingest prioritization and, when explicitly enabled, the trust-bounded ingest gate (`INGESTOR_TRUST_GATE_*`). The gate is a separate, opt-in enforcement surface from read-side trust policy (`TRUST_DISCOVERY_*`, `TRUST_SEARCH_*`, etc.).
- canonical ingest durability for **open kinds** (`0`, `3`, `10002`) and bootstrap behavior during gate warmup should remain broad even when the gate is in shadow mode.

See [trust-bounded-ingest.md](trust-bounded-ingest.md) for gate semantics and rollout.

### Trust graph snapshot (feeds the ingest gate)

`trust_worker` now maintains `trust_graph_snapshot` on a schedule:

- **Seed reconcile** on startup: `TRUST_SEED_PUBKEYS` → `trust_seeds` (authoritative when configured).
- **Snapshot refresh**: `TRUST_GRAPH_SNAPSHOT_REFRESH_INTERVAL` (default `10m`) rebuilds the BFS reachable set from seeds + `follower_edges`.
- **Global trust runs**: `TRUST_RUN_INTERVAL` (default `1h`) when score compute is enabled.

The ingestor loads this snapshot into an in-memory `TrustedAuthorSet` for gate decisions without per-event DB lookups.

## Versioning and rebuild model

Trust/ranking should follow the same derivation discipline as the rest of NostrMash:

- explicit derivation names
- compiled, target, and active versions
- scoped and full rebuild support
- durable run metadata

Redis rebuilds should be associated with a trust run identifier so operators can reason about:

- which graph snapshot a score set came from
- whether Redis and published Postgres outputs are in sync
- whether a failed trust run should be retried or rolled back

## Deployment shape

Two viable deployment shapes:

### Option A: Trust jobs in existing worker

Use the current worker and job queue.

Pros:

- simpler deployment
- fewer binaries and fewer queues
- easiest first implementation

Cons:

- heavy trust workloads may compete with projection jobs
- harder to isolate Redis/ranking failures from normal projection work

### Option B: Dedicated trust worker (Current)

Add a dedicated trust service consuming trust-specific jobs.

Pros:

- better isolation for heavy graph work
- easier tuning and rollout
- clearer ownership of Redis-related failures and rebuilds

Cons:

- one more service to operate

Current path:

- dedicated `trust_worker` runs `trust_sync_graph_redis -> trust_compute_global_scores -> [trust_compute_neighborhoods] -> trust_promote_run` (neighborhoods opt-in via `TRUST_ENABLE_NEIGHBORHOODS`)
- each run is correlated with `trust_runs.redis_snapshot_ref`, which can be `postgres-only` when Redis sync is disabled
- ingest/backfill relay ordering can be biased from `trust_scores_global` + `relay_lists_latest` while remaining bounded by configured allowlists
- ingestor can maintain `ingest_pubkey_frontier` and perform bounded trust-targeted author fetches from configured relays
- operator-facing relay recommendations are exposed via persisted `trust_relay_suggestions`
- `trust_scores_global` should stay seed-free and semantically stable across operator seed changes

## Suggested rollout phases

### Phase 1: Postgres-Native Trust Foundation

- add trust seed configuration
- derive simple trusted sets and follower-based qualification from `follower_edges`
- publish trust outputs in Postgres only

### Phase 2: Redis Working Graph

Mostly implemented:

- publish graph inputs into Redis (`TRUST_ENABLE_REDIS_SYNC`)
- compute global scores and optional seeded neighborhoods (`TRUST_ENABLE_NEIGHBORHOODS`)
- write results back into Postgres (`trust_scores_global`, `trust_pubkeys_latest`, `trust_neighborhood_members`)

`trust_neighborhood_members` is deliberately dormant on the product side: it is
computed and published when the flag is on, but no query capability or API
surface reads it yet. The intended first consumer is seed-scoped discovery
filtering or moderation tooling; wiring a consumer is a product decision, not
an infrastructure gap. Until then the table stays rebuildable data with zero
read dependencies, so it can be dropped or reshaped without a product
migration.

Still deferred: personalized/walk working state (Phase 4).

### Phase 3: Trust-Aware Product Behavior

Mostly implemented:

- trust-aware discovery/search/fallback modes (`open` / `prefer_trusted` / `trusted_only`)
- profile `trust_summary` (tier / hop / percentile; no raw score)
- optional discovery soft boost via `TRUST_DISCOVERY_SCORE_BOOST_WEIGHT` (default `0`)
- trust-aware crawl prioritization and relay recommendation/discovery support

Still deferred from the original Phase 3 wording: viewer-personalized feed ordering (see Phase 4).

### Phase 4: Advanced Vertex-Style Ranking

Implemented (default-inert):

- personalized PageRank core (`ComputePersonalizedRank`) and opt-in query capability `GetPersonalizedTrustRanking` (no default product surface); the API attaches a Redis result cache when `TRUST_REDIS_URL` is set, and without it every cache miss loads the full follow-graph adjacency, so set it before exposing any personalized route
- optional seed-anchored global rank via `TRUST_ENABLE_SEED_TELEPORT` (see algorithm progression above)
- optional interaction-graph merge via `TRUST_ENABLE_INTERACTION_GRAPH` plus admin comparison at `GET /admin/v1/trust/interaction-rank-comparison`

Still deferred: Monte Carlo / random-walk variants.

## Decision rules

When choosing where a trust feature belongs:

- If it defines canonical event truth: it belongs in Postgres ingest/derivation, not Redis.
- If it is graph working state or iterative algorithm state: Redis is acceptable.
- If an API depends on it for stable product behavior: publish the output back into Postgres.
- If it is transport-specific translation: it belongs in `internal/api` or `internal/api_primal`, but only after shared query/store support exists.

## Recommended default

The default target architecture for NostrMash trust/ranking is:

- Postgres for canonical truth and published trust outputs
- Redis for graph working state and advanced ranking computation
- worker- or trust-worker-driven synchronization between the two
- no separate SQLite-based ingest pipeline

That captures the useful parts of Vertex-style WoT without abandoning NostrMash's durability and rebuild guarantees.
