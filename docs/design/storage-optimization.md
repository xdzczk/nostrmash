# Storage optimization and relay fallback

This is a scoped design note for storage optimization and relay fallback work in NostrMash. It is not the general operator guide; use [../operations.md](../operations.md) for runtime procedures and [../observability.md](../observability.md) for live signal interpretation.

It is intentionally not a storage-model rewrite. NostrMash remains a durable local ingest/index/query system. This pass adds bounded best-effort fallback for selected lookup paths and improves storage discipline in safe categories.

## Goals

- reduce long-term storage growth safely
- preserve strong local performance for core paths
- add bounded best-effort relay fallback for selected entity lookups
- keep search local-only in this pass
- improve observability for fallback and storage policy outcomes

## Core requirement

Supported entity lookups must not fail solely because data is absent locally when configured relays can return that entity within bounded fallback limits.

This is a best-effort behavior for supported lookup surfaces only. It is not a guarantee of broad historical completeness.

## Current storage audit

### Canonical durable data (Layer 1)

- `events` (canonical raw JSON + core fields)
- `event_tags` (expanded tag rows)
- `event_relays` (relay provenance)
- `invalid_events` (invalid payload quarantine)
- `ingest_checkpoints` (relay progress durability)

Canonical truth is rooted in `events` and heavily referenced by derived/projection tables via foreign keys.

### Rebuildable derivation/projection data (Layer 2/3)

- Layer 2 style derivations:
  - `event_references`, `pubkey_references`
  - `replaceable_state`
  - derivation version/rebuild control tables
- Layer 3 style read models:
  - `profiles_latest`, `author_recent_events`
  - `thread_edges`, `unresolved_thread_references`
  - counts and contribution tables
  - `reaction_events`, `repost_events`, `deletion_events`
  - replaceable/profile/list projections, follower graph, DM/zap projections
  - curated parity tables

These are rebuildable from lower layers and should be treated differently from canonical durability.

### Operational tables

- `jobs` queue/history
- rebuild run metadata tables
- trust run/state tables

These are durable for operations, not canonical event truth.

### Likely storage-heavy consumers

By schema/code inspection, most likely heavy sources are:

- `events` table (`raw_json` + content fields + indexes)
- `event_tags` cardinality growth
- search-related indexes on `events` and `profiles_latest`
- append-heavy projections such as `author_recent_events`
- edge/contribution tables under high network activity

### Likely high-footprint indexes

- kind/content search indexes on `events`
- profile search indexes on `profiles_latest`
- high-cardinality reference/edge indexes (`event_tags`, references, followers, thread/projection tables)

## Current read-path audit

### Local-only assumptions today

The shared query services and many handlers currently assume local presence:

- event reads: `GetEventRawByID` / `GetEventRawsByIDs`
- profile reads: `GetProfileByPubkey` / `GetProfilesByPubkeys`
- thread assembly reads local ancestors/replies and may return not found or missing ancestor IDs

On local miss, current behavior generally maps to `not_found` (or missing arrays in batch responses) without relay lookup.

### Mandatory Phase 1 fallback surfaces

Direct entity fallback (lower risk):

1. event lookup by id
2. batch event lookup by ids
3. profile lookup by pubkey
4. batch profile lookup by pubkeys

### Optional higher-risk surfaces (deferred unless clean)

Context-completion fallback:

- missing thread root/ancestor completion
- selected compatibility/WS lookup paths only when they naturally reuse shared services cleanly

## Explicit non-goals (this pass)

- broad relay-backed text/content search
- global archival completeness across relays
- aggressive canonical pruning of `events`/`event_tags`
- major ingest/storage architecture redesign
- transport-layer fallback duplication across surfaces

## Relay query ownership model

### Chosen ownership

- Fallback relay querying is owned by the `api` process.
- Fallback orchestration is owned by shared query/application services (`internal/query`).
- HTTP/WS handlers remain thin transport shapers and should not independently implement relay fallback loops.

### Runtime/config shape

Fallback is explicitly controlled by API config:

- enabled flag
- event fallback URL floor (`API_RELAY_FALLBACK_URLS`, else `INGESTOR_RELAY_URLS`) plus optional live ranking from the fastest healthy registry relays
- profile fallback URL set (`API_RELAY_FALLBACK_PROFILE_URLS`, default `wss://purplepag.es`)
- request timeout
- relay fanout bound

This avoids overloading diagnostics-only intent and makes fallback behavior operator-visible.

### Deferred if runtime is insufficient

If full runtime support is not available for higher-risk context completion, only mandatory direct-entity surfaces are enabled and optional surfaces are deferred.

## Storage classes for this repo

1. Canonical durable local data
   - ingest truth and provenance required for correctness/rebuild
2. Rebuildable derived/projection data
   - read models that can be rebuilt from canonical truth
3. Bounded transient cache data
   - optional, size/time bounded helper cache
4. Relay-fallback-only non-durable results
   - returned to caller without requiring durable persistence

## Caching decision (Phase 1)

Phase 1 default: **no caching** for relay-fetched fallback results.

Reasoning:

- minimizes scope/risk for first pass
- avoids quietly reintroducing unbounded durable growth
- keeps fallback behavior explicit and bounded

If caching is introduced later, prefer a bounded separate cache class over canonical write-through by default.

## Safe first-pass storage optimization targets

Prioritize safe categories before canonical retention changes:

1. improve visibility for major table/category growth
2. bound/prune clearly non-canonical operational data where policy is straightforward
3. review projection growth risks and implement bounded retention only where semantics are well tested
4. keep index reductions evidence-driven and conservative

## Safe first-pass retention/bounding strategy

- no canonical `events`/`event_tags` aggressive pruning in this pass
- no broad projection pruning without query/regression tests
- any introduced pruning must be explicit, bounded, observable, and documented

Implemented first-pass bounded retention:

- worker periodically purges old terminal `jobs` rows (`succeeded` and `dead`)
- worker periodically purges old `invalid_events` rows
- optional second-stage `invalid_events` payload trimming can clear `raw_payload` earlier while retaining error metadata for debugging
- retention is explicit and bounded by:
  - `WORKER_JOB_RETENTION_SUCCEEDED_MAX_AGE`
  - `WORKER_JOB_RETENTION_DEAD_MAX_AGE`
  - `WORKER_JOB_RETENTION_RUN_INTERVAL`
  - `WORKER_JOB_RETENTION_DELETE_BATCH_LIMIT`
  - `WORKER_INVALID_EVENTS_RETENTION_MAX_AGE`
  - `WORKER_INVALID_EVENTS_RETENTION_RUN_INTERVAL`
  - `WORKER_INVALID_EVENTS_RETENTION_DELETE_BATCH_LIMIT`
  - optional payload trim controls:
    - `WORKER_INVALID_EVENTS_PAYLOAD_TRIM_ENABLED`
    - `WORKER_INVALID_EVENTS_PAYLOAD_TRIM_MAX_AGE` (must be shorter than full-row retention max age)
    - `WORKER_INVALID_EVENTS_PAYLOAD_TRIM_BATCH_LIMIT`
- canonical ingest truth (`events`, `event_tags`, `event_relays`) is unchanged

## Observability requirements

For mandatory fallback surfaces, emit low-cardinality telemetry for:

- local hit
- local miss
- relay fallback attempt
- relay fallback success
- relay fallback miss/failure
- relay fallback latency

Storage growth visibility now also includes:

- `nostrmash_storage_database_bytes`
- `nostrmash_storage_table_bytes{table}`
- `nostrmash_storage_table_rows{table}`

If caching/pruning is enabled in future, also emit:

- cache insertions
- pruning activity

## Expected deferred work

- broad context-completion fallback (thread ancestor/root live completion) unless cleanly supported by the shared fallback abstraction
- relay-backed search
- canonical retention policy changes
