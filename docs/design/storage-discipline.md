# Storage discipline

This is a scoped design note for the storage-discipline pass on NostrMash. It is not a redesign of the storage model; it is the plan we use to stop unbounded growth in places where the discipline is currently weak. Use [storage-optimization.md](storage-optimization.md) for the broader fallback/optimization context, [../operations.md](../operations.md) for runtime procedures, and [../observability.md](../observability.md) for live signal interpretation.

NostrMash remains a durable local ingest/index/query system. Canonical event truth stays canonical. Rebuildable projections stay rebuildable. What we change here is whether each persisted/indexed structure justifies its storage cost, and whether retention/bounding is explicit and enforced.

## Goals

- Stop uncontrolled storage growth driven by schema/index/retention choices.
- Make every persisted table fit a clear class: canonical, rebuildable, or operational.
- Make every retained index have an explicit query owner.
- Make every rebuildable projection either bounded, prunable, or explicitly justified as unbounded.
- Treat operational exhaust (queue rows, transient state, helper materializations) as cost that must be controlled, not data that must be preserved.

## Implementation status

All phases below are implemented. The shipped state, in dependency order:

- **Phase 1 (jobs lifetime)** — shipped earlier; see `## Phase 1` below.
- **Phase 2 (index audit surface)** — `GET /admin/v1/storage/indexes` reports
  `pg_stat_user_indexes` usage plus per-table live/dead tuple counts. Index
  drops remain operator-gated on that evidence; none were dropped by inertia
  (the `pubkey_references` indexes left with the table).
- **Phase 3 (`pubkey_references`)** — option (a) shipped: the table is dropped
  (migration `000053`), `GetEventsReferencingPubkey` reads canonical
  `event_tags` via `idx_event_tags_p_lookup`, and the relationships derivation
  no longer materializes p-tag references. This also gives that partial index
  a live query owner, resolving the `idx_scan = 0` ambiguity below.
- **Phase 4 (rebuildable retention)** — shipped as five worker loops, all
  env-tunable and on by default:
  - untrusted-author canonical retention (`WORKER_RETENTION_UNTRUSTED_AUTHOR_*`,
    14 d default, fail-safe no-op while `trust_graph_snapshot` is empty);
  - `author_recent_events` age + per-author cap
    (`WORKER_RETENTION_AUTHOR_RECENT_*`, 90 d / 200 rows);
  - `search_documents` body trim + orphan prune
    (`WORKER_RETENTION_SEARCH_DOCS_*`, 30 d / 280 chars);
  - `event_relays` duplicate-provenance prune retaining the earliest row per
    event (`WORKER_RETENTION_EVENT_RELAYS_*`, 180 d);
  - trust-retention hooks wired to durable deletes (`TRUST_RETENTION_*`):
    stale `trusted_*_discovery_candidates` and idle low-value `account_states`
    rows (the latter scope ships disabled, as before).
- **Phase 5 (observability + backstop)** — `TrackedStorageTables()` covers the
  full projection surface; the storage governor ships with a real capacity
  budget in the compose files (50 GiB default), logs an error when unset, and
  its aggressive drain now triggers the new loops too. Migration `000055` adds
  LZ4 TOAST compression for `raw_json` and per-table autovacuum tuning;
  [../operations.md](../operations.md#storage-reclamation) documents the
  one-time `pg_repack` shrink.

## Top storage offenders (production snapshot)

This is the snapshot that motivates the work. Use it to read the rest of the doc.

- Postgres database: 38 GB across roughly 9.5 days of runtime.
- `event_tags`: 15 GB total (table 6918 MB, indexes 8941 MB, ~29.6 M rows).
- `events`: 6985 MB total (table 2624 MB, indexes 1943 MB, ~1.86 M rows).
- `pubkey_references`: 5247 MB total (table 1902 MB, indexes 3344 MB, ~10.4 M rows).
- `jobs`: 2994 MB total (table 1660 MB, indexes 1334 MB, ~5.55 M rows in 9.5 days).
- `follower_edges`: 2124 MB total (table 336 MB, indexes 1787 MB, ~1.18 M rows).
- `search_documents`: 2035 MB total (table 1117 MB, indexes 696 MB, ~836 K rows).
- `event_relays`: 974 MB total (~3.49 M rows).

Two structural facts come out of this:

1. The `jobs` table is being treated as a permanent archive. ~580 K rows/day with the current 30 d / 180 d retention defaults means a steady-state of ~17 M succeeded rows. That is queue exhaust, not history.
2. `event_tags` is carrying multi-GB partial indexes that backed DM/parity/highlight surfaces; if those surfaces are not exercised in this deployment (`idx_scan = 0`), the indexes are rent without revenue.

Index scan numbers from production:

- `event_tags_pkey`: 4811 MB, idx_scan 85,873,024 (production-hot).
- `idx_event_tags_p_lookup`: 3754 MB, idx_scan 0 (defined for DM/mentions p-tag joins).
- `idx_event_tags_tag_name_value`: 250 MB, idx_scan 0.
- `idx_event_tags_e_lookup`: 134 MB, idx_scan 0.
- `pubkey_references_pkey`: 3228 MB, idx_scan 11,136,833.
- `idx_pubkey_references_referenced`: 117 MB, idx_scan 1250.

## Storage classification

Every table falls into exactly one class.

### Canonical roots (never auto-pruned)

- `events` — raw signed events. Source of truth for everything else. Pruning canonical events is out of scope for this pass.
- `event_tags` — expanded tag rows per event. Canonical because they are produced from `events.raw_json` at ingest time and used as a join target by many downstream tables. Pruning is not allowed.
- `event_relays` — provenance of which relays delivered each event. Append-only, canonical for ingest provenance.
- `invalid_events` — quarantine of payloads that failed validation. Already has age + payload-trim retention via `WORKER_INVALID_EVENTS_*`.
- `ingest_checkpoints` — durable per-relay ingest cursor. Operational but canonical-shaped.

### Operational / queue (must have explicit retention)

- `jobs` — generic job queue. Live queue + lease state; terminal rows are bounded by `WORKER_JOB_RETENTION_*`. Phase 1 tightens this. References from `projection_rebuild_runs`, `trust_runs` are `ON DELETE SET NULL`, so purging terminal rows is safe and does not break run bookkeeping.
- `projection_rebuild_runs` — rebuild dispatch state. Small, low-volume; no retention required for now.
- `trust_runs` — trust pipeline run state. Small; no retention required for now.
- `derivation_active_versions`, `derivation_versions` — projection version control. Tiny.

### Rebuildable derived (Layer 2 / Layer 3)

All of these can be rebuilt from canonical events and lower projections. They should be either bounded, prunable, or explicitly justified.

- `pubkey_references` — derived p-tag references with relation classification. Currently unbounded. Used only by `GetMentions` ([internal/store/compat_queries_events.go](../../internal/store/compat_queries_events.go)) and the primal `user_mentions` cache. Phase 3 candidate for redesign or replacement.
- `event_references` — derived e-tag references. Same shape concern; less load.
- `replaceable_state` — replaceable-key index. Cardinality bounded by distinct (pubkey, kind, d_tag) coordinates, not raw event count.
- `follower_edges` — kind-3 contact graph derived from events. Unique (followed_pubkey, follower_pubkey). Bounded by social graph size, not by ingest velocity. Indexes are large relative to the table; index audit is a Phase 2 follow-up.
- `event_hashtags`, `event_urls` — extracted tag projections. Bounded by note count.
- `profiles_latest`, `contact_lists_latest`, `relay_lists_latest` — replaceable-event projections. Bounded by distinct authors.
- `author_recent_events` — every ingested event for an author. **Unbounded by design today** (one row per event per author, no kind filter). Already flagged in [storage-optimization.md](storage-optimization.md). Phase 4 retention target.
- `author_*_stats` (analytics suites under `migrations/000029` and `000030`): bounded grids per author/window; not unbounded.
- `note_discovery_stats` — one row per kind-1 note. Bounded by note count. Self-deletes when source is missing or kind is not projectable.
- `profile_discovery_stats` — one row per scored pubkey. Self-deletes when all metrics are zero.
- `thread_summaries`, `thread_edges`, `unresolved_thread_references` — thread graph. Bounded by participating roots.
- `search_documents` — Meili-style local search index, trigger-maintained from `profiles_latest` / `events` (kind 1) / `event_hashtags` / `ingest_checkpoints`. Rebuildable. No retention currently. `body` holds full note content for search. Phase 4 candidate for body trim or stale-row prune.
- `trusted_note_discovery_candidates`, `trusted_profile_discovery_candidates`, `trusted_discovery_projection_state` — trust-qualified discovery. Refreshed in bulk with the trust snapshot. Parent FKs were dropped (migration `000051`) because concurrent parent-row deletes raced the refresh INSERT and aborted `trust_graph_snapshot` rebuilds; soft cleanup (`DELETE WHERE NOT EXISTS` parent) remains. `TrustRetentionHooks` exist in config ([internal/config/trust_retention.go](../../internal/config/trust_retention.go)) but are not wired into actual deletes. Phase 4.
- `trust_graph_snapshot`, `trust_scores_global` — trust outputs. Bulk-refreshed. Bounded by distinct pubkeys.
- `trust_scores_global_stage` — per-run staging for score promote. Cleared for the promoted run (and any already-terminal runs) on successful promote; must not accumulate across runs.
- Reaction / repost / deletion / DM / zap / curated parity tables — projection family, bounded by activity volume.

## Index ownership audit

Every retained index must have an owner. Anything we cannot tie to a hot read path or a documented startup/rebuild path is a removal candidate.

### `event_tags`

- `event_tags_pkey` `(event_id, tag_index, value_index)` — production-hot. Owners: insert path ([internal/store/events_canonical.go](../../internal/store/events_canonical.go)), per-event reads in DM unread / replaceable / note media flag projections, replay snapshot ordered scan. Keep.
- `idx_event_tags_tag_name_value` `(tag_name, value) WHERE tag_name IN ('e','p')` — owners: moderation surfaces in [internal/store/parity_moderation.go](../../internal/store/parity_moderation.go). Largely overlaps with the partial p/e lookup indexes for direct value lookups. Verify in Phase 2; candidate for removal if redundant.
- `idx_event_tags_p_lookup` `(value, event_id, tag_index) WHERE tag_name='p' AND value_index=0` — owners: DM parity ([internal/store/parity_dm_messages.go](../../internal/store/parity_dm_messages.go), [internal/store/parity_dm_contacts.go](../../internal/store/parity_dm_contacts.go), [internal/store/parity_dm_counts.go](../../internal/store/parity_dm_counts.go)) and the DM unread derivation ([internal/derivation/handlers_dm_unread.go](../../internal/derivation/handlers_dm_unread.go)). Code-hot, but production reports `idx_scan=0`. Phase 2 must reconcile this before any drop: either the DM endpoints are not exercised in this deployment, or stats were reset.
- `idx_event_tags_e_lookup` `(value, event_id, tag_index) WHERE tag_name='e' AND value_index=0` — owner: highlights-by-event-id in [internal/store/compat_queries_followers_highlights.go](../../internal/store/compat_queries_followers_highlights.go). Same `idx_scan=0` reconciliation needed.

### `pubkey_references`

- `pubkey_references_pkey` `(source_event_id, tag_index, referenced_pubkey, relation)` — 4-column wide PK explains the 3.2 GB footprint. Insert path: [internal/derivation/handlers_relationships.go](../../internal/derivation/handlers_relationships.go). Kept implicitly by every read.
- `idx_pubkey_references_referenced` `(referenced_pubkey, relation)` — owner: `GetEventsReferencingPubkey` in [internal/store/compat_queries_events.go](../../internal/store/compat_queries_events.go). idx_scan = 1250, low traffic.

If `pubkey_references` is kept as-is, both indexes stay. If Phase 3 redesigns the table, this section is rewritten.

### `follower_edges`

- `follower_edges_pkey` `(followed_pubkey, follower_pubkey)` — production-hot. Keep.
- `idx_follower_edges_lookup` `(followed_pubkey, contact_list_created_at DESC, source_event_id DESC, follower_pubkey ASC)` — owner: ranked-follower listing. Verify in Phase 2.
- `idx_follower_edges_by_follower` `(follower_pubkey)` — owner: reverse lookup. Verify in Phase 2.

### `search_documents`

- `idx_search_documents_search_tsv` GIN — owner: full-text search.
- `idx_search_documents_type_popularity` `(entity_type, popularity DESC, freshness DESC)` — owner: ranked search.
- `idx_search_documents_freshness` `(freshness DESC, updated_at DESC)` — owner: recency-sorted listings.

All three are part of the search read path. Phase 4 may shrink the table by trimming `body` for stale rows; the indexes themselves are not the bottleneck.

## Retention candidates (ranked by impact)

1. **`jobs`** — biggest leverage. Existing retention has too generous defaults (30 d / 180 d) and the trust worker never runs the retention loop. Also, retention currently uses `updated_at`, which moves on every claim/retry/maintenance touch and so is not a faithful "finished_at". **Phase 1.**
2. **`pubkey_references`** — second biggest and grows ~5 rows per ingested event with current p-tag patterns. Either drop in favor of `event_tags WHERE tag_name='p'` (which already has a partial index) or slim the PK. **Phase 3.**
3. **`author_recent_events`** — third biggest unbounded growth driver. One row per event per author with no kind filter. Cap to last N per author or last 30 d. **Phase 4.**
4. **`search_documents`** — body-heavy. Either trim `body` for stale rows or rebuild on demand. **Phase 4.**
5. **`trusted_*_discovery_candidates`** — wire dormant `TrustRetentionHooks` to actual deletes. **Phase 4.**
6. **`event_tags` partial indexes** — only after Phase 2 confirms that the DM/parity/highlights endpoints are not in this deployment's scope. Drop confirmed-dead indexes. **Phase 2.**

## Tier 2/3: trust-bounded ingest + engagement retention (shipped)

These changes bound storage growth at the source rather than pruning canonical notes after the fact.

### Trust-bounded canonical ingest (ingest gate)

- **Author gate** (kinds `1`/`4`/`9802`/`10000`/`10003`/`30023`): persist authored content (notes, DMs, highlights, mute/bookmark lists, long-form articles) only when the author is in `trust_graph_snapshot` within `INGESTOR_TRUST_GATE_MAX_HOPS` of a seed. Kind `4` DMs gate on the sender.
- **Kinds `6`/`7`/`9735` target gate**: persist engagement only when the target event already exists locally (self-consistent; lossy if engagement arrives before its target).
- **Open kinds** (`0`, `3`, `5`, `10002`): still ingested so the trust graph and profiles can bootstrap.
- **Shadow rollout**: default `INGESTOR_TRUST_GATE_MODE=open` records `nostrmash_ingest_gate_decisions_total` without rejecting; flip to `trusted_only` after trusted-set metrics look sane.
- **Prerequisites**: `trust_worker` reconciles seeds, refreshes `trust_graph_snapshot`, and schedules global trust runs.

See [architecture/trust-bounded-ingest.md](../architecture/trust-bounded-ingest.md).

### Engagement raw retention

- **Target**: raw `events WHERE kind IN (6,7,9735)` older than `WORKER_RETENTION_ENGAGEMENT_MAX_AGE` (default 14d).
- **Survivors**: `reaction_counts`, `repost_counts` (no FK to `events`); windowed discovery scores unaffected.
- **Derivation-safe guard**: do not purge while `derive_event_bundle` is pending/running, or dead within `WORKER_RETENTION_ENGAGEMENT_DEAD_GRACE` (default 7d).
- **Metrics**: `nostrmash_retention_purged_rows_total{target="engagement_events"}`.

### Superseded replaceable retention

- **Target**: raw `events WHERE kind IN (0,3,10000,10002,10003)` (non-parameterized, `d_tag=''`) plus parameterized `30023` (matched per `(pubkey,kind,d_tag)`) that are strictly superseded by a newer winner in `replaceable_state` and whose `first_seen_at` is older than `WORKER_RETENTION_REPLACEABLE_MIN_AGE` (default 24h).
- **Survivors**: the current winner plus `contact_lists_latest`, `relay_lists_latest`, `profiles_latest`, `replaceable_state` (all reference the winner). A newer-but-unprojected version is protected because it ranks above the recorded winner.
- **Derivation-safe guard**: do not purge while `derive_event_bundle` is pending/running, or dead within `WORKER_RETENTION_REPLACEABLE_DEAD_GRACE` (default 7d).
- **Payoff**: kind `3` dominates — superseded contact lists carry the largest tag fan-out, so this is the primary lever against `event_tags` / `pubkey_references` growth; superseded `30023` article revisions are next because long-form content is the largest per-event payload.
- **Metrics**: `nostrmash_retention_purged_rows_total{target="replaceable_events"}`.

### Processed deletion retention

- **Target**: raw `events WHERE kind = 5` older than `WORKER_RETENTION_DELETION_MAX_AGE` (default 14d) whose derivation has completed.
- **Survivors**: the distilled `deletion_events` ledger `(deleter_pubkey, target_event_id, created_at)`. Migration `000050` dropped the `deletion_events.event_id` FK cascade so the tombstone outlives the raw event.
- **Derivation-safe guard**: the `derive_event_bundle` job is enqueued in the same transaction as the event insert, so a freshly-ingested deletion is always blocked until its ledger row is projected; dead jobs stop blocking past `WORKER_RETENTION_DELETION_DEAD_GRACE` (default 7d).
- **Payoff**: kind `5` is the largest raw-event population on the firehose; the ledger is a fraction of the raw size.
- **Metrics**: `nostrmash_retention_purged_rows_total{target="deletion_events"}`.
- **Rebuild caveat**: a full projection rebuild cannot recreate `deletion_events` rows whose raw kind-5 event was purged. This matches the engagement-retention tradeoff: retention trades rebuildability for the durable distilled form.

### What remains unbounded (by design)

- Canonical events from trusted authors (retention deferred by design; the
  trust graph bounds this to social-graph-sized growth, not firehose growth).
- Untrusted-author events are now bounded by the Phase 4 retention loop;
  `pubkey_references` is dropped; `author_recent_events` and
  `search_documents` are bounded by their Phase 4 loops.

## Implementation order

1. **Phase 1 (this PR)**: jobs lifetime correctness + tighter retention + trust-worker retention + per-status/per-job_type metrics. See `## Phase 1` below.
2. **Phase 2**: gated `event_tags` index audit. Add an admin endpoint that reports `pg_stat_user_indexes` for the four indexes; only drop after operators confirm idx_scan stays at 0 in a fresh window AND DM/mentions endpoints are not enabled here.
3. **Phase 3**: classify `pubkey_references`. Two candidate paths: (a) drop the table; rewrite `GetEventsReferencingPubkey` against `event_tags WHERE tag_name='p' AND value=$1` using `idx_event_tags_p_lookup`; (b) slim PK to `(referenced_pubkey, source_event_id)` with `relation` and `tag_index` as columns. Pick after measuring (a) latency.
4. **Phase 4**: rebuildable retention framework. Concrete first targets: `author_recent_events` (per-author cap + age cap), `search_documents` body trim, wire `TrustRetentionHooks` into actual deletes, document `search_documents` re-sync behavior.
5. **Phase 5**: storage observability. Extend `TrackedStorageTables()` in [internal/api/admin_storage.go](../../internal/api/admin_storage.go) to cover `search_documents`, `note_discovery_stats`, `profile_discovery_stats`, `thread_summaries`, `pubkey_references`, `event_references`, `event_hashtags`, `event_urls`. Extend the slope recording rule. Add per-table-family growth alerts.

## Phase 1: jobs lifetime correctness

Concrete changes shipped in this PR:

- New column `jobs.finished_at TIMESTAMPTZ` set on the terminal transition (`succeeded` via `CompleteJob`, `dead` via `FailJob` or stale recovery). It is **not** touched on retry-to-pending.
- New partial index `idx_jobs_terminal_finished_at ON jobs (status, finished_at) WHERE status IN ('succeeded','dead')` to make the purge cheap.
- `PurgeTerminalJobs` filters by `finished_at`, not `updated_at`. Old rows are backfilled (`finished_at = updated_at WHERE status IN ('succeeded','dead')`) by the migration.
- Tighter defaults: `WORKER_JOB_RETENTION_SUCCEEDED_MAX_AGE=24h`, `WORKER_JOB_RETENTION_DEAD_MAX_AGE=336h` (14 d), `WORKER_JOB_RETENTION_RUN_INTERVAL=15m`, `WORKER_JOB_RETENTION_DELETE_BATCH_LIMIT=2000`.
- Retention loop now also runs in the trust worker (was main worker only). Trust jobs were effectively never purged.
- Auto-pacing in the retention loop ([internal/jobs/retention.go](../../internal/jobs/retention.go)): when a delete batch comes back saturated (`deleted >= DeleteBatchLimit`), the loop immediately re-runs after a short courtesy pause instead of sleeping for `RunInterval`. This makes `DeleteBatchLimit` a per-batch chunking knob, not a throughput ceiling, so transient backlogs (operator-induced or workload spikes) drain at disk speed without anyone retuning env defaults. Steady-state cost is unchanged: a below-limit batch returns to the normal `RunInterval` sleep. A `job_retention_catchup` log line is emitted every 50 consecutive saturated batches so operators can see sustained burndowns.
- New gauges: `nostrmash_jobs_rows{status,job_type}` and `nostrmash_jobs_oldest_finished_age_seconds{status}`. Cardinality is bounded by the fixed enum of known job types in [internal/jobs/types.go](../../internal/jobs/types.go); unknown types are reported under `job_type="other"`.

### Acceptance criteria

- After deploy + one `RunInterval` tick, no `succeeded` row in `jobs` is older than 24 h.
- Trust worker logs include `job_retention_enabled` and `job_retention_purged` events.
- `nostrmash_jobs_rows{status,job_type}` and `nostrmash_jobs_oldest_finished_age_seconds{status}` are visible.
- No FK breakage in `projection_rebuild_runs` / `trust_runs` (FKs are `ON DELETE SET NULL`, so terminal purge nulls the pointer safely).

### Migration / rollback

- Migration `000040_jobs_finished_at.sql` is additive (new nullable column + new partial index + one-shot UPDATE). Rollback: revert the deploy and drop the column and index. Workers running the OLD code keep writing terminal rows without `finished_at`; that is safe because the column is nullable. The backfill `UPDATE` is inline; for very large `jobs` tables operators can run it manually before deploying so the inline statement is a no-op.

## Non-goals

- Pruning trusted-author `events` or their `event_tags`. Those are canonical.
  (Untrusted-author events are pruned by the Phase 4 loop; that is a
  trust-policy decision, not canonical-data cleanup.)
- Building a job-history archive subsystem. Bounded retention is enough.
- ClickHouse / external store / archival side-system to dodge local discipline.
- Removing indexes by inertia without `pg_stat_user_indexes` evidence over a fresh window.
- Manual DBA cleanup as a steady-state strategy.
- Making the retention loop "smart" per-job-type until evidence shows uniform retention is wrong. The smallest credible fix first.
