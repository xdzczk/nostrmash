# Incremental author/profile stats derivation

Scoping note for replacing full-history recompute with incrementally-maintained
counters in the author-analytics and profile-stats projections. This is a
follow-on to the mark-and-sweep coalescing design already in place
(`pending_author_analytics_recomputes` / `pending_profile_stats_recomputes`,
see the comment header in `migrations/000041_pending_author_analytics.sql`).
That design solved "N events → N inline rebuilds"; this note addresses what's
left: "1 coalesced rebuild → still an unbounded full-history/full-window scan."

## Implementation status

Implemented (cutover defaults on):

- Migration `000063_incremental_author_stats.sql` (`applied_stat_deltas`,
  `author_hashtag_daily`, `author_media_daily`, `author_hourly_activity`)
- Bundle step `ApplyIncrementalAuthorStats` with exactly-once ledger claims
- Follower/following ±1 deltas from kind=3 contact-list diffs
- Sweeper skips full `profile_public_stats` / `author_activity_daily` rebuilds
  when incremental flags are enabled
- Windowed topic/media/hourly roll-ups from fine-grained daily tables (with
  source-scan fallback when daily rows are absent)
- Feature flags (default `true`):
  - `WORKER_INCREMENTAL_PROFILE_PUBLIC_STATS`
  - `WORKER_INCREMENTAL_AUTHOR_ACTIVITY_DAILY`
  - `WORKER_INCREMENTAL_WINDOWED_ROLLUPS`

- Migration `000064_backfill_incremental_stat_daily_tables.sql`: one-time
  bulk backfill of `author_hashtag_daily` / `author_media_daily` /
  `author_hourly_activity` for the last 90 days (the largest supported
  `window_days`), so the "any row after cutoff ⇒ use only the daily table"
  windowed-rollup check doesn't silently drop pre-deploy history for
  already-active pubkeys. Idempotent (overwrite, not additive), safe to
  re-run, safe to run after the incremental writer is already live.
- Retention decrement path: `derivation.Handlers.ReverseIncrementalAuthorStatsTx`
  undoes the exact deltas `ApplyIncrementalAuthorStats` applied for an event,
  gated by the same `applied_stat_deltas` ledger (unclaim-then-decrement
  mirrors claim-then-increment). Wired into the two retention purges that
  hard-delete events contributing to these counters —
  `PurgeExpiredEngagementEvents` (kinds 6/7/9735) and
  `PurgeUntrustedAuthorEvents` (kind 1 notes, primarily) — via a narrow
  `retention.IncrementalStatsReverser` interface set post-construction in
  `bootstrap.go` (avoids an `internal/store/retention` → `internal/derivation`
  import). Deliberately does not roll back `recent_activity_at` (see the
  doc comment on `ReverseIncrementalAuthorStatsTx`); NIP-09 deletion requests
  don't hard-delete the target event directly, so this only fires from
  age-based retention purges.
- Periodic reconciliation sampler: `derivation.Handlers.ReconcileIncrementalAuthorStatsSample`
  full-recomputes a sample of pubkeys (half most-recently-active, half
  uniform-random) read-only and compares against the live
  `profile_public_stats` / `author_activity_daily` values, logging
  `incremental_stats_reconciliation_mismatch` and incrementing
  `nostrmash_worker_incremental_stats_reconciliation_mismatches_total{projection,field}`
  for every divergence. Run by `RunIncrementalStatsReconciliationLoop`
  (`WORKER_INCREMENTAL_STATS_RECONCILIATION_*`, default hourly / 200
  pubkeys). This is exactly the mechanism that would have caught the
  `profile_public_stats` fan-out gap fixed in
  `da431a9` (kind-1-only updates vs. full rebuild's all-kinds scope) had it
  existed at the time.
- `applied_stat_deltas` retention pruning: `Retention.PruneOrphanedAppliedStatDeltas`
  (`WORKER_RETENTION_APPLIED_STAT_DELTAS_*`, default every 6h) deletes ledger
  rows whose source event has already been deleted. Deliberately *not*
  age-based — a live event's ledger row must survive for the event's entire
  lifetime, since a future reversal-aware retention purge may still need it
  to gate a decrement (see the doc comment on `PruneOrphanedAppliedStatDeltas`
  for why an age-only purge would silently reintroduce the exact upward-drift
  bug the reversal path exists to prevent). Real orphans come from the two
  purges that hard-delete events without touching incremental stats
  (`PurgeSupersededReplaceableEvents`, `PurgeProcessedDeletionEvents`).

This closes out every item scoped in this design.

## Current state (why the sweeper coalescing wasn't enough)

The mark-and-sweep design collapses bursts of dirty-marks for the same pubkey
into a single rebuild per sweeper cycle. But each rebuild still recomputes its
numbers **from scratch** by re-scanning base tables:

- `ProjectProfilePublicStats` runs `COUNT(*) FROM events WHERE pubkey = $1 AND
  kind = 1 AND [NOT] EXISTS (reply ref)` — i.e. rescans *every note the author
  has ever written* — plus full `follower_edges` scans for follower/following
  counts. This happens on every rebuild, for an account that may have
  thousands of notes.
- `rebuildAuthorActivityDailyTx` runs 4 CTEs joining
  `events`/`event_references`/`reaction_events`/`repost_events`/`zap_receipts`
  over a lookback window, for every rebuild.
- `rebuildAuthorWindowedStatsTx` repeats similarly heavy queries per
  `window_days` (7, 30) for `author_engagement_stats`, `author_topic_stats`,
  `author_media_mix_stats`, `author_activity_windows`, `author_posting_patterns`.

Coalescing reduces *how often* this runs per pubkey, but the *cost per run* is
still `O(account's full history)` or `O(full window scan across 5 tables)`.
For popular/high-degree accounts this is 30-160s per rebuild (already
documented in `projection_author_activity_daily.go`), and it's what's
currently saturating Postgres CPU/IO (see prior diagnosis: the Postgres
container itself was observed at ~1072% CPU while all app containers were
essentially idle — the cost lives entirely in these queries).

Reducing sweeper concurrency only serializes this same expensive work; it
does not reduce the per-rebuild cost, so it doesn't remove the ceiling — it
only moves it further out. As ingest volume grows (more relays, more users),
the mark rate grows and we hit the wall again.

## Goal

Make the steady-state cost of processing one new event `O(1)` (a handful of
indexed upserts) instead of `O(account history)` / `O(window scan)`, while
preserving:

- correctness (numbers must stay right, forever, under retries/replays/backfill overlap)
- the existing derivation-version/rebuild-on-schema-change machinery
  (`derivation_active_versions`, full-rebuild code paths used for backfills)
- the existing claim/lease/timeout safety net pattern for background work

Non-goals for this pass: changing what the API surfaces, changing the
schema of the *output* tables (`profile_public_stats`,
`author_activity_daily`, `author_engagement_stats`, etc.) — only how they get
populated.

## Core design principle

**Compute deltas at the point where we already know them (event ingest time),
apply them as cheap indexed increments, and keep full recompute only as an
infrequent correctness backstop — not the steady-state path.**

This works because, for every event that currently triggers a dirty-mark, we
already know at mark-time exactly what changed:

- a new kind-1 note/reply → +1 to the author's `note_count` or `reply_count`,
  and +1 to the author's `(pubkey, activity_date)` bucket in
  `author_activity_daily`, keyed off the event's own `created_at` — no scan
  required.
- a new reaction/repost/reply/zap targeting someone → +1 to that target's
  `engagement_received` bucket for the *interaction event's* `created_at`
  date (this is exactly how `given_sources`/`received_sources` are already
  keyed today — they use the interaction event's `created_at`, not the
  target's), and +1 to the actor's `engagement_given` bucket.
- a kind-3 contact list diff (already computed in
  `projection_replaceable_lists.go` to add/remove `follower_edges` rows) →
  each added/removed edge is directly a ±1 to `follower_count` /
  `following_count` for the two pubkeys involved.
- a deletion (NIP-09 kind 5, or retention-driven event removal — see
  `internal/store/retention/events_retention_deletion.go`) → the mirror
  decrement of whichever of the above applied when the event was created.

None of this requires scanning history — the delta is fully determined by the
single event being processed.

## Per-table plan

### 1. `profile_public_stats` (note_count, reply_count, recent_activity_at)

Replace the `COUNT(*)` scan with a direct `UPDATE ... SET note_count =
note_count + $delta, reply_count = reply_count + $delta, recent_activity_at =
GREATEST(recent_activity_at, $new_created_at)` at the same point
`MarkAuthorAnalyticsDirty`/`MarkProfileStatsPending`-equivalent logic runs
today. No sweeper needed for this piece at all — it can be applied inline,
since the cost is now a single indexed row update, not a full scan.

### 2. `profile_public_stats` (follower_count / following_count)

Hook into the existing contact-list diff in `projection_replaceable_lists.go`
(it already computes which `follower_edges` rows are added/removed). Emit a
±1 delta per edge change to both the follower's `following_count` and the
followed's `follower_count`, instead of recomputing via `COUNT(*) FROM
follower_edges`.

### 3. `author_activity_daily`

Same treatment: turn the full CTE rebuild into a direct `INSERT ... ON
CONFLICT (pubkey, activity_date) DO UPDATE SET post_count =
author_activity_daily.post_count + EXCLUDED.post_count, ...` per affected
pubkey/day, computed directly from the triggering event. This table becomes
fully incrementally maintained with no sweeper involvement, eliminating the
single most expensive part of the current rebuild (4 joined CTEs across 5
tables).

### 4. Windowed roll-ups (`author_engagement_stats`, `author_topic_stats`,
`author_media_mix_stats`, `author_activity_windows`, `author_posting_patterns`)

These need rolling-window semantics (7/30/90 days) — a plain increment can't
"age out" old data on its own, since an account can be quiet for weeks while
the window boundary keeps moving. Two changes here:

- **Recompute source**: once `author_activity_daily` is incrementally
  maintained and small (one row per pubkey per active day), roll these
  windowed tables up **from `author_activity_daily`** (bounded to ≤90 rows
  per pubkey) instead of from raw `events`/`event_references`/`reaction_events`/
  `repost_events`/`zap_receipts`. This alone turns an unbounded multi-join
  scan into a bounded ≤90-row aggregate — a large win even without full
  incrementalization of this layer.
- **New fine-grained daily tables** are needed for the sub-day-bucketed
  and categorical stats that `author_activity_daily` doesn't carry today:
  `author_hashtag_daily(pubkey, activity_date, hashtag, count)`,
  `author_media_daily(pubkey, activity_date, with_image/with_video/with_link/text_only counts)`,
  `author_hourly_activity(pubkey, activity_date, day_of_week, hour_of_day, post/engagement counts)`.
  These are incrementally maintained the same way as (3), and the existing
  `window_days`-scoped tables become cheap `SUM(...) WHERE activity_date >=
  cutoff` roll-ups over them, still run by the sweeper (this part keeps the
  existing claim/lease/timeout pattern — it's still periodic batch work, just
  now bounded/cheap instead of unbounded/expensive).

### 5. Idempotency (the part that makes this safe)

The current full-recompute design is accidentally idempotent — re-running a
`COUNT(*)` twice produces the same correct answer. A `+1` delta does not have
that property, so this is the part that needs real care. The pipeline already
has at-least-once characteristics we must assume (e.g.
`INGESTOR_LIVE_RESUME_OVERLAP_SECONDS=60` exists specifically because live
resume can redeliver overlapping events; bundle/job retries on failure are
another source).

Add a small ledger table, e.g.:

```sql
CREATE TABLE applied_stat_deltas (
    event_id TEXT NOT NULL,
    projection TEXT NOT NULL, -- 'profile_public_stats' | 'author_activity_daily' | 'follower_edges' | ...
    PRIMARY KEY (event_id, projection)
);
```

Delta application does `INSERT INTO applied_stat_deltas (event_id, projection)
VALUES ($1, $2) ON CONFLICT DO NOTHING` in the *same transaction* as the
counter update, and only applies the update if the insert actually inserted a
row (0 rows back = already applied, skip). This makes each projection's delta
application exactly-once regardless of redelivery, at the cost of one small
PK-indexed insert per event per projection — negligible next to what it
replaces.

(`applied_stat_deltas` needs its own retention. **Revised from the original
plan below**: a redelivery-window horizon turned out to be unsafe once the
decrement path (§6) was added, because a ledger row also gates *that* — an
event's row must survive for the event's entire lifetime, not just past its
redelivery window, or a later retention purge would silently skip its
decrement and reintroduce drift. The implemented rule prunes a row only once
its source event no longer exists at all; see the "Still open from this
design" section above.)

### 6. Deletions and corrections

Every increment path needs a mirror decrement path for event deletion
(NIP-09) and retention-driven deletion. Since deletes are just another event
flowing through the same derivation entrypoint, this is the same delta
mechanism with the sign flipped, gated by the same `applied_stat_deltas`
ledger (keyed by the deletion event's own id, not the deleted event's id, so
a delete is itself idempotent).

## Correctness backstop: keep full-recompute, make it rare

Incremental systems drift when there's a bug, a missed edge case, or manual
data surgery. We should not remove the full-recompute code path — it already
exists and is well-tested — just stop it from being the steady-state path:

- Keep it as the mechanism for derivation-version bumps (already how
  `resolveDerivationWriteVersion` / full rebuilds work today).
- Add a low-priority background **reconciliation job** that periodically
  (e.g. nightly, low concurrency, low priority) full-recomputes a sample of
  pubkeys (weighted toward high-activity accounts, where drift is most likely
  and most visible) and compares against the incrementally-maintained values,
  alerting/logging on mismatch. This is cheap in aggregate (small sample,
  off-peak) and gives an ongoing correctness signal without paying the O(n)
  cost on every event.
- Manual `nostrmash-admin`-style "recompute this pubkey" escape hatch stays
  available using the existing full-rebuild function, for support/debugging.

## Migration / rollout plan

1. **Backfill baseline**: seed the new fine-grained tables
   (`author_hashtag_daily`, `author_media_daily`, `author_hourly_activity`)
   and confirm `author_activity_daily`/`profile_public_stats` current values
   via one full backfill pass (reusing existing rebuild code), establishing a
   known-correct starting point.
2. **Shadow mode**: land the incremental write path but keep the sweeper's
   full-recompute as the source of truth; write incremental results to
   shadow columns/tables and compare on every rebuild, logging mismatches.
   Run until confident (target: zero mismatches across a representative
   traffic period, including at least one backfill/live-overlap event).
3. **Cutover**: flip reads to the incrementally-maintained tables; demote
   full-recompute to the reconciliation job described above.
4. **Remove the old per-event heavy queries** from the hot path once cutover
   is stable and the reconciliation job has run clean for a defined bake
   period.
5. Feature-flag each projection's cutover independently
   (`profile_public_stats` first — it's the simplest — then
   `author_activity_daily`, then the windowed roll-ups) so a regression in
   one doesn't block the others or require a big-bang rollback.

## Expected impact

- Per-event cost drops from `O(account history)` / `O(window scan across 5
  tables)` to `O(1)` indexed upserts + one small idempotency-ledger insert.
- `pending_*_recomputes` backlogs should stay near-zero in steady state since
  there's no more expensive rebuild to coalesce toward — the sweeper's role
  shrinks to the bounded windowed roll-ups and the periodic reconciliation
  sample.
- Removes the direct link between "how large/popular an account is" and "how
  much it costs the DB every time it gets one more reply" — which is exactly
  the scaling property we need given we're actively increasing relay/ingest
  capacity.

## Open questions / risks

- ~~Exact retention horizon for `applied_stat_deltas`~~ — resolved: it's
  orphan-based, not horizon-based (see §5 revision note and "Still open from
  this design" above).
- Need to confirm all fan-out paths in `authorAnalyticsAffectedPubkeys` /
  the profile-stats equivalent are exhaustively covered by the new delta
  emission points, or a rare event type will silently stop updating stats
  instead of being slow (a worse failure mode — must be caught by the
  reconciliation job, not discovered by users). One instance of exactly this
  was already caught and fixed pre-reconciliation-job (`da431a9`); the
  reconciliation sampler is now live as the ongoing backstop for any others.
- `author_engagement_stats.cadence_posts_per_day` /
  `cadence_posts_per_active_day` are derived ratios computed at roll-up time,
  not raw counters — these stay as roll-up-time computations from the
  fine-grained tables, not incremental themselves.
- This is a meaningful engineering lift (new tables + migrations, new write
  paths in `projection_replaceable_lists.go` and the event-derivation
  entrypoints, an idempotency ledger, a new reconciliation job, and a
  multi-phase rollout) — should be scoped as its own project with its own
  milestones rather than done inline with other work.
