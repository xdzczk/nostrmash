# Trust Phases 2–4 Implementation Plan

This is the implementation plan for closing out [trust-subsystem.md](../architecture/trust-subsystem.md) Phases 2, 3, and 4: Redis Working Graph, Trust-Aware Product Behavior, and Advanced Vertex-Style Ranking. Use [trust-policy-boundaries.md](../architecture/trust-policy-boundaries.md) as the guardrail doc for every change here — trust logic stays in `internal/query`/`internal/trust`/`trust_worker`, never in handlers or store methods.

## Relationship to the AI/bot-engagement ranking problem

This plan does **not** fix engagement-farmed content dominating Discovery (see chat discussion 2026-08-10). That's an anti-gaming ranking problem (engagement-quality scoring, near-duplicate suppression, author/domain diversity caps) orthogonal to trust-graph phases. Phase 3's soft trust-boost (§3.2) helps only when spam accounts sit outside the seeded graph; a dense fake-engagement ring that a seed follows will not be caught by any of this work. Track that as a separate workstream.

## Current state (baseline)

| Phase | Status |
| --- | --- |
| Phase 1 (Postgres-native foundation) | Done |
| Phase 2 (Redis working graph) | Partial — sync/compute done, opt-in; seeded neighborhoods and personalized/walk state not started |
| Phase 3 (trust-aware product behavior) | Partial — discovery/search/fallback/ingest-gate/retention policy done; profile trust UX and score-weighted ranking not done |
| Phase 4 (advanced Vertex-style ranking) | Not started |

## Guiding constraints (from trust-policy-boundaries.md)

- All new policy knobs live in `internal/config`, orchestration in `internal/query` / `internal/trust`, data access only in `internal/store`.
- No new canonical-ingest-durability coupling. Open bootstrap kinds (`0`, `3`, `10002`) stay open.
- Every new table/derivation is rebuildable, versioned, and promoteable — no Redis-only product dependency.
- New behavior defaults to a no-op (flag off / weight 0) so rollout is additive, not a breaking change to current rankings.
- No frontend lives in this repo. Everything here is API/data-layer; UI consumption (profile badges, Discovery boost) happens in the separate frontend and is out of scope, but response shapes are specified so that integration is unambiguous.

---

## Workstream A — Phase 2: Redis Working Graph

### A1. `trust_pubkeys_latest` (deferred item, now built) — **done**

Denormalizes `trust_graph_snapshot` (hop/seed) + `trust_scores_global` (score/rank) into one row per pubkey. This removes the `LEFT JOIN` pattern currently duplicated in `trust_state_queries.go`, `trusted_discovery_projection_tx.go`, and `projection_trusted_discovery_candidates.go`, and is the read path both A2 and Workstream B need.

- New migration `migrations/000073_trust_pubkeys_latest.sql`: table `trust_pubkeys_latest(pubkey PK, min_hops, is_seed, score, rank, source_run_id, computed_at, updated_at)`.
- Populated in the existing promote transaction (`internal/trust/runtime_promote.go`) right after `trust_scores_global` is swapped, and refreshed by `RefreshTrustGraphSnapshot` for hop/seed-only changes between score runs.
- `internal/store/trust/trust.go`: add `GetTrustPubkeyLatest`/batch variant backed by this table; keep `GetTrustScore` as-is for API compatibility.
- Register as a new derivation (`DerivationTrustPubkeysLatest`, version 1) in `derivation_registry.go` and `derivation.go`.
- Update `internal/store/migrations_test.go` table list and `internal/api/admin_storage.go` (`StorageTierDerived`).

### A2. Seeded trust neighborhoods

- New migration `migrations/000074_trust_neighborhood_members.sql`: table `trust_neighborhood_members(seed_pubkey, member_pubkey, hops, weight, source_run_id, computed_at)`, PK `(seed_pubkey, member_pubkey)`, index on `member_pubkey`.
- New phase in `internal/trust/runtime_compute.go`: `executeNeighborhoodsRun`, running BFS-with-weight per active seed (bounded by `TRUST_NEIGHBORHOOD_MAX_MEMBERS`, default 5000) over the same adjacency already loaded for global rank — no second graph load.
- Redis: extend `redisKeyspace` in `internal/trust/redis_graph.go` with `runNeighborhoodKey(runID, snapshotRef, seedPubkey)`; write neighborhood sets under the same run-scoped TTL discipline already used for adjacency (every key gets its TTL in the same pipeline batch it's created in).
- New job type `trust_compute_neighborhoods` in `internal/jobs/types.go` (auto-routes to `WorkerPoolTrust` via the existing `trust_` prefix match) inserted into the run lifecycle: `trust_sync_graph_redis → trust_compute_global_scores → trust_compute_neighborhoods → trust_promote_run`.
- Config (`internal/config/trust_worker.go` + `doc_env_trust_worker.go`): `TRUST_ENABLE_NEIGHBORHOODS` (default `false`), `TRUST_NEIGHBORHOOD_MAX_MEMBERS` (default `5000`), validated the same way as existing `TRUST_MAX_HOPS`.
- Metrics (`internal/metrics/trust.go`): `nostrmash_trust_neighborhood_members_total{seed}` gauge (cardinality-bounded — seed count is operator-configured, not user-scale), reuse `trustPhaseDuration` with `phase="neighborhoods"`.

### A3. Redis reap/versioning reuse

No new mechanism needed — A2's keys are additional per-run keys under the existing `runScanPattern`/`unlinkMatching` reaper and the existing `trust_runs.redis_snapshot_ref` correlation. This is the payoff of building A1/A2 on top of the current run model instead of a parallel one.

**Explicitly still deferred (not in this plan):** `trust_relay_suggestions`-style materialization for personalized/neighborhood-scoped relay recommendations — no product need identified yet.

---

## Workstream B — Phase 3 completion: trust-aware product behavior

### B1. Profile trust summary (API only — no raw score) — **done**

Per the earlier discussion: expose hop distance / tier, not the raw float.

- `internal/query/models_trust.go`: add
  ```go
  type TrustSummary struct {
      Tier         string  // "seed" | "in_network" | "unranked"
      HopDistance  *int
      Percentile   *float64 // top-N% by global rank, computed from rank/total, omitted when unranked
  }
  ```
- `internal/query`: `trustTierFromState` already exists in `trust_qualification_policy.go` (hop-bucketing helper) — extend it into a small `TrustSummaryFromState(state, totalRanked int64)` mapper in `mappers_trust.go`. No raw `Score` field is serialized.
- Wire into profile assembly (`internal/query` profile service) as an optional field, sourced from `GetTrustState` (already a capability) + a new cheap `CountRankedPubkeys` store method for percentile denominator (cached in-process, refreshed on trust promote — avoid a `COUNT(*)` per profile request).
- `internal/api/profile_identity.go`: add `trust_summary` to the profile JSON payload, guarded by capability presence (unsupported-capability degrades to omission, matching the existing pattern in `discovery_trust.go`).
- Exact score/rank stays available only via the existing `/api/v1/trust/scores/{pubkey}` and admin endpoints — unchanged.

### B2. Discovery soft ranking boost (notes & profiles)

Currently `trustedNoteRowsByMode`/`trustedProfileRowsByMode` in `internal/query/discovery_trust.go` only bucket trusted-first vs. untrusted in `prefer_trusted` mode; within each bucket, original engagement order is preserved. Add an optional secondary blend:

- Extend `trustedNoteCandidate` / `trustedProfileCandidate` with the already-available `Score`/`Rank` from `TrustQualification` (currently discarded — only the `Trusted` bool survives).
- New config knob `TRUST_DISCOVERY_SCORE_BOOST_WEIGHT` (float, default `0.0` — no behavior change until explicitly set). When `> 0`, blend: `finalOrder = engagementRank + boostWeight * normalizedTrustRank`, applied only inside the `prefer_trusted` bucket-split, never crossing into `trusted_only` filtering semantics.
- Same knob reused for hashtags/links: `getTrendingHashtagsTrustAware` already derives hashtags from trust-qualified notes; extend `hashtagAgg` with a trust-weighted secondary sort key (sum of author trust score, not just `eventCount`/`uniqueAuthors`).
- Ship default `0.0` so this is inert until an operator opts in — matches the "additive, not breaking" constraint.

### B3. Config & docs

- `doc_env_shared.go`: register `TRUST_DISCOVERY_SCORE_BOOST_WEIGHT` (`configdoc-check` will fail CI otherwise).
- Update `trust-policy-boundaries.md` operator-tradeoffs table with the new knob.
- Update `trust-subsystem.md` Phase 3 status.

---

## Workstream C — Phase 4: personalized/advanced ranking

This is the highest-risk, most novel workstream. Ship it shadow-first.

### C1. Generalize the ranking core for personalization

- Refactor `internal/trust/ranking.go`: `computeIterativeGlobalRank` currently hardcodes a uniform teleport vector. Extract the core iteration into `computePersonalizedRank(adjacency, nodeSet, teleport map[string]float64, damping float64)`; `computeIterativeGlobalRank` becomes a thin wrapper passing a uniform teleport vector (`1/n` for every node) — **zero behavior change** for the existing global run, verified by keeping `ranking_test.go`/`ranking_benchmark_test.go` green unmodified.
- Personalized calls concentrate teleport mass on a caller-supplied seed set (e.g. `{viewerPubkey: 1.0}` or a viewer's follow list) instead of the uniform vector.

### C2. Viewer-scoped personalized trust (on-demand, cached)

- Reuse the existing unauthenticated `viewer_pubkey`/`user_pubkey` client-supplied convention already used for moderation lists (`internal/api_primal/primal_moderation.go`) — no new auth mechanism needed, no session state.
- New query capability `GetPersonalizedTrustRanking(ctx, viewerPubkey string, limit int)`, computed by loading the same Postgres/Redis adjacency already used for global rank (bounded reuse, not a new crawl), running `computePersonalizedRank` seeded on the viewer's direct follows (from `contact_lists_latest`), and caching the result in Redis keyed by `(viewerPubkey, active trust run id)` with a TTL (e.g. 1h) so repeat requests in the same run don't recompute.
- Guardrail: only compute for viewers with a bounded follow-list size (e.g. ≤ 2000, configurable `TRUST_PERSONALIZED_MAX_SEED_FOLLOWS`); otherwise fall back to global rank. This bounds worst-case compute cost per request — the same "bounded, operator-safe" principle applied to fallback fetch elsewhere in the codebase.
- Not wired into any default product surface in this plan — it's an opt-in capability a caller (or the separate frontend, later) can call. Wiring it into Discovery-as-viewed-by-you is a follow-up product decision, not part of this plan.

### C3. Richer interaction graph (optional additional signal)

- New derivation aggregating existing engagement tables (`reaction_events`, `repost_events`, `zap_receipts`, thread reply edges) into weighted directed edges: table `trust_interaction_edge_weights(src_pubkey, dst_pubkey, weight, updated_at)` — aggregated weights, not raw event duplication, incrementally maintained the same way `follower_edges` is.
- New migration `migrations/000075_trust_interaction_edge_weights.sql`.
- Config `TRUST_ENABLE_INTERACTION_GRAPH` (default `false`). When enabled, `loadAdjacencyFromPostgres`/`loadAdjacencyFromRedis` optionally merge weighted interaction edges into the follow-graph adjacency for ranking, behind the flag — global rank without the flag is byte-for-byte unchanged.
- **Before enabling by default anywhere:** ship an admin-only comparison report (rank correlation / top-N overlap between follow-only and follow+interaction rank) so an operator can evaluate the change before it affects any product surface. Do not flip the default in this plan; that's a follow-up decision once data is in hand.

---

## Sequencing (PR breakdown)

1. **PR1** — A1 `trust_pubkeys_latest` (migration, store, derivation registry, tests). No behavior change, pure data-layer prep.
2. **PR2** — B1 profile trust summary API (depends on PR1 for the percentile denominator).
3. **PR3** — B2/B3 discovery score-boost knob (independent of PR1/2; can ship in parallel).
4. **PR4** — A2 seeded neighborhoods (migration, redis, job, config, metrics).
5. **PR5** — C1 ranking core generalization (refactor only, protected by existing tests staying green).
6. **PR6** — C2 personalized trust capability (depends on PR5).
7. **PR7** — C3 interaction graph + comparison report (largest, ships disabled).

Each PR is independently mergeable and each defaults to inert (flag off / weight 0), so `main` never regresses ranking behavior mid-rollout.

---

## Testing strategy

- Unit tests colocated per file (existing repo convention): `ranking_test.go` pattern for C1's generalized function: assert `computePersonalizedRank` with a uniform teleport vector reproduces `computeIterativeGlobalRank` output exactly.
- Integration tests (Postgres+Redis, mirroring `runtime_integration_test.go`) for A2's neighborhood run and C3's interaction-edge derivation.
- Benchmark coverage: `internal/trust` is already in `BENCH_HOT_PKGS` in the Makefile — add `ranking_benchmark_test.go` cases for the personalized path so `make benchmark-hot` catches regressions.
- Property test extension: `trust_scheduling_property_test.go` pattern applies to any new weighted-ordering logic (B2's blended sort) to catch ordering-stability edge cases.

## CI checklist (must pass, per `make ci`)

| Check | What this plan must satisfy |
| --- | --- |
| `fmt-check` / `imports-check` | Standard gofmt/goimports on all new files |
| `lint-ci` | golangci-lint clean |
| `mod-verify` | No stray `go.mod` changes unless a dependency is actually added (none planned) |
| `vulncheck` | No new vulnerable deps |
| `test-race-policy` | All new tests pass under `-race`, incl. the new Redis-pipeline code in A2/C2 |
| `cover` + `coverage-policy` | New packages/functions need tests — `scripts/coverage_check.sh` enforces this per package |
| `contract-drift` | No new HTTP routes in this plan (B1 extends an existing payload; C2 is a query-layer capability with no route wired yet) — if a route is added later, register it in the OpenAPI contract |
| `rules-check` | Only relevant if new Prometheus alert rules are added for the new metrics — not required, but recommended for A2/C3 backlog metrics |
| `configdoc-check` | Every new env var (`TRUST_ENABLE_NEIGHBORHOODS`, `TRUST_NEIGHBORHOOD_MAX_MEMBERS`, `TRUST_DISCOVERY_SCORE_BOOST_WEIGHT`, `TRUST_PERSONALIZED_MAX_SEED_FOLLOWS`, `TRUST_ENABLE_INTERACTION_GRAPH`) must be registered in the relevant `doc_env_*.go` file |
| `sqlc-check` | Not touched — this plan doesn't modify `internal/store/account` (the sqlc-managed package) |
| `build` | All binaries (`api`, `worker`, `ingestor`, `trust_worker`) must still build |

Repo-specific gotchas easy to miss:
- `internal/store/migrations_test.go` hardcodes the expected table list — every new table (A1, A2, C3) must be added there or CI fails.
- `internal/api/admin_storage.go`'s `trackedStorageTableTiers` should classify new tables explicitly (defaults to `derived`, which is correct here, but list them for clarity).
- `derivation_registry.go` requires the new derivation entries or `EnsureRegisteredDerivations` won't seed `derivation_versions`/`derivation_active_versions` for them.

## Rollout

- Every new flag ships **default off** (`false`/`0.0`). Nothing in this plan changes production ranking or trust-worker behavior on deploy.
- Enablement order for an operator who wants all of it: A1/A2 (data prep, safe) → B2/B3 boost weight at a small nonzero value, watch discovery outcome metrics → C2 personalized capability (opt-in, no default surface) → C3 only after reviewing the comparison report.
- Update `trust-subsystem.md` "Suggested rollout phases" section to mark Phase 2/3 items done as they ship, and add Phase 4 status once C1–C3 land.
- Update `docs/coolify.md` / `docs/operations.md` with the new env vars in the trust-worker section.

## Risks

| Risk | Mitigation |
| --- | --- |
| Seeded neighborhoods scale poorly for high-follow seeds | `TRUST_NEIGHBORHOOD_MAX_MEMBERS` cap; reuses already-loaded adjacency, no extra graph scan |
| Personalized rank compute cost per request | Bounded seed-follow-count cutoff + Redis cache keyed by run id |
| Interaction graph changes rank in surprising ways | Ships disabled; comparison report required before any default flip |
| Percentile computation drifts from actual rank count | Recompute `CountRankedPubkeys` on trust promote, not per-request |
| New tables silently missing from storage/retention tooling | Explicit checklist item above; add to `admin_storage.go` and retention docs in the same PR that adds the migration |
