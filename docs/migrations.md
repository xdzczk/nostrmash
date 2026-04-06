# Migration Safety Guide

This guide defines practical safety expectations for schema evolution in NostrMash.

NostrMash runs embedded migrations on service startup. That is convenient, but it also means unsafe migrations can break boot and recovery paths quickly. Treat migration design as production-impacting work.

Use this guide with:

- `../RELEASE.md` for release/rollback notes
- `operations.md` for runtime validation and incident triage
- `../VERSIONING.md` and `compatibility.md` when schema changes alter external behavior

## Safety Rules

- **Append-only migration history**: never edit or reorder old migration files once applied in any environment.
- **Prefer additive changes first**: add new tables/columns/indexes before removing old structures.
- **Avoid long blocking operations in one step**: split risky backfills or large index/table changes into staged releases.
- **Keep rollout reversible at release level**: if full DB rollback is not realistic, make application rollout/rollback safe by avoiding immediate destructive schema changes.
- **Document operational impact** in release notes when migrations can change boot time, lock behavior, storage growth, or rebuild/job throughput.

## Rollback-Aware Practices

Schema rollback is often harder than binary rollback. Plan for this explicitly:

1. **Expand phase**: introduce schema additions that old and new binaries can both tolerate.
2. **Switch phase**: ship binaries that read/write the new shape while preserving compatibility where possible.
3. **Contract phase**: only remove legacy columns/indexes/tables in a later release after operational confirmation.

For changes that cannot be expanded/contracted cleanly (for example, major table rewrites), use staged rollout and explicit operator communication before cutover.

## Changes Requiring Extra Scrutiny

- large index builds or table rewrites on hot tables
- changes to queue/job state tables
- changes affecting checkpoint durability or replay/rebuild surfaces
- irreversible data transformations
- anything that can materially increase startup migration time

For these changes, require:

- staging-like rehearsal with representative data volume when feasible
- explicit rollback notes in `RELEASE.md` output
- post-release monitoring focus via `docs/operations.md`

## Operational Validation Checklist

After deploying schema changes:

- verify all services boot cleanly (`/health`, `/ready`)
- inspect queue/job and checkpoint behavior (`GET /admin/v1/jobs`, `GET /admin/v1/relays`)
- inspect derivation/rebuild state (`GET /admin/v1/derivation-versions`, `GET /admin/v1/rebuilds`)
- watch DB/pool saturation and operation error metrics for regressions

## Contributor Checklist For Migration PRs

For migration-related changes, include in PR description:

1. Migration risk class (low/medium/high) and why.
2. Rollout order assumptions (single-step vs staged).
3. Rollback posture (binary-only vs requires operator action).
4. Validation evidence from staging-like environment when change is high risk.

## Scope Limits

- This project does **not** promise instant reversible down-migrations for every change.
- Safety is achieved through phased schema design, staged rollout, and operational readiness rather than aggressive automatic rollback tooling.
