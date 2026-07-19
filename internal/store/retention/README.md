# retention store context — sqlc (second context)

The retention bounded context owns bounded `DELETE`/`UPDATE` sweeps over the
event, relay, search-document, account-state, and trusted-discovery tables. It
is the **second sqlc context** after the account-state pilot, adopted once that
pilot's [verdict](../account/README.md#verdict-jul-2026-positive--extend-to-retention)
came back positive.

Why retention was the right next context: every statement is a static CTE
sweep with **zero dynamic SQL**, so each one is a plain `:execrows` query that
returns `RowsAffected`. The generated layer is pure plumbing; nothing about the
public surface, validation, or transaction shape changed.

## What is generated

- schema: the full [`migrations/`](../../../migrations) directory (retention
  touches many tables; sqlc applies the migrations in lexical order)
- queries: [`queries/retention.sql`](queries/retention.sql)
- config: the second `sql` block in [`sqlc.yaml`](../../../sqlc.yaml) (repo root)

Generated code lives in [`retentiondb/`](retentiondb) and is committed.
Regenerate with `make sqlc`; `make sqlc-check` (wired into `make ci`) diffs
**all** configured contexts, so retention drift fails CI with no extra wiring.

## What is and isn't generated

`*Retention`'s exported methods remain the public surface. Each keeps its
`nil`-store guard, argument validation, `metrics.ObserveDBOperation` timing, and
error wrapping, then delegates the SQL to `*retentiondb.Queries` via the
unexported `queries()` helper. Two small conversions bridge the domain types to
the generated params:

- `tsz(time.Time) pgtype.Timestamptz` — sqlc types bind parameters as nullable,
  so timestamptz params surface as `pgtype.Timestamptz`; the wrappers mark them
  valid.
- `int32Kinds([]int) []int32` — narrows the package-level replaceable-kind list
  to the generated `ANY($1::int[])` parameter type.

Nothing in this context needed a hand-written raw-pgx escape: unlike the
account pilot's `unnest`/cross-context-join statements, every retention sweep is
expressible by the analyzer.
