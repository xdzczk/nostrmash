# Hotspot Watchlist

Use this page during review for changes that touch structurally risky areas. This is not a performance map and not an architecture replacement; it is a "how this can rot" checklist tied to current code boundaries.

For latency/throughput evidence, use [performance.md](performance.md). For system layering, use [architecture.md](architecture.md).

## 1) `internal/api_primal` (HTTP + WS compatibility boundary)

- **Ownership/boundary:** Primal shape translation only. Keep product compatibility adaptation in `internal/api_primal`, not in `internal/query` or `internal/store`.
- **Likely failure mode:** endpoint-specific behavior diverges between HTTP handlers and WS request kinds, producing contract drift and inconsistent fallback/error behavior.
- **Treat cautiously when changing:** request parsing/validation, response shaping, WS frame dispatch (`wsFrameHandlers`), and any fan-out that joins query results into transport-specific payloads.
- **Anti-patterns to avoid:** adding direct store calls in handlers, copy-pasting normalization logic across handlers, and introducing one-off behavior that bypasses shared query service methods.
- **Preferred extension pattern:** add/extend query-layer capability first, then adapt once in handlers/gateway; cover compatibility with contract fixtures/tests before broad refactors.
- **Refactor smells:** repeated JSON-shaping branches across multiple handlers, new endpoint behavior that must be patched in both HTTP and WS paths, or handler methods accumulating unrelated concerns (validation + orchestration + transport shaping + fallback policy).

## 2) `internal/query` (read orchestration and capability adaptation)

- **Ownership/boundary:** transport-agnostic read orchestration and feature capability adaptation (`Service`, capability adapters, focused service interfaces).
- **Likely failure mode:** capability creep turns `Service` into an implicit mega-interface with scattered optional behavior and fragile not-supported handling.
- **Treat cautiously when changing:** `Reader`/capability interfaces, fallback wiring, normalization helpers, and method semantics used by both native and compatibility transports.
- **Anti-patterns to avoid:** widening base interfaces for one endpoint, leaking transport shape assumptions into query methods, and re-implementing normalization/ordering in handlers.
- **Preferred extension pattern:** add a narrow capability + adapter + focused service method, preserve sentinel errors (`ErrUnsupportedCapability`, not-found semantics), and keep transport formatting outside query.
- **Refactor smells:** repeated capability guards in many methods, "just pass through" methods with no clear boundary value, or frequent regressions where one transport change unexpectedly breaks another.

## 3) Config/doc generation surface (`internal/config`, `cmd/configdoc`, `docs/configuration.md`)

- **Ownership/boundary:** env var metadata lives in `internal/config`; `cmd/configdoc` materializes canonical docs; `docs/configuration.md` is generated output.
- **Likely failure mode:** runtime config and documentation drift apart, especially when new env vars are added without corresponding doc metadata.
- **Treat cautiously when changing:** env var names/defaults/runtime ownership, generation ordering/formatting, and any "manual edit" workflow around generated docs.
- **Anti-patterns to avoid:** hand-editing `docs/configuration.md`, embedding runtime-specific policy text in generated docs, and centralizing all env metadata into one giant manually maintained list.
- **Preferred extension pattern:** add env docs in the runtime-scoped `doc_env_*.go` area, regenerate via `cmd/configdoc`, and keep descriptions concise and operationally precise.
- **Refactor smells:** duplicated env var definitions across files, generated diff churn unrelated to metadata changes, or contributors routinely forgetting to update generated config docs.

## 4) Trust frontier + store-heavy query surfaces

- **Ownership/boundary:** trust API/admin reads should flow through query trust capabilities (`GetTrustScore`, `ListTopTrustedPubkeys`, `GetTrustRun`, `ListTrustRuns`) with clear optional-capability behavior.
- **Likely failure mode:** trust-specific reads bypass capability boundaries and become tightly coupled to store details, making fallback/unsupported behavior inconsistent and hard to reason about.
- **Treat cautiously when changing:** trust endpoints in route declarations, trust capability adapters, result-limit/default handling, and any store-backed list/read behavior with potentially high cardinality.
- **Anti-patterns to avoid:** treating trust read models as always-present in all runtimes, adding ad hoc store calls from handlers for "just one trust field," and introducing unbounded list reads.
- **Preferred extension pattern:** introduce/extend trust capabilities first, keep query-level defaults explicit, and make unsupported-capability behavior deliberate so transports can map it consistently.
- **Refactor smells:** many trust-specific conditionals spread across handlers, repeated limit/default parsing logic, or recurring bugs where trust routes work in one runtime path but not another.

## 5) Route and contract registration surfaces (`cmd/api/routes.go` + contract tests)

- **Ownership/boundary:** `buildRouteDefinitions` is the authoritative route inventory; `OwnsContract` signals whether this repo owns/guarantees behavior; contract fixtures/tests validate compatibility surfaces.
- **Likely failure mode:** routes are added/changed without contract updates or ownership intent, causing accidental public API drift.
- **Treat cautiously when changing:** route patterns, handler wiring, `OwnsContract` flags, and compatibility payload semantics.
- **Anti-patterns to avoid:** ad hoc mux registrations outside route definitions, route additions without contract/fixture coverage, and marking unstable surfaces as contract-owned "temporarily."
- **Preferred extension pattern:** add routes through route definitions only, set ownership explicitly, and update/extend contract tests for behavior-visible changes.
- **Refactor smells:** duplicate route patterns for the same intent, hard-to-audit contract ownership decisions, or repeated test failures caused by hidden route behavior changes rather than intentional contract updates.

## Review quick checks

- Does this change cross a boundary that should stay in one layer?
- If behavior is user-visible, is there contract coverage in the matching surface?
- If a new option/capability was introduced, is it narrow and explicitly adapted?
- Are we adding repeated special cases instead of extending an existing pattern once?
- Did this increase the amount of "if this transport/runtime" branching in the hotspot?
