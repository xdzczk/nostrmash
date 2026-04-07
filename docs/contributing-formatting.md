# Contributing: Formatting and docs style

Use this page for two things:

- formatting and import-order checks that CI enforces
- the editorial rules for writing and updating docs in this repository

The goal is simple: code should stay mechanically consistent, and docs should read like one designed system rather than a pile of separate notes.

## Verify (matches CI)

```bash
make fmt-check
make imports-check
```

## Fix formatting and imports

```bash
make format
```

This runs:

- `gofmt -w .`
- `goimports -w .` (via `go run golang.org/x/tools/cmd/goimports@latest`)

## Docs style system

Use these rules when adding or rewriting Markdown docs.

### Page archetypes

Pick one primary archetype per page:

- **Landing doc**: short promise, audience routing, minimal detail, obvious next steps
- **Concept doc**: narrative explanation, one diagram, one decision table, one \"why this matters\" section
- **Runbook**: start with a checklist or triage path, then move into detailed reference
- **Reference doc**: one-sentence orientation, then tightly structured lookup content

### Preferred page rhythm

For most important docs, follow this order:

1. What this page is for
2. When to read it
3. Quick orientation table or short decision block
4. Main content
5. One concrete example, walkthrough, or scenario
6. A short \"go next\" line instead of a long related-doc footer

### Visual language for Markdown-only docs

Prefer:

- compact tables for comparisons, routing, and decision support
- numbered sequences for workflows and walkthroughs
- mermaid diagrams only when they reduce cognitive load
- short lead paragraphs before dense sections
- fenced examples for realistic commands, flows, and scenarios

Avoid:

- long undifferentiated bullet walls
- repetitive \"This page...\" openings on every section
- diagrams that restate obvious text without adding clarity
- long \"Related docs\" lists when one short next-step line is enough

### Tables vs bullets vs diagrams

| Use this | When you are... |
| --- | --- |
| Table | comparing choices, roles, stages, routes, or ownership |
| Bullets | listing a small number of facts or properties |
| Numbered list | walking the reader through a sequence or procedure |
| Mermaid | explaining a flow, boundary, or branching decision that is easier to see than read |

### Writing tone

- Keep the voice calm, direct, and technically confident.
- Prefer short sentences with strong cadence.
- Explain why before diving into exhaustiveness.
- Use examples sparingly but deliberately; every major audience path should have at least one.
- Keep wording precise without sounding procedural or corporate.

### Headings and intros

- Use sentence case for headings.
- Keep intros short: usually 1-3 sentences.
- Do not repeat the same opening formula on every page.
- Start dense sections with a framing line if the reader needs to know how to use what follows.

### Footers and cross-links

- Prefer one short closing line such as \"Use X for deeper detail\".
- Link to the canonical page for a topic instead of duplicating the explanation.
- Do not end pages with large link farms unless the page truly functions as a hub.

### Example blocks

Good examples:

- one operator incident walkthrough
- one contributor workflow from change to validation
- one API surface-selection path
- one deployment verification sequence

Bad examples:

- toy examples that do not match real repo workflows
- examples that merely restate the section without helping the reader decide or act

## Notes

- CI uses `make fmt-check imports-check` and blocks merges if either check fails.
- `goimports` is fetched and run through `go run`; no separate installation step is required.
- Lint checks are also blocking in CI; run `make lint` before opening/updating a PR.
- Full contributor workflow is documented in `CONTRIBUTING.md`; use `make ci` for full local CI parity.
