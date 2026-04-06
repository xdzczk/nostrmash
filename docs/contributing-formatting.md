# Contributing: Formatting

Formatting and import ordering are enforced in CI. Use the same local commands before opening or updating a PR.

## Verify (matches CI)

```bash
make fmt-check
make imports-check
```

## Fix formatting/imports

```bash
make format
```

This runs:

- `gofmt -w .`
- `goimports -w .` (via `go run golang.org/x/tools/cmd/goimports@latest`)

## Notes

- CI uses `make fmt-check imports-check` and blocks merges if either check fails.
- `goimports` is fetched and run through `go run`; no separate installation step is required.
- Lint checks are also blocking in CI; run `make lint` before opening/updating a PR.
- Full contributor workflow is documented in `CONTRIBUTING.md`; use `make ci` for full local CI parity.
