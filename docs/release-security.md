# Release Security Foundations

This document describes the practical release hardening that NostrMash currently applies for published artifacts.

## What Is Generated

Release workflows generate:

- Linux `amd64` tarball artifacts for `api`, `ingestor`, and `worker`.
- `sha256sums.txt` covering those tarballs.
- An SPDX JSON SBOM: `nostrmash.spdx.json` (generated from the release `dist/` contents).
- Sigstore keyless signatures + certificates for:
  - `sha256sums.txt`
  - `nostrmash.spdx.json`
- GitHub Artifact Attestations (SLSA-style provenance foundation) for release subjects.

Release workflow trigger notes:

- Tag pushes matching `v*` produce published release assets and GHCR image tags.
- Manual `workflow_dispatch` runs still build/sign/attest and upload workflow artifacts, but do not publish GitHub Release assets or push GHCR tags unless the run is on a `v*` tag ref.

## What Is Signed / Attested

- **Signed**: checksum manifest and SBOM (`cosign sign-blob`, keyless OIDC identity from GitHub Actions).
- **Attested**: release artifacts/checksum/SBOM using `actions/attest-build-provenance`.

This gives consumers integrity metadata (checksums + signature), package inventory (SBOM), and build provenance foundations (artifact attestations).

## How To Verify Artifacts

### 1) Verify checksums

Download release assets and run:

```bash
sha256sum --check sha256sums.txt
```

### 2) Verify checksum signature (Sigstore keyless)

Use `cosign` and the release assets `sha256sums.txt`, `sha256sums.txt.sig`, and `sha256sums.txt.pem`:

```bash
cosign verify-blob \
  --certificate sha256sums.txt.pem \
  --signature sha256sums.txt.sig \
  --certificate-identity-regexp "https://github.com/.+/.+/.github/workflows/release.yml@refs/tags/.+" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
  sha256sums.txt
```

### 3) Verify SBOM signature

```bash
cosign verify-blob \
  --certificate nostrmash.spdx.json.pem \
  --signature nostrmash.spdx.json.sig \
  --certificate-identity-regexp "https://github.com/.+/.+/.github/workflows/release.yml@refs/tags/.+" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
  nostrmash.spdx.json
```

### 4) Verify provenance attestation (optional but recommended)

Use GitHub's artifact attestation verification flow (`gh` CLI) against downloaded release artifacts.

Example (verify a downloaded tarball):

```bash
gh attestation verify ./api_v1.2.3_linux_amd64.tar.gz --owner <github-org-or-user>
```

You can run the same command for other downloaded release assets (for example checksum manifest or SBOM).

## Scope And Current Limits

- This is a foundation: useful integrity and provenance metadata without adding heavy compliance overhead.
- Signing is currently focused on checksum/SBOM rather than every individual tarball blob.
- The workflow targets Linux `amd64` release archives at this stage; additional platform matrices can be added as release volume and operator demand justify it.
