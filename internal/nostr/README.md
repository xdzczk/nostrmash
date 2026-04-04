# Nostr Event Validation Pipeline

This package validates raw Nostr event JSON in four deterministic stages:

1. **Structural validation**: JSON must be a bounded object with required fields.
2. **Canonical field validation**: field types/formats are checked and `id` is recomputed from canonical serialization.
3. **Signature verification**: Schnorr signature is verified against `id` and `pubkey`.
4. **Content safety checks**: suspicious control bytes and oversized content are rejected.

`ParseAndValidate` returns:

- a validated `Event` when all checks pass;
- a typed `ValidationError` (`Code`, `Message`, `Stage`) suitable for `invalid_events`;
- retained `RawJSON` only after the payload is safe to keep.

Fixture-driven tests live in `testdata/` and cover valid, malformed, invalid, and malicious-like payloads.
