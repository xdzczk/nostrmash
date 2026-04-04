// Package nostr provides deterministic, side-effect-free parsing and validation
// of Nostr events.
//
// Validation runs in four stages:
//   - Stage 1: structural JSON validation and required top-level fields.
//   - Stage 2: canonical field validation (types, formats, and ID recomputation).
//   - Stage 3: Schnorr signature verification.
//   - Stage 4: content safety checks for suspicious payload characteristics.
//
// The pipeline returns typed error codes/messages suitable for invalid_events
// storage and retains raw JSON only after the payload is considered safe to keep.
package nostr
