// Package api_primal provides a minimal, isolated Primal compatibility adapter.
//
// Supported route checklist (Task 16 explicit scope):
// - [x] GET /primal/v1/events/{id}
// - [x] POST /primal/v1/events/batch
// - [x] GET /primal/v1/profiles/{pubkey}
//
// Intentionally out of scope:
// - Any route outside the checklist above.
// - Changes to native API contracts for compatibility purposes.
// - Primal-specific fields leaking into core domain models.
package api_primal
