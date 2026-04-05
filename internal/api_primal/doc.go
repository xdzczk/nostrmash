// Package api_primal provides a minimal, isolated Primal compatibility adapter.
//
// Supported route checklist (expanded compatibility scope):
// - [x] GET /primal/v1/events/{id}
// - [x] POST /primal/v1/events/batch
// - [x] GET /primal/v1/profiles/{pubkey}
// - [x] POST /primal/v1/user_infos
// - [x] GET /primal/v1/threads/{eventId}
// - [x] GET /primal/v1/authors/{pubkey}/events
// - [x] GET /primal/v1/authors/{pubkey}/replies
// - [x] GET /primal/v1/events/{id}/actions
// - [x] GET /primal/v1/contact-lists/{pubkey}
// - [x] GET /primal/v1/relay-lists/{pubkey}
// - [x] GET /primal/ws (REQ/CLOSE gateway)
//
// Handler-level contract coverage for this checklist is maintained by fixture/golden
// tests under internal/api_primal/testdata/primal_contracts.
//
// Remaining deferred scope is tracked in docs/primal_compatibility_matrix.md.
package api_primal
