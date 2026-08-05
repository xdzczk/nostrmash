package meili

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
)

var meiliDocIDValid = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// meiliDocIDMaxLen is Meilisearch's hard cap. Composite IDs longer
// than this are also rejected.
const meiliDocIDMaxLen = 511

// safeMeiliDocID returns a Meilisearch-safe document identifier for an
// (entity_type, entity_id) pair. When entity_id already satisfies
// Meilisearch's constraints the legacy "type_id" form is preserved so
// existing documents in the index stay addressable; otherwise a
// deterministic hash-based id ("type_h<hex>") is substituted so the
// document still gets indexed under a stable key without poisoning the
// upsert task.
func safeMeiliDocID(entityType, entityID string) string {
	composite := entityType + "_" + entityID
	if len(composite) <= meiliDocIDMaxLen && meiliDocIDValid.MatchString(entityID) {
		return composite
	}
	sum := sha256.Sum256([]byte(entityID))
	return entityType + "_h" + hex.EncodeToString(sum[:16])
}

type SyncStats struct {
	Notes     int64 `json:"notes"`
	Profiles  int64 `json:"profiles"`
	Documents int64 `json:"documents"`
}

// noteDocumentSelect is the shared projection feeding the Meilisearch `notes`
// index. Content is truncated in SQL (Go trim further to rune limits) and
// per-row event_tags laterals are avoided so FullSync pages stay inside
// production statement_timeout guardrails. Search hydrates full events by id.
const noteDocumentSelect = `
	SELECT
		e.id,
		left(coalesce(e.content, ''), 2000),
		e.pubkey,
		e.created_at,
		coalesce(nds.primary_language, 'und')
	FROM events e
	LEFT JOIN note_discovery_stats nds ON nds.event_id = e.id
`
