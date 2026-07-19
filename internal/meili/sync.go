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
// index. Kind 1 text notes index their content directly; kind 30023 long-form
// articles fold their `title` tag into the indexed content so articles are
// findable by title. The index only matches/highlights on `content` and search
// hydrates the raw event by id, so folding the title in here does not change
// the payloads returned to clients. Callers append their own
// WHERE/ORDER/pagination clauses; both note kinds share the same column shape.
const noteDocumentSelect = `
	SELECT
		e.id,
		CASE
			WHEN e.kind = 30023 THEN btrim(coalesce(t.title, '') || E'\n' || coalesce(e.content, ''))
			ELSE coalesce(e.content, '')
		END,
		e.pubkey,
		e.created_at,
		coalesce(nds.primary_language, 'und')
	FROM events e
	LEFT JOIN note_discovery_stats nds ON nds.event_id = e.id
	LEFT JOIN LATERAL (
		SELECT et.value AS title
		FROM event_tags et
		WHERE et.event_id = e.id
		  AND et.tag_name = 'title'
		  AND et.value_index = 0
		ORDER BY et.tag_index ASC
		LIMIT 1
	) t ON true
`
