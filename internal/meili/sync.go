package meili

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strconv"

	"github.com/xdzczk/nostrmash/internal/languagelabel"
	"github.com/xdzczk/nostrmash/internal/nostr"
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
// index. Content is truncated in SQL (Go trim further to rune limits). The
// title/summary and d-tag lookups run only for non-kind-1 and kind-30311
// rows, so kind-1 FullSync pages stay inside production statement_timeout
// guardrails. Search hydrates full events by id (or event_id for live
// activities).
var noteDocumentSelect = `
	SELECT
		e.id,
		left(
			CASE
				WHEN e.kind = 1 THEN coalesce(e.content, '')
				ELSE concat_ws(
					E'\n',
					coalesce(e.content, ''),
					(
						SELECT string_agg(et.value, ' ' ORDER BY et.tag_index, et.value_index)
						FROM event_tags et
						WHERE et.event_id = e.id
						  AND et.tag_name IN ('title', 'summary')
					)
				)
			END,
			2000
		),
		e.pubkey,
		e.created_at,
		` + languagelabel.ChartSQL("nds.primary_language", "nds.language_source") + `,
		e.kind,
		CASE
			WHEN e.kind = 30311 THEN coalesce((
				SELECT et.value
				FROM event_tags et
				WHERE et.event_id = e.id
				  AND et.tag_name = 'd'
				  AND et.value_index = 0
				ORDER BY et.tag_index
				LIMIT 1
			), '')
			ELSE ''
		END
	FROM events e
	LEFT JOIN note_discovery_stats nds ON nds.event_id = e.id
`

// indexedNotesWhere is the notes-index corpus. Kind 30311 is only the
// current replaceable winner, so each live activity is one document.
const indexedNotesWhere = `
	e.kind IN (` + nostr.IndexedNoteKindsSQL + `)
	AND (
		e.kind <> 30311
		OR EXISTS (
			SELECT 1 FROM replaceable_state rs
			WHERE rs.event_id = e.id AND rs.kind = 30311
		)
	)`

// namedProfileSQL keeps the profiles index to rows that have a name or
// display name. Empty profiles are not searchable and were the bulk of
// the 1.37M-document profiles index.
const namedProfileSQL = `(btrim(coalesce(p.name, '')) <> '' OR btrim(coalesce(p.display_name, '')) <> '')`

// indexedHashtagSQL drops one-off hashtags. popularity is the event count
// maintained by refresh_search_document_hashtag.
func indexedHashtagSQL() string {
	return "entity_type <> 'hashtag' OR popularity >= " + strconv.Itoa(minIndexedHashtagPopularity)
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanNoteDocument(sc rowScanner) (NoteDocument, string, error) {
	var (
		row  NoteDocument
		kind int
		dTag string
	)
	if err := sc.Scan(&row.ID, &row.Content, &row.Pubkey, &row.CreatedAt, &row.Language, &kind, &dTag); err != nil {
		return NoteDocument{}, "", err
	}
	eventID := assignLiveActivityDocumentID(&row, kind, dTag)
	return row, eventID, nil
}

func scanProfileDocument(sc rowScanner) (ProfileDocument, error) {
	var (
		row        ProfileDocument
		popularity int64
	)
	if err := sc.Scan(
		&row.Pubkey,
		&row.MetadataEventID,
		&row.MetadataCreatedAt,
		&row.Name,
		&row.DisplayName,
		&row.About,
		&row.NIP05,
		&popularity,
	); err != nil {
		return ProfileDocument{}, err
	}
	row.Popularity = float64(popularity)
	return row, nil
}
