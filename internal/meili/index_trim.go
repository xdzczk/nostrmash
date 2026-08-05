package meili

import (
	"strings"
	"time"
	"unicode/utf8"
)

const (
	// Indexed note bodies are searchable prefixes only; search hydrates the
	// full event from Postgres by id. Keeping the full note text in Meili is
	// the dominant driver of the 40GB+ on-disk index on a single-server host.
	indexedNoteContentMaxRunes = 480
	indexedAboutMaxRunes       = 280
	indexedBodyMaxRunes        = 280

	// Notes older than this are omitted from full sync / age-bounded streams.
	// Incremental SyncEvent of a specific id may still upsert recent activity
	// targets, but FullSync stops once the keyset cursor crosses this age.
	// 30d keeps the rebuild tractable on a single-server host (~hundreds of
	// thousands of notes rather than multi-million).
	indexedNotesMaxAge = 30 * 24 * time.Hour
)

func truncateIndexedText(s string, maxRunes int) string {
	s = strings.TrimSpace(s)
	if maxRunes <= 0 || s == "" {
		return s
	}
	if utf8.RuneCountInString(s) <= maxRunes {
		return s
	}
	runes := []rune(s)
	return string(runes[:maxRunes])
}

func trimNoteDocument(row *NoteDocument) {
	if row == nil {
		return
	}
	row.Content = truncateIndexedText(row.Content, indexedNoteContentMaxRunes)
}

func trimProfileDocument(row *ProfileDocument) {
	if row == nil {
		return
	}
	row.About = truncateIndexedText(row.About, indexedAboutMaxRunes)
	// Full kind:0 JSON is large and redundant with searchable name fields;
	// search reconstructs a slim profile_json for API responses.
	row.ProfileJSON = nil
}

func trimSearchDocument(row *SearchDocument) {
	if row == nil {
		return
	}
	row.Body = truncateIndexedText(row.Body, indexedBodyMaxRunes)
	row.Title = truncateIndexedText(row.Title, indexedNoteContentMaxRunes)
}

func indexedNotesMinCreatedAt(now time.Time) int64 {
	return now.UTC().Add(-indexedNotesMaxAge).Unix()
}
