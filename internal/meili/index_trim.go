package meili

import (
	"regexp"
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
	// 14d keeps the Meili notes index small enough for the single-server
	// RAM/disk budget (<10 GB total after dropping redundant documents).
	indexedNotesMaxAge = 14 * 24 * time.Hour

	// liveActivityKind is indexed once per NIP-33 address (the current
	// replaceable_state winner), not once per revision.
	liveActivityKind = 30311

	// minIndexedHashtagPopularity skips hashtags that appear on a single
	// event. Those one-off tags dominated the documents index (~723k docs)
	// and each distinct tag is a word-dictionary entry.
	minIndexedHashtagPopularity = 2
)

// Opaque tokens that must not enter the Meilisearch word dictionary.
//
// Meilisearch rebuilds the whole words FST on every indexing batch, so the
// per-batch cost scales with the number of distinct words in the index, not
// with the batch. Production traces showed `post processing words > word fst`
// at ~27s of a ~33s single-document batch on `notes` (and ~10s on
// `profiles`), with Meilisearch pinned at ~300% CPU around the clock. Nostr
// text is dense with one-off tokens that nobody searches for verbatim:
// bech32 references, hex ids, lightning invoices, base64 blobs, URL paths.
var (
	// nostr:npub1..., nevent1..., note1..., nprofile1..., naddr1... with or
	// without the nostr: prefix; also lightning invoices (lnbc...) and
	// lightning addresses are left alone (short, user@domain).
	opaqueBech32Pattern = regexp.MustCompile(`(?i)\b(?:nostr:)?(?:npub|nsec|note|nevent|nprofile|naddr|nrelay|lnbc|lntb|lnbcrt)1[02-9ac-hj-np-z]{20,}\b`)
	// 32+ hex chars: event ids, pubkeys, sha256, etc.
	opaqueHexPattern = regexp.MustCompile(`\b[0-9a-fA-F]{32,}\b`)
	// URLs are reduced to their host so "youtube.com" stays searchable but
	// paths, query strings and fragments do not become dictionary words.
	urlPattern = regexp.MustCompile(`(?i)\b(?:https?|wss?)://([^/\s?#]+)[^\s]*`)
	// Any remaining very long run without whitespace (base64, encoded blobs).
	opaqueLongRunPattern = regexp.MustCompile(`\S{48,}`)
	whitespaceRunPattern = regexp.MustCompile(`\s+`)
)

// stripOpaqueTokens removes tokens that would only bloat the Meilisearch word
// dictionary. It is applied to searchable free text before truncation.
func stripOpaqueTokens(s string) string {
	if s == "" {
		return s
	}
	s = urlPattern.ReplaceAllString(s, " $1 ")
	s = opaqueBech32Pattern.ReplaceAllString(s, " ")
	s = opaqueHexPattern.ReplaceAllString(s, " ")
	s = opaqueLongRunPattern.ReplaceAllString(s, " ")
	return strings.TrimSpace(whitespaceRunPattern.ReplaceAllString(s, " "))
}

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
	row.Content = truncateIndexedText(stripOpaqueTokens(row.Content), indexedNoteContentMaxRunes)
}

func trimProfileDocument(row *ProfileDocument) {
	if row == nil {
		return
	}
	row.About = truncateIndexedText(stripOpaqueTokens(row.About), indexedAboutMaxRunes)
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

// assignLiveActivityDocumentID keys a kind-30311 note by its address so a
// later revision replaces the indexed document. Other kinds keep the event
// id as the Meilisearch primary key. The returned eventID is the original
// row id, for keyset cursors that must follow events.id.
func assignLiveActivityDocumentID(row *NoteDocument, kind int, dTag string) (eventID string) {
	if row == nil {
		return ""
	}
	eventID = row.ID
	if kind != liveActivityKind {
		return eventID
	}
	row.EventID = eventID
	row.ID = safeMeiliDocID("live", strings.ToLower(strings.TrimSpace(row.Pubkey))+":"+dTag)
	return eventID
}

// filterMeiliDocumentsIndexRows drops entity types that are already covered by
// the dedicated notes/profiles indexes, and drops one-off hashtags.
// Keeping notes and profiles in `documents` roughly doubled Meili disk on
// the single-server host without changing API results (splitSearchDocuments
// only returns hashtag/identity/relay).
func filterMeiliDocumentsIndexRows(docs []SearchDocument) []SearchDocument {
	keep, _ := splitMeiliDocuments(docs)
	return keep
}

// splitMeiliDocuments returns the documents to upsert and the Meilisearch
// ids of hashtags below minIndexedHashtagPopularity. Those ids are not
// written. Documents already stored under the cutoff are removed by
// PurgeRareHashtags, not one delete task per sweeper batch.
func splitMeiliDocuments(docs []SearchDocument) (keep []SearchDocument, dropHashtagIDs []string) {
	if len(docs) == 0 {
		return nil, nil
	}
	keep = make([]SearchDocument, 0, len(docs))
	for _, doc := range docs {
		switch strings.TrimSpace(doc.EntityType) {
		case "hashtag":
			if doc.Popularity < minIndexedHashtagPopularity {
				if doc.ID != "" {
					dropHashtagIDs = append(dropHashtagIDs, doc.ID)
				}
				continue
			}
			keep = append(keep, doc)
		case "identity", "relay":
			keep = append(keep, doc)
		}
	}
	return keep, dropHashtagIDs
}
