// Package readmodel holds transport- and storage-neutral read models shared
// across the query, meili, and store layers. Keeping these DTOs in a leaf
// package lets lower-level adapters (e.g. meili) return domain values without
// importing the higher-level query orchestration layer, and lets query map at
// the store edge without a hard dependency on the concrete store package.
package readmodel

import (
	"encoding/json"
	"errors"
	"time"
)

// ErrNotFound is the storage-neutral sentinel returned when a requested record
// does not exist. store re-exports this value so both layers share one sentinel.
var ErrNotFound = errors.New("not found")

// Profile is a projected profile read model (metadata event plus provenance).
type Profile struct {
	Pubkey            string
	MetadataEventID   string
	MetadataCreatedAt int64
	ProfileJSON       json.RawMessage
}

// HashtagSuggestion is a lightweight hashtag suggestion entry.
type HashtagSuggestion struct {
	Hashtag       string `json:"hashtag"`
	EventCount    int64  `json:"event_count"`
	UniqueAuthors int64  `json:"unique_authors"`
}

// SearchDocument is a neutral representation of a search index document.
type SearchDocument struct {
	EntityType     string
	EntityID       string
	Title          string
	Body           string
	Aliases        []string
	IdentityTokens []string
	Freshness      time.Time
	Popularity     float64
	TrustScore     *float64
	Score          float64
}
