package meili

import "encoding/json"

type NoteDocument struct {
	ID        string `json:"id"`
	Content   string `json:"content"`
	Pubkey    string `json:"pubkey"`
	CreatedAt int64  `json:"created_at"`
	Language  string `json:"language,omitempty"`
}

type ProfileDocument struct {
	Pubkey            string          `json:"pubkey"`
	MetadataEventID   string          `json:"metadata_event_id"`
	MetadataCreatedAt int64           `json:"metadata_created_at"`
	Name              string          `json:"name,omitempty"`
	DisplayName       string          `json:"display_name,omitempty"`
	About             string          `json:"about,omitempty"`
	NIP05             string          `json:"nip05,omitempty"`
	ProfileJSON       json.RawMessage `json:"profile_json"`
	Popularity        float64         `json:"popularity"`
}

type SearchDocument struct {
	ID             string   `json:"id"`
	EntityType     string   `json:"entity_type"`
	EntityID       string   `json:"entity_id"`
	Title          string   `json:"title"`
	Body           string   `json:"body"`
	Aliases        []string `json:"aliases"`
	IdentityTokens []string `json:"identity_tokens"`
	Freshness      int64    `json:"freshness"`
	Popularity     float64  `json:"popularity"`
	TrustScore     *float64 `json:"trust_score,omitempty"`
}
