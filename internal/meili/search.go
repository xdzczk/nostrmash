package meili

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	ms "github.com/meilisearch/meilisearch-go"
	"github.com/xdzczk/nostrmash/internal/query"
)

type eventHydrator interface {
	GetEventRawsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error)
}

type Searcher struct {
	client     *Client
	events     eventHydrator
	enabled    bool
	mu         sync.Mutex
	highlights map[string]any
}

func NewSearcher(client *Client, events eventHydrator) *Searcher {
	if client == nil || !client.Enabled() {
		return &Searcher{}
	}
	return &Searcher{
		client:     client,
		events:     events,
		enabled:    true,
		highlights: make(map[string]any),
	}
}

func (s *Searcher) Enabled() bool {
	return s != nil && s.enabled && s.client != nil && s.client.Enabled()
}

func (s *Searcher) SearchNotes(
	ctx context.Context,
	searchQuery string,
	sort string,
	window *time.Duration,
	language string,
	limit int,
	offset int,
) ([]json.RawMessage, error) {
	if !s.Enabled() {
		return nil, fmt.Errorf("meilisearch searcher is disabled")
	}
	req := &ms.SearchRequest{
		Limit:                 int64(limit),
		Offset:                int64(offset),
		AttributesToRetrieve:  []string{"id"},
		AttributesToHighlight: []string{"content"},
	}
	filters := make([]string, 0, 2)
	if window != nil && *window > 0 {
		minCreatedAt := time.Now().UTC().Add(-*window).Unix()
		filters = append(filters, fmt.Sprintf("created_at >= %d", minCreatedAt))
	}
	language = strings.TrimSpace(strings.ToLower(language))
	if language != "" {
		filters = append(filters, fmt.Sprintf("language = \"%s\"", language))
	}
	if len(filters) == 1 {
		req.Filter = filters[0]
	}
	if len(filters) > 1 {
		req.Filter = filters
	}
	if strings.EqualFold(strings.TrimSpace(sort), "latest") {
		req.Sort = []string{"created_at:desc"}
	}
	resp, err := s.client.service.Index(IndexNotes).SearchWithContext(ctx, searchQuery, req)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(resp.Hits))
	localHighlights := make(map[string]any)
	for _, hit := range resp.Hits {
		if id := hitString(hit, "id"); id != "" {
			ids = append(ids, id)
			if formatted := hitFormatted(hit); len(formatted) > 0 {
				localHighlights[id] = formatted
			}
		}
	}
	s.recordHighlights(localHighlights)
	if len(ids) == 0 {
		return []json.RawMessage{}, nil
	}
	if s.events == nil {
		return nil, fmt.Errorf("event hydrator is not configured")
	}
	raws, err := s.events.GetEventRawsByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	out := make([]json.RawMessage, 0, len(ids))
	for _, id := range ids {
		if raw, ok := raws[id]; ok {
			out = append(out, raw)
		}
	}
	return out, nil
}

func (s *Searcher) SearchProfiles(
	ctx context.Context,
	searchQuery string,
	_ string,
	limit int,
	offset int,
) ([]query.Profile, error) {
	if !s.Enabled() {
		return nil, fmt.Errorf("meilisearch searcher is disabled")
	}
	resp, err := s.client.service.Index(IndexProfiles).SearchWithContext(ctx, searchQuery, &ms.SearchRequest{
		Limit:                int64(limit),
		Offset:               int64(offset),
		AttributesToRetrieve: []string{"pubkey", "metadata_event_id", "metadata_created_at", "profile_json"},
	})
	if err != nil {
		return nil, err
	}
	out := make([]query.Profile, 0, len(resp.Hits))
	localHighlights := make(map[string]any)
	for _, hit := range resp.Hits {
		pubkey := hitString(hit, "pubkey")
		if pubkey == "" {
			continue
		}
		if formatted := hitFormatted(hit); len(formatted) > 0 {
			localHighlights[pubkey] = formatted
		}
		out = append(out, query.Profile{
			Pubkey:            pubkey,
			MetadataEventID:   hitString(hit, "metadata_event_id"),
			MetadataCreatedAt: hitInt64(hit, "metadata_created_at"),
			ProfileJSON:       hitJSON(hit, "profile_json"),
		})
	}
	s.recordHighlights(localHighlights)
	return out, nil
}

func (s *Searcher) SuggestProfiles(ctx context.Context, searchQuery string, limit int) ([]query.Profile, error) {
	if !s.Enabled() {
		return nil, fmt.Errorf("meilisearch searcher is disabled")
	}
	resp, err := s.client.service.Index(IndexProfiles).SearchWithContext(ctx, searchQuery, &ms.SearchRequest{
		Limit:                   int64(limit),
		Offset:                  0,
		AttributesToRetrieve:    []string{"pubkey", "metadata_event_id", "metadata_created_at", "profile_json"},
		MatchingStrategy:        ms.Last,
		AttributesToSearchOn:    []string{"pubkey", "name", "display_name", "nip05"},
		AttributesToHighlight:   []string{"name", "display_name", "nip05"},
		ShowRankingScore:        true,
		ShowRankingScoreDetails: false,
	})
	if err != nil {
		return nil, err
	}
	out := make([]query.Profile, 0, len(resp.Hits))
	localHighlights := make(map[string]any)
	for _, hit := range resp.Hits {
		pubkey := hitString(hit, "pubkey")
		if pubkey == "" {
			continue
		}
		if formatted := hitFormatted(hit); len(formatted) > 0 {
			localHighlights[pubkey] = formatted
		}
		out = append(out, query.Profile{
			Pubkey:            pubkey,
			MetadataEventID:   hitString(hit, "metadata_event_id"),
			MetadataCreatedAt: hitInt64(hit, "metadata_created_at"),
			ProfileJSON:       hitJSON(hit, "profile_json"),
		})
	}
	s.recordHighlights(localHighlights)
	return out, nil
}

func (s *Searcher) SuggestHashtags(ctx context.Context, searchQuery string, limit int) ([]query.HashtagSuggestion, error) {
	rows, err := s.SearchDocuments(ctx, searchQuery, limit*3)
	if err != nil {
		return nil, err
	}
	out := make([]query.HashtagSuggestion, 0, limit)
	seen := make(map[string]struct{}, limit)
	for _, row := range rows {
		if row.EntityType != "hashtag" {
			continue
		}
		tag := strings.TrimSpace(strings.TrimPrefix(row.EntityID, "#"))
		if tag == "" {
			continue
		}
		if _, ok := seen[tag]; ok {
			continue
		}
		seen[tag] = struct{}{}
		out = append(out, query.HashtagSuggestion{
			Hashtag:    tag,
			EventCount: int64(row.Popularity),
		})
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (s *Searcher) SearchDocuments(ctx context.Context, searchQuery string, limit int) ([]query.SearchDocument, error) {
	if !s.Enabled() {
		return nil, fmt.Errorf("meilisearch searcher is disabled")
	}
	resp, err := s.client.service.Index(IndexDocuments).SearchWithContext(ctx, searchQuery, &ms.SearchRequest{
		Limit:                int64(limit),
		Offset:               0,
		AttributesToRetrieve: []string{"id", "entity_type", "entity_id", "title", "body", "aliases", "identity_tokens", "freshness", "popularity", "trust_score"},
	})
	if err != nil {
		return nil, err
	}
	out := make([]query.SearchDocument, 0, len(resp.Hits))
	for _, hit := range resp.Hits {
		entityType := hitString(hit, "entity_type")
		entityID := hitString(hit, "entity_id")
		if entityType == "" || entityID == "" {
			continue
		}
		row := query.SearchDocument{
			EntityType:     entityType,
			EntityID:       entityID,
			Title:          hitString(hit, "title"),
			Body:           hitString(hit, "body"),
			Aliases:        hitStringList(hit, "aliases"),
			IdentityTokens: hitStringList(hit, "identity_tokens"),
			Freshness:      time.Unix(hitInt64(hit, "freshness"), 0).UTC(),
			Popularity:     hitFloat64(hit, "popularity"),
			TrustScore:     hitFloatPtr(hit, "trust_score"),
		}
		out = append(out, row)
	}
	return out, nil
}

func hitString(hit ms.Hit, key string) string {
	raw, ok := hit[key]
	if !ok || len(raw) == 0 {
		return ""
	}
	var value string
	if err := json.Unmarshal(raw, &value); err == nil {
		return strings.TrimSpace(value)
	}
	return ""
}

func hitInt64(hit ms.Hit, key string) int64 {
	raw, ok := hit[key]
	if !ok || len(raw) == 0 {
		return 0
	}
	var v int64
	if err := json.Unmarshal(raw, &v); err == nil {
		return v
	}
	var vf float64
	if err := json.Unmarshal(raw, &vf); err == nil {
		return int64(vf)
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		if parsed, parseErr := strconv.ParseInt(strings.TrimSpace(s), 10, 64); parseErr == nil {
			return parsed
		}
	}
	return 0
}

func hitFloat64(hit ms.Hit, key string) float64 {
	raw, ok := hit[key]
	if !ok || len(raw) == 0 {
		return 0
	}
	var v float64
	if err := json.Unmarshal(raw, &v); err == nil {
		return v
	}
	var vi int64
	if err := json.Unmarshal(raw, &vi); err == nil {
		return float64(vi)
	}
	return 0
}

func hitFloatPtr(hit ms.Hit, key string) *float64 {
	raw, ok := hit[key]
	if !ok || len(raw) == 0 {
		return nil
	}
	if string(raw) == "null" {
		return nil
	}
	v := hitFloat64(hit, key)
	return &v
}

func hitStringList(hit ms.Hit, key string) []string {
	raw, ok := hit[key]
	if !ok || len(raw) == 0 {
		return nil
	}
	var values []string
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil
	}
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func hitJSON(hit ms.Hit, key string) json.RawMessage {
	raw, ok := hit[key]
	if !ok || len(raw) == 0 {
		return json.RawMessage(`{}`)
	}
	var payload json.RawMessage
	if err := json.Unmarshal(raw, &payload); err != nil || len(payload) == 0 {
		return json.RawMessage(`{}`)
	}
	return payload
}

func hitFormatted(hit ms.Hit) map[string]string {
	raw, ok := hit["_formatted"]
	if !ok || len(raw) == 0 {
		return nil
	}
	var payload map[string]string
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil
	}
	return payload
}

func (s *Searcher) recordHighlights(rows map[string]any) {
	if len(rows) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.highlights == nil {
		s.highlights = make(map[string]any, len(rows))
	}
	for key, value := range rows {
		s.highlights[key] = value
	}
}

func (s *Searcher) ConsumeHighlights() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.highlights) == 0 {
		return nil
	}
	out := make(map[string]any, len(s.highlights))
	for key, value := range s.highlights {
		out[key] = value
	}
	s.highlights = make(map[string]any)
	return out
}
