package meili

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	ms "github.com/meilisearch/meilisearch-go"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/readmodel"
)

// DefaultSearchTimeout is the per-request Meilisearch search deadline.
// On timeout or circuit-open, callers fall back to Postgres.
const DefaultSearchTimeout = 2 * time.Second

type eventHydrator interface {
	GetEventRawsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error)
}

type Searcher struct {
	client     *Client
	events     eventHydrator
	enabled    bool
	timeout    time.Duration
	circuit    *searchCircuit
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
		timeout:    DefaultSearchTimeout,
		circuit:    newSearchCircuit(),
		highlights: make(map[string]any),
	}
}

func (s *Searcher) Enabled() bool {
	return s != nil && s.enabled && s.client != nil && s.client.Enabled()
}

// Available reports whether Meilisearch search is currently usable.
// When false, the query layer should report search_engine=degraded and
// prefer Postgres fallbacks without waiting on Meili.
func (s *Searcher) Available() bool {
	return s.Enabled() && (s.circuit == nil || !s.circuit.open())
}

func (s *Searcher) searchTimeout() time.Duration {
	if s == nil || s.timeout <= 0 {
		return DefaultSearchTimeout
	}
	return s.timeout
}

func (s *Searcher) beginSearch(ctx context.Context, index string) (context.Context, context.CancelFunc, string, bool) {
	if !s.Enabled() {
		return ctx, func() {}, "error", false
	}
	if s.circuit != nil && !s.circuit.allow() {
		metrics.ObserveMeiliSearch(index, "circuit_open", 0)
		return ctx, func() {}, "circuit_open", false
	}
	searchCtx, cancel := context.WithTimeout(ctx, s.searchTimeout())
	return searchCtx, cancel, "success", true
}

func (s *Searcher) finishSearch(outcome *string, err error) {
	if err == nil {
		if s.circuit != nil {
			s.circuit.success()
		}
		return
	}
	if s.circuit != nil {
		s.circuit.failure()
	}
	if errorsIsTimeout(err) {
		*outcome = "timeout"
		return
	}
	*outcome = "error"
}

func errorsIsTimeout(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, context.DeadlineExceeded) || strings.Contains(strings.ToLower(err.Error()), "timeout")
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
	started := time.Now()
	outcome := "success"
	defer func() {
		metrics.ObserveMeiliSearch(IndexNotes, outcome, time.Since(started))
	}()
	searchCtx, cancel, preOutcome, ok := s.beginSearch(ctx, IndexNotes)
	defer cancel()
	if !ok {
		outcome = preOutcome
		if preOutcome == "circuit_open" {
			return nil, fmt.Errorf("meilisearch circuit open")
		}
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
	resp, err := s.client.service.Index(IndexNotes).SearchWithContext(searchCtx, searchQuery, req)
	if err != nil {
		s.finishSearch(&outcome, err)
		return nil, err
	}
	s.finishSearch(&outcome, nil)
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
		outcome = "error"
		return nil, fmt.Errorf("event hydrator is not configured")
	}
	raws, err := s.events.GetEventRawsByIDs(ctx, ids)
	if err != nil {
		outcome = "error"
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
) ([]readmodel.Profile, error) {
	started := time.Now()
	outcome := "success"
	defer func() {
		metrics.ObserveMeiliSearch(IndexProfiles, outcome, time.Since(started))
	}()
	searchCtx, cancel, preOutcome, ok := s.beginSearch(ctx, IndexProfiles)
	defer cancel()
	if !ok {
		outcome = preOutcome
		if preOutcome == "circuit_open" {
			return nil, fmt.Errorf("meilisearch circuit open")
		}
		return nil, fmt.Errorf("meilisearch searcher is disabled")
	}
	resp, err := s.client.service.Index(IndexProfiles).SearchWithContext(searchCtx, searchQuery, &ms.SearchRequest{
		Limit:  int64(limit),
		Offset: int64(offset),
		AttributesToRetrieve: []string{
			"pubkey", "metadata_event_id", "metadata_created_at",
			"name", "display_name", "about", "nip05",
		},
	})
	if err != nil {
		s.finishSearch(&outcome, err)
		return nil, err
	}
	s.finishSearch(&outcome, nil)
	out := make([]readmodel.Profile, 0, len(resp.Hits))
	localHighlights := make(map[string]any)
	for _, hit := range resp.Hits {
		pubkey := hitString(hit, "pubkey")
		if pubkey == "" {
			continue
		}
		if formatted := hitFormatted(hit); len(formatted) > 0 {
			localHighlights[pubkey] = formatted
		}
		out = append(out, profileFromMeiliHit(hit))
	}
	s.recordHighlights(localHighlights)
	return out, nil
}

func (s *Searcher) SuggestProfiles(ctx context.Context, searchQuery string, limit int) ([]readmodel.Profile, error) {
	started := time.Now()
	outcome := "success"
	defer func() {
		metrics.ObserveMeiliSearch(IndexProfiles, outcome, time.Since(started))
	}()
	searchCtx, cancel, preOutcome, ok := s.beginSearch(ctx, IndexProfiles)
	defer cancel()
	if !ok {
		outcome = preOutcome
		if preOutcome == "circuit_open" {
			return nil, fmt.Errorf("meilisearch circuit open")
		}
		return nil, fmt.Errorf("meilisearch searcher is disabled")
	}
	resp, err := s.client.service.Index(IndexProfiles).SearchWithContext(searchCtx, searchQuery, &ms.SearchRequest{
		Limit:  int64(limit),
		Offset: 0,
		AttributesToRetrieve: []string{
			"pubkey", "metadata_event_id", "metadata_created_at",
			"name", "display_name", "about", "nip05",
		},
		MatchingStrategy:        ms.Last,
		AttributesToSearchOn:    []string{"pubkey", "name", "display_name", "nip05", "about"},
		AttributesToHighlight:   []string{"name", "display_name", "nip05"},
		ShowRankingScore:        true,
		ShowRankingScoreDetails: false,
	})
	if err != nil {
		s.finishSearch(&outcome, err)
		return nil, err
	}
	s.finishSearch(&outcome, nil)
	out := make([]readmodel.Profile, 0, len(resp.Hits))
	localHighlights := make(map[string]any)
	for _, hit := range resp.Hits {
		pubkey := hitString(hit, "pubkey")
		if pubkey == "" {
			continue
		}
		if formatted := hitFormatted(hit); len(formatted) > 0 {
			localHighlights[pubkey] = formatted
		}
		out = append(out, profileFromMeiliHit(hit))
	}
	s.recordHighlights(localHighlights)
	return out, nil
}

func (s *Searcher) SuggestHashtags(ctx context.Context, searchQuery string, limit int) ([]readmodel.HashtagSuggestion, error) {
	rows, err := s.SearchDocuments(ctx, searchQuery, limit*3)
	if err != nil {
		return nil, err
	}
	out := make([]readmodel.HashtagSuggestion, 0, limit)
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
		out = append(out, readmodel.HashtagSuggestion{
			Hashtag:    tag,
			EventCount: int64(row.Popularity),
		})
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (s *Searcher) SearchDocuments(ctx context.Context, searchQuery string, limit int) ([]readmodel.SearchDocument, error) {
	started := time.Now()
	outcome := "success"
	defer func() {
		metrics.ObserveMeiliSearch(IndexDocuments, outcome, time.Since(started))
	}()
	searchCtx, cancel, preOutcome, ok := s.beginSearch(ctx, IndexDocuments)
	defer cancel()
	if !ok {
		outcome = preOutcome
		if preOutcome == "circuit_open" {
			return nil, fmt.Errorf("meilisearch circuit open")
		}
		return nil, fmt.Errorf("meilisearch searcher is disabled")
	}
	resp, err := s.client.service.Index(IndexDocuments).SearchWithContext(searchCtx, searchQuery, &ms.SearchRequest{
		Limit:                int64(limit),
		Offset:               0,
		AttributesToRetrieve: []string{"id", "entity_type", "entity_id", "title", "body", "aliases", "identity_tokens", "freshness", "popularity", "trust_score"},
	})
	if err != nil {
		s.finishSearch(&outcome, err)
		return nil, err
	}
	s.finishSearch(&outcome, nil)
	out := make([]readmodel.SearchDocument, 0, len(resp.Hits))
	for _, hit := range resp.Hits {
		entityType := hitString(hit, "entity_type")
		entityID := hitString(hit, "entity_id")
		if entityType == "" || entityID == "" {
			continue
		}
		row := readmodel.SearchDocument{
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

func profileFromMeiliHit(hit ms.Hit) readmodel.Profile {
	name := hitString(hit, "name")
	displayName := hitString(hit, "display_name")
	about := hitString(hit, "about")
	nip05 := hitString(hit, "nip05")
	slim, _ := json.Marshal(map[string]any{
		"name":         name,
		"display_name": displayName,
		"about":        about,
		"nip05":        nip05,
	})
	return readmodel.Profile{
		Pubkey:            hitString(hit, "pubkey"),
		MetadataEventID:   hitString(hit, "metadata_event_id"),
		MetadataCreatedAt: hitInt64(hit, "metadata_created_at"),
		ProfileJSON:       slim,
	}
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
