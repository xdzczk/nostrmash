package read

import (
	"context"
	"fmt"
	"strings"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

// HashtagDocumentFrequencies returns search_documents popularity for hashtag
// rows. Popularity is the number of events carrying that tag. Missing tags
// are absent from the map (document frequency 0).
func (s *Read) HashtagDocumentFrequencies(ctx context.Context, terms []string) (map[string]int64, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	cleaned := make([]string, 0, len(terms))
	seen := make(map[string]struct{}, len(terms))
	for _, term := range terms {
		term = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(term, "#")))
		if term == "" {
			continue
		}
		if _, ok := seen[term]; ok {
			continue
		}
		seen[term] = struct{}{}
		cleaned = append(cleaned, term)
		if len(cleaned) >= 1500 {
			break
		}
	}
	out := make(map[string]int64, len(cleaned))
	if len(cleaned) == 0 {
		return out, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT entity_id, GREATEST(popularity, 0)::bigint
		FROM search_documents
		WHERE entity_type = 'hashtag' AND entity_id = ANY($1::text[])
	`, cleaned)
	if err != nil {
		return nil, fmt.Errorf("hashtag document frequencies: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var term string
		var count int64
		if err := rows.Scan(&term, &count); err != nil {
			return nil, fmt.Errorf("scan hashtag document frequency: %w", err)
		}
		term = strings.ToLower(strings.TrimSpace(term))
		if term == "" || count <= 0 {
			continue
		}
		if count > out[term] {
			out[term] = count
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read hashtag document frequencies: %w", err)
	}
	return out, nil
}

// SearchProfileSignals loads follower and activity figures used to rank and
// explain profile search rows. Bot is the kind-0 "bot" flag.
func (s *Read) SearchProfileSignals(ctx context.Context, pubkeys []string) (map[string]readmodel.ProfileSearchSignal, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	cleaned := make([]string, 0, len(pubkeys))
	seen := make(map[string]struct{}, len(pubkeys))
	for _, pubkey := range pubkeys {
		pubkey = strings.ToLower(strings.TrimSpace(pubkey))
		if pubkey == "" {
			continue
		}
		if _, ok := seen[pubkey]; ok {
			continue
		}
		seen[pubkey] = struct{}{}
		cleaned = append(cleaned, pubkey)
		if len(cleaned) >= 200 {
			break
		}
	}
	out := make(map[string]readmodel.ProfileSearchSignal, len(cleaned))
	if len(cleaned) == 0 {
		return out, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT
			p.pubkey,
			coalesce(p.name, ''),
			coalesce(p.display_name, ''),
			coalesce(p.nip05, ''),
			coalesce(stats.follower_count, 0),
			coalesce(discovery.recent_post_count, 0),
			coalesce(discovery.recent_activity_at, stats.recent_activity_at),
			CASE
				WHEN lower(coalesce(p.profile_json->>'bot', '')) IN ('true', '1', 'yes') THEN true
				ELSE false
			END
		FROM profiles_latest p
		LEFT JOIN profile_public_stats stats ON stats.pubkey = p.pubkey
		LEFT JOIN profile_discovery_stats discovery ON discovery.pubkey = p.pubkey
		WHERE p.pubkey = ANY($1::text[])
	`, cleaned)
	if err != nil {
		return nil, fmt.Errorf("search profile signals: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var signal readmodel.ProfileSearchSignal
		if err := rows.Scan(
			&signal.Pubkey,
			&signal.Name,
			&signal.DisplayName,
			&signal.NIP05,
			&signal.FollowerCount,
			&signal.RecentPostCount,
			&signal.RecentActivityAt,
			&signal.Bot,
		); err != nil {
			return nil, fmt.Errorf("scan search profile signal: %w", err)
		}
		signal.Pubkey = strings.ToLower(strings.TrimSpace(signal.Pubkey))
		if signal.Pubkey == "" {
			continue
		}
		out[signal.Pubkey] = signal
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read search profile signals: %w", err)
	}
	return out, nil
}

// searchContentNeedles splits a query into the tsquery text and the ILIKE
// needle. Quoted phrases are a phrase operator: websearch_to_tsquery keeps
// the quotes, and the LIKE needle is the phrase without them.
func searchContentNeedles(raw string) (string, string) {
	raw = strings.TrimSpace(raw)
	if len(raw) >= 2 && strings.HasPrefix(raw, `"`) && strings.HasSuffix(raw, `"`) {
		inner := strings.TrimSpace(raw[1 : len(raw)-1])
		if inner != "" && !strings.Contains(inner, `"`) {
			return raw, inner
		}
	}
	return raw, raw
}
