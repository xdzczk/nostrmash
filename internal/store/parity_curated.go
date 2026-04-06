package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func (s *PostgresStore) GetNetworkStats(ctx context.Context) (NetworkStats, error) {
	out := NetworkStats{}
	if s == nil || s.pool == nil {
		return out, fmt.Errorf("store is not initialized")
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&out.Events); err != nil {
		return out, fmt.Errorf("count events: %w", err)
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM profiles_latest`).Scan(&out.Profiles); err != nil {
		return out, fmt.Errorf("count profiles: %w", err)
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM ingest_checkpoints`).Scan(&out.Relays); err != nil {
		return out, fmt.Errorf("count relays: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetCuratedValues(ctx context.Context, tableName string, valueColumn string, limit int) ([]string, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	tableName = strings.TrimSpace(tableName)
	valueColumn = strings.TrimSpace(valueColumn)
	if tableName == "" || valueColumn == "" {
		return nil, fmt.Errorf("table and value column are required")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	allowed := map[string]map[string]struct{}{
		"curated_recommended_reads": {"event_id": {}},
		"curated_reads_topics":      {"topic": {}},
		"curated_featured_authors":  {"pubkey": {}},
	}
	columns, ok := allowed[tableName]
	if !ok {
		return nil, fmt.Errorf("unsupported curated table: %s", tableName)
	}
	if _, ok := columns[valueColumn]; !ok {
		return nil, fmt.Errorf("unsupported curated value column: %s", valueColumn)
	}
	query := fmt.Sprintf(`SELECT %s FROM %s ORDER BY rank DESC, %s ASC LIMIT $1`, valueColumn, tableName, valueColumn)
	rows, err := s.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("get curated values from %s: %w", tableName, err)
	}
	defer rows.Close()
	out := make([]string, 0, limit)
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, fmt.Errorf("scan curated value row: %w", err)
		}
		out = append(out, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read curated values rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetCuratedRecommendedReads(ctx context.Context, limit int) ([]CuratedRecommendedRead, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT event_id, title, url, rank
		FROM curated_recommended_reads
		ORDER BY rank DESC, event_id ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("get curated recommended reads: %w", err)
	}
	defer rows.Close()
	out := make([]CuratedRecommendedRead, 0, limit)
	for rows.Next() {
		var row CuratedRecommendedRead
		if err := rows.Scan(&row.EventID, &row.Title, &row.URL, &row.Rank); err != nil {
			return nil, fmt.Errorf("scan curated recommended read row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read curated recommended read rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetCuratedReadsTopics(ctx context.Context, limit int) ([]CuratedReadsTopic, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT topic, rank
		FROM curated_reads_topics
		ORDER BY rank DESC, topic ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("get curated reads topics: %w", err)
	}
	defer rows.Close()
	out := make([]CuratedReadsTopic, 0, limit)
	for rows.Next() {
		var row CuratedReadsTopic
		if err := rows.Scan(&row.Topic, &row.Rank); err != nil {
			return nil, fmt.Errorf("scan curated reads topic row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read curated reads topic rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetCuratedFeaturedAuthors(ctx context.Context, limit int) ([]CuratedFeaturedAuthor, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT pubkey, rank
		FROM curated_featured_authors
		ORDER BY rank DESC, pubkey ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("get curated featured authors: %w", err)
	}
	defer rows.Close()
	out := make([]CuratedFeaturedAuthor, 0, limit)
	for rows.Next() {
		var row CuratedFeaturedAuthor
		if err := rows.Scan(&row.Pubkey, &row.Rank); err != nil {
			return nil, fmt.Errorf("scan curated featured author row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read curated featured author rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetCreatorPaidTiers(ctx context.Context, pubkey string) ([]json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return []json.RawMessage{}, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT json_build_object(
			'tier_id', tier_id,
			'title', title,
			'price_sats', price_sats
		)::text
		FROM curated_creator_paid_tiers
		WHERE pubkey = $1
		ORDER BY price_sats ASC, tier_id ASC
	`, pubkey)
	if err != nil {
		return nil, fmt.Errorf("get creator paid tiers: %w", err)
	}
	defer rows.Close()
	out := make([]json.RawMessage, 0)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan creator paid tier row: %w", err)
		}
		out = append(out, json.RawMessage(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read creator paid tier rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetPubkeyByLNAddress(ctx context.Context, lnAddress string) (string, error) {
	if s == nil || s.pool == nil {
		return "", fmt.Errorf("store is not initialized")
	}
	normalized := strings.ToLower(strings.TrimSpace(lnAddress))
	if normalized == "" {
		return "", ErrNotFound
	}
	var pubkey string
	err := s.pool.QueryRow(ctx, `
		SELECT pubkey
		FROM profiles_latest
		WHERE lower(coalesce(nip05, '')) = $1
		LIMIT 1
	`, normalized).Scan(&pubkey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", fmt.Errorf("get pubkey by ln address: %w", err)
	}
	return pubkey, nil
}
