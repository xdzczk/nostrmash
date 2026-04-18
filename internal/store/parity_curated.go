package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func (s *PostgresStore) GetNetworkStats(ctx context.Context) (NetworkStats, error) {
	out := NetworkStats{}
	if s == nil || s.pool == nil {
		return out, fmt.Errorf("store is not initialized")
	}
	// events is the largest table in the schema (~10M+ rows in production).
	// `SELECT count(*)` forces a sequential scan on every call and adds
	// 1-3s of latency to the homepage bundle, which is recomputed every
	// time the in-process cache expires. The "events ingested" number on
	// the public homepage is informational only — an estimate within a
	// few percent is indistinguishable to the user — so prefer the
	// planner's live tuple estimate maintained by autoanalyze, falling
	// back to the exact count only if the estimate is unavailable
	// (fresh table that hasn't been analyzed yet).
	out.Events, _ = approxLiveTupleCount(ctx, s.pool, "events")
	if out.Events <= 0 {
		if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&out.Events); err != nil {
			return out, fmt.Errorf("count events: %w", err)
		}
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM profiles_latest`).Scan(&out.Profiles); err != nil {
		return out, fmt.Errorf("count profiles: %w", err)
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM ingest_checkpoints`).Scan(&out.Relays); err != nil {
		return out, fmt.Errorf("count relays: %w", err)
	}
	return out, nil
}

// approxLiveTupleCount returns the planner's estimate of the live row
// count for `tableName` from pg_stat_user_tables. The estimate is updated
// by autoanalyze and is good to within a few percent on busy tables,
// which is sufficient for displaying ingestion volume on the public
// homepage. Returns (0, nil) if the planner has no estimate yet.
func approxLiveTupleCount(ctx context.Context, pool *pgxpool.Pool, tableName string) (int64, error) {
	if pool == nil || strings.TrimSpace(tableName) == "" {
		return 0, nil
	}
	var estimate int64
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(MAX(n_live_tup), 0)::bigint
		FROM pg_stat_user_tables
		WHERE schemaname = current_schema()
		  AND relname = $1
	`, tableName).Scan(&estimate)
	if err != nil {
		return 0, fmt.Errorf("approx tuple count for %s: %w", tableName, err)
	}
	return estimate, nil
}

func (s *PostgresStore) GetPublicDiscoveryNetworkStats(ctx context.Context, hashtagLimit int) (PublicDiscoveryNetworkStats, error) {
	out := PublicDiscoveryNetworkStats{}
	if s == nil || s.pool == nil {
		return out, fmt.Errorf("store is not initialized")
	}
	if hashtagLimit <= 0 {
		hashtagLimit = 10
	}
	if hashtagLimit > 50 {
		hashtagLimit = 50
	}
	networkStats, err := s.GetNetworkStats(ctx)
	if err != nil {
		return out, err
	}
	out.EventsIngested = networkStats.Events
	out.ProjectedProfiles = networkStats.Profiles
	relaySummary, err := s.getRelaySummaryStats(ctx)
	if err != nil {
		return out, err
	}
	out.RelaySummary = relaySummary
	out.Relays = relaySummary.Total
	topRelays, err := s.getTopRelaysByActivity(ctx, 10)
	if err != nil {
		return out, err
	}
	out.TopRelays = topRelays

	now := time.Now().UTC()
	last24hNotes, last24hAuthors, err := s.getPublicWindowStats(ctx, now.Add(-24*time.Hour).Unix())
	if err != nil {
		return out, err
	}
	last7dNotes, last7dAuthors, err := s.getPublicWindowStats(ctx, now.Add(-7*24*time.Hour).Unix())
	if err != nil {
		return out, err
	}
	out.NoteVolume = WindowedCount{
		Last24h: last24hNotes,
		Last7d:  last7dNotes,
	}
	out.ActiveAuthors = WindowedCount{
		Last24h: last24hAuthors,
		Last7d:  last7dAuthors,
	}
	topLanguages24h, err := s.getTopLanguages(ctx, now.Add(-24*time.Hour).Unix(), 8)
	if err != nil {
		return out, err
	}
	topLanguages7d, err := s.getTopLanguages(ctx, now.Add(-7*24*time.Hour).Unix(), 8)
	if err != nil {
		return out, err
	}
	out.TopLanguages24h = topLanguages24h
	out.TopLanguages7d = topLanguages7d

	// Projection tables are schema-scoped in tests; explicitly check current schema
	// so we don't silently fall back to another schema via search_path.
	var hashtagsProjectionAvailable bool
	if err := s.pool.QueryRow(ctx, `
		SELECT to_regclass(current_schema() || '.event_hashtags') IS NOT NULL
	`).Scan(&hashtagsProjectionAvailable); err != nil {
		return out, fmt.Errorf("check event_hashtags projection availability: %w", err)
	}
	if !hashtagsProjectionAvailable {
		return out, nil
	}

	top24h, err := s.GetTrendingHashtags(ctx, 24*time.Hour, hashtagLimit, 0)
	if err != nil {
		if !isUndefinedRelationError(err) {
			return out, err
		}
		return out, nil
	}
	top7d, err := s.GetTrendingHashtags(ctx, 7*24*time.Hour, hashtagLimit, 0)
	if err != nil {
		if !isUndefinedRelationError(err) {
			return out, err
		}
		return out, nil
	}
	out.TopHashtags = &TrendingHashtagWindows{
		Last24h: top24h,
		Last7d:  top7d,
	}
	return out, nil
}

func (s *PostgresStore) getTopLanguages(ctx context.Context, minCreatedAt int64, limit int) ([]LanguageSummary, error) {
	if limit <= 0 {
		limit = 8
	}
	rows, err := s.pool.Query(ctx, `
		SELECT
			COALESCE(primary_language, 'und') AS language,
			COUNT(*)::bigint AS count_value
		FROM note_discovery_stats
		WHERE created_at >= $1
		GROUP BY COALESCE(primary_language, 'und')
		ORDER BY count_value DESC, language ASC
		LIMIT $2
	`, minCreatedAt, limit)
	if err != nil {
		return nil, fmt.Errorf("get top languages: %w", err)
	}
	defer rows.Close()
	out := make([]LanguageSummary, 0, limit)
	for rows.Next() {
		var row LanguageSummary
		if err := rows.Scan(&row.Language, &row.Count); err != nil {
			return nil, fmt.Errorf("scan top language row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read top language rows: %w", err)
	}
	return out, nil
}

// Snapshot label constants — must match the labels written by the
// projection in internal/derivation/projection_relay_window_snapshots.go
// and seeded by migration 000047_relay_window_snapshots.sql.
const (
	relaySnapshotLabelSummary     = "summary"
	relaySnapshotLabelTopRelays7d = "top_relays_7d"
)

// relaySummarySnapshotPayload mirrors the JSONB shape stored under
// snapshot_label = 'summary'. Kept private; the public response shape
// is RelaySummaryStats (defined in parity_types.go).
type relaySummarySnapshotPayload struct {
	Total      int64 `json:"total"`
	Active24h  int64 `json:"active_24h"`
	Active7d   int64 `json:"active_7d"`
	Events24h  int64 `json:"events_24h"`
	Events7d   int64 `json:"events_7d"`
	Authors24h int64 `json:"authors_24h"`
	Authors7d  int64 `json:"authors_7d"`
}

type relaySnapshotTopRelayRow struct {
	RelayURL      string `json:"relay_url"`
	EventCount    int64  `json:"event_count"`
	UniqueAuthors int64  `json:"unique_authors"`
}

// getTopRelaysByActivity reads the precomputed top-relays list from
// relay_window_snapshots. The actual aggregate (which spends ~10s of
// CPU per run on production) is owned by the worker process (see
// internal/derivation/projection_relay_window_snapshots.go). Reading
// is a single sub-millisecond row lookup followed by a JSON decode.
//
// Why we never fall back to a live query
// --------------------------------------
// The whole point of this snapshot is to keep the multi-second
// COUNT(DISTINCT pubkey) aggregate off the request path. A
// "fall back to live query when snapshot is missing" branch would
// reintroduce exactly the failure mode this projection exists to
// prevent: one cold cache could timeout the homepage handler and
// starve every other endpoint of DB connections. The migration
// seeds the row, the worker keeps it fresh, and on bootstrap
// failure we'd rather serve stale-but-fast data than risk a
// cascading outage.
//
// If the row is missing entirely we return an empty list and rely
// on the missing-row signal in metrics/logs to surface the
// operational failure (the migration has not run, or the table was
// truncated).
func (s *PostgresStore) getTopRelaysByActivity(ctx context.Context, limit int) ([]RelayUsageSummary, error) {
	if limit <= 0 {
		limit = 10
	}
	if limit > 50 {
		limit = 50
	}
	var payload []byte
	err := s.pool.QueryRow(ctx, `
		SELECT payload
		FROM relay_window_snapshots
		WHERE snapshot_label = $1
	`, relaySnapshotLabelTopRelays7d).Scan(&payload)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return []RelayUsageSummary{}, nil
		}
		if isUndefinedRelationError(err) {
			// Pre-000047 environments: the table doesn't exist yet.
			// Serve an empty list so the homepage stays up while the
			// deploy completes.
			return []RelayUsageSummary{}, nil
		}
		return nil, fmt.Errorf("read top relays snapshot: %w", err)
	}
	var rows []relaySnapshotTopRelayRow
	if err := json.Unmarshal(payload, &rows); err != nil {
		return nil, fmt.Errorf("decode top relays snapshot payload: %w", err)
	}
	if len(rows) > limit {
		rows = rows[:limit]
	}
	out := make([]RelayUsageSummary, 0, len(rows))
	for _, r := range rows {
		out = append(out, RelayUsageSummary{
			RelayURL:      r.RelayURL,
			EventCount:    r.EventCount,
			UniqueAuthors: r.UniqueAuthors,
		})
	}
	return out, nil
}

// getRelaySummaryStats reads the precomputed summary row from
// relay_window_snapshots. See the comment on getTopRelaysByActivity
// for why there is intentionally no live-query fallback.
func (s *PostgresStore) getRelaySummaryStats(ctx context.Context) (RelaySummaryStats, error) {
	var payload []byte
	err := s.pool.QueryRow(ctx, `
		SELECT payload
		FROM relay_window_snapshots
		WHERE snapshot_label = $1
	`, relaySnapshotLabelSummary).Scan(&payload)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RelaySummaryStats{}, nil
		}
		if isUndefinedRelationError(err) {
			return RelaySummaryStats{}, nil
		}
		return RelaySummaryStats{}, fmt.Errorf("read relay summary snapshot: %w", err)
	}
	var p relaySummarySnapshotPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return RelaySummaryStats{}, fmt.Errorf("decode relay summary snapshot payload: %w", err)
	}
	return RelaySummaryStats{
		Total:     p.Total,
		Active24h: p.Active24h,
		Active7d:  p.Active7d,
		EventVolume: WindowedCount{
			Last24h: p.Events24h,
			Last7d:  p.Events7d,
		},
		UniqueAuthors: WindowedCount{
			Last24h: p.Authors24h,
			Last7d:  p.Authors7d,
		},
	}, nil
}

func (s *PostgresStore) getPublicWindowStats(ctx context.Context, minCreatedAt int64) (int64, int64, error) {
	var noteVolume int64
	var activeAuthors int64
	if err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*)::bigint, COUNT(DISTINCT author_pubkey)::bigint
		FROM note_discovery_stats
		WHERE created_at >= $1
	`, minCreatedAt).Scan(&noteVolume, &activeAuthors); err != nil {
		return 0, 0, fmt.Errorf("get note discovery window stats: %w", err)
	}
	return noteVolume, activeAuthors, nil
}

func isUndefinedRelationError(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P01"
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
