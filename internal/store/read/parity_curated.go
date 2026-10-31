package read

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

func (s *Read) GetNetworkStats(ctx context.Context) (NetworkStats, error) {
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

func (s *Read) GetPublicDiscoveryNetworkStats(ctx context.Context, hashtagLimit int, lang string) (PublicDiscoveryNetworkStats, error) {
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
	relaySummary, relaySummaryComputedAt, err := s.getRelaySummaryStats(ctx)
	if err != nil {
		return out, err
	}
	out.RelaySummary = relaySummary
	out.Relays = relaySummary.Total
	out.SectionTimes.Relays = relaySummaryComputedAt
	topRelays24h, topRelays24hAt, err := s.getTopRelaysSnapshot(ctx, relaySnapshotLabelTopRelays24h, 10)
	if err != nil {
		return out, err
	}
	topRelays7d, topRelays7dAt, err := s.getTopRelaysSnapshot(ctx, relaySnapshotLabelTopRelays7d, 10)
	if err != nil {
		return out, err
	}
	out.TopRelays24h = topRelays24h
	out.TopRelays7d = topRelays7d
	out.TopRelays = topRelays7d
	out.SectionTimes.TopRelays24h = topRelays24hAt
	out.SectionTimes.TopRelays7d = topRelays7dAt

	// Note volume + active authors are served from the
	// home_window_24h / home_window_7d snapshot rows. The underlying
	// COUNT(DISTINCT author_pubkey) over note_discovery_stats is
	// CPU-bound at multiple seconds on production scale, so it lives
	// in the projection (see
	// internal/derivation/projection_relay_window_snapshots.go) just
	// like the relay summary does.
	homeWindow24h, err := s.getHomeWindowSnapshot(ctx, relaySnapshotLabelHomeWindow24h)
	if err != nil {
		return out, err
	}
	homeWindow7d, err := s.getHomeWindowSnapshot(ctx, relaySnapshotLabelHomeWindow7d)
	if err != nil {
		return out, err
	}
	homeWindow30d, err := s.getHomeWindowSnapshot(ctx, relaySnapshotLabelHomeWindow30d)
	if err != nil {
		return out, err
	}
	out.NoteVolume = WindowedCount{
		Last24h: homeWindow24h.NoteVolume,
		Last7d:  homeWindow7d.NoteVolume,
		Last30d: homeWindow30d.NoteVolume,
	}
	out.ActiveAuthors = WindowedCount{
		Last24h: homeWindow24h.ActiveAuthors,
		Last7d:  homeWindow7d.ActiveAuthors,
		Last30d: homeWindow30d.ActiveAuthors,
	}
	if !homeWindow24h.ComputedAt.IsZero() {
		out.ComputedAt = &homeWindow24h.ComputedAt
	}
	out.SectionTimes.Home24h = homeWindow24h.ComputedAt
	out.SectionTimes.Home7d = homeWindow7d.ComputedAt
	out.SectionTimes.Home30d = homeWindow30d.ComputedAt

	topLanguages24h, languages24hAt, err := s.getTopLanguagesSnapshot(ctx, relaySnapshotLabelTopLanguages24h)
	if err != nil {
		return out, err
	}
	topLanguages7d, languages7dAt, err := s.getTopLanguagesSnapshot(ctx, relaySnapshotLabelTopLanguages7d)
	if err != nil {
		return out, err
	}
	topLanguages30d, languages30dAt, err := s.getTopLanguagesSnapshot(ctx, relaySnapshotLabelTopLanguages30d)
	if err != nil {
		return out, err
	}
	out.TopLanguages24h = topLanguages24h
	out.TopLanguages7d = topLanguages7d
	out.TopLanguages30d = topLanguages30d
	out.SectionTimes.Languages24h = languages24hAt
	out.SectionTimes.Languages7d = languages7dAt
	out.SectionTimes.Languages30d = languages30dAt

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

	// Homepage trending hashtags and the topics tab both read this
	// snapshot for 24h, 7d, and 30d. Other windows still run a live aggregate.
	top24h, hashtags24hAt, err := s.getTopHashtagsSnapshot(ctx, relaySnapshotLabelTopHashtags24h, hashtagLimit)
	if err != nil {
		return out, err
	}
	top7d, hashtags7dAt, err := s.getTopHashtagsSnapshot(ctx, relaySnapshotLabelTopHashtags7d, hashtagLimit)
	if err != nil {
		return out, err
	}
	top30d, hashtags30dAt, err := s.getTopHashtagsSnapshot(ctx, relaySnapshotLabelTopHashtags30d, hashtagLimit)
	if err != nil {
		return out, err
	}
	out.TopHashtags = &TrendingHashtagWindows{
		Last24h: top24h,
		Last7d:  top7d,
		Last30d: top30d,
	}
	out.SectionTimes.Hashtags24h = hashtags24hAt
	out.SectionTimes.Hashtags7d = hashtags7dAt
	out.SectionTimes.Hashtags30d = hashtags30dAt
	if lang == "" {
		return out, nil
	}
	home24, err := s.homeWindowLanguage(ctx, relaySnapshotLabelHomeWindow24hLang, lang)
	if err != nil {
		return out, err
	}
	home7, err := s.homeWindowLanguage(ctx, relaySnapshotLabelHomeWindow7dLang, lang)
	if err != nil {
		return out, err
	}
	home30, err := s.homeWindowLanguage(ctx, relaySnapshotLabelHomeWindow30dLang, lang)
	if err != nil {
		return out, err
	}
	out.NoteVolume = WindowedCount{Last24h: home24.NoteVolume, Last7d: home7.NoteVolume, Last30d: home30.NoteVolume}
	out.ActiveAuthors = WindowedCount{Last24h: home24.ActiveAuthors, Last7d: home7.ActiveAuthors, Last30d: home30.ActiveAuthors}
	tags24, err := s.languageHashtags(ctx, relaySnapshotLabelTopHashtags24hLang, lang, hashtagLimit, 0)
	if err != nil {
		return out, err
	}
	tags7, err := s.languageHashtags(ctx, relaySnapshotLabelTopHashtags7dLang, lang, hashtagLimit, 0)
	if err != nil {
		return out, err
	}
	tags30, err := s.languageHashtags(ctx, relaySnapshotLabelTopHashtags30dLang, lang, hashtagLimit, 0)
	if err != nil {
		return out, err
	}
	out.TopHashtags = &TrendingHashtagWindows{Last24h: tags24, Last7d: tags7, Last30d: tags30}
	return out, nil
}

// Snapshot label constants — must match the labels written by the
// projection in internal/derivation/projection_relay_window_snapshots.go
// and seeded by migrations 000047_relay_window_snapshots.sql and
// 000048_homepage_window_snapshots.sql.
const (
	relaySnapshotLabelSummary         = "summary"
	relaySnapshotLabelTopRelays24h    = "top_relays_24h"
	relaySnapshotLabelTopRelays7d     = "top_relays_7d"
	relaySnapshotLabelHomeWindow24h   = "home_window_24h"
	relaySnapshotLabelHomeWindow7d    = "home_window_7d"
	relaySnapshotLabelTopLanguages24h = "top_languages_24h"
	relaySnapshotLabelTopLanguages7d  = "top_languages_7d"
	relaySnapshotLabelTopHashtags24h  = "top_hashtags_24h"
	relaySnapshotLabelTopHashtags7d   = "top_hashtags_7d"
	relaySnapshotLabelTopHashtags30d  = "top_hashtags_30d"
	relaySnapshotLabelTopDomains24h   = "top_domains_24h"
	relaySnapshotLabelTopDomains7d    = "top_domains_7d"
	relaySnapshotLabelTopDomains30d   = "top_domains_30d"
	relaySnapshotLabelHomeWindow30d   = "home_window_30d"
	relaySnapshotLabelTopLanguages30d = "top_languages_30d"

	relaySnapshotLabelHomeWindow24hLang  = "home_window_24h.lang"
	relaySnapshotLabelHomeWindow7dLang   = "home_window_7d.lang"
	relaySnapshotLabelHomeWindow30dLang  = "home_window_30d.lang"
	relaySnapshotLabelTopHashtags24hLang = "top_hashtags_24h.lang"
	relaySnapshotLabelTopHashtags7dLang  = "top_hashtags_7d.lang"
	relaySnapshotLabelTopHashtags30dLang = "top_hashtags_30d.lang"
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

// getTopRelaysSnapshot reads the precomputed top-relays list from
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
func (s *Read) getTopRelaysSnapshot(ctx context.Context, label string, limit int) ([]RelayUsageSummary, time.Time, error) {
	if limit <= 0 {
		limit = 10
	}
	if limit > 50 {
		limit = 50
	}
	var payload []byte
	var computedAt time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT payload, computed_at
		FROM relay_window_snapshots
		WHERE snapshot_label = $1
	`, label).Scan(&payload, &computedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return []RelayUsageSummary{}, time.Time{}, nil
		}
		if isUndefinedRelationError(err) {
			// Pre-000047 environments: the table doesn't exist yet.
			// Serve an empty list so the homepage stays up while the
			// deploy completes.
			return []RelayUsageSummary{}, time.Time{}, nil
		}
		return nil, time.Time{}, fmt.Errorf("read top relays snapshot %q: %w", label, err)
	}
	var rows []relaySnapshotTopRelayRow
	if err := json.Unmarshal(payload, &rows); err != nil {
		return nil, time.Time{}, fmt.Errorf("decode top relays snapshot payload %q: %w", label, err)
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
	return out, computedAt, nil
}

// getRelaySummaryStats reads the precomputed summary row from
// relay_window_snapshots. See the comment on getTopRelaysSnapshot
// for why there is intentionally no live-query fallback.
func (s *Read) getRelaySummaryStats(ctx context.Context) (RelaySummaryStats, time.Time, error) {
	var payload []byte
	var computedAt time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT payload, computed_at
		FROM relay_window_snapshots
		WHERE snapshot_label = $1
	`, relaySnapshotLabelSummary).Scan(&payload, &computedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RelaySummaryStats{}, time.Time{}, nil
		}
		if isUndefinedRelationError(err) {
			return RelaySummaryStats{}, time.Time{}, nil
		}
		return RelaySummaryStats{}, time.Time{}, fmt.Errorf("read relay summary snapshot: %w", err)
	}
	var p relaySummarySnapshotPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return RelaySummaryStats{}, time.Time{}, fmt.Errorf("decode relay summary snapshot payload: %w", err)
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
	}, computedAt, nil
}

// homeWindowSnapshotPayload mirrors the JSONB shape stored under
// snapshot_label = 'home_window_24h' / 'home_window_7d'. Kept private
// to the store package; the public response shape splits these into
// PublicDiscoveryNetworkStats.NoteVolume and .ActiveAuthors.
type homeWindowSnapshotPayload struct {
	NoteVolume    int64 `json:"note_volume"`
	ActiveAuthors int64 `json:"active_authors"`
	ComputedAt    time.Time
}

func (s *Read) getHomeWindowSnapshot(ctx context.Context, label string) (homeWindowSnapshotPayload, error) {
	var payload []byte
	var out homeWindowSnapshotPayload
	err := s.pool.QueryRow(ctx, `
		SELECT payload, computed_at
		FROM relay_window_snapshots
		WHERE snapshot_label = $1
	`, label).Scan(&payload, &out.ComputedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) || isUndefinedRelationError(err) {
			// Pre-000048 or freshly-truncated environments: serve
			// zeros so the homepage stays up. The next worker tick
			// will populate the row.
			return homeWindowSnapshotPayload{}, nil
		}
		return homeWindowSnapshotPayload{}, fmt.Errorf("read home window snapshot %q: %w", label, err)
	}
	if err := json.Unmarshal(payload, &out); err != nil {
		return homeWindowSnapshotPayload{}, fmt.Errorf("decode home window snapshot %q: %w", label, err)
	}
	return out, nil
}

// GetDiscoveryStatsSeries returns hourly snapshots of rolling 24-hour public
// metrics. The table is intentionally queried directly rather than falling
// back to live aggregates so this endpoint stays safe at production scale.
func (s *Read) GetDiscoveryStatsSeries(ctx context.Context, metric string, window time.Duration) (DiscoveryStatsSeries, error) {
	out := DiscoveryStatsSeries{
		Metric: metric,
		Points: []DiscoveryStatsSeriesPoint{},
	}
	if s == nil || s.pool == nil {
		return out, fmt.Errorf("store is not initialized")
	}
	if window <= 0 {
		return out, fmt.Errorf("window must be positive")
	}
	column, ok := map[string]string{
		"note_volume":    "note_volume",
		"active_authors": "active_authors",
		"relay_events":   "relay_events",
	}[metric]
	if !ok {
		return out, fmt.Errorf("unsupported stats metric %q", metric)
	}

	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT bucket_start, computed_at, %s
		FROM stats_snapshot_history
		WHERE bucket_start >= $1
		ORDER BY bucket_start ASC
	`, column), time.Now().UTC().Add(-window))
	if err != nil {
		if isUndefinedRelationError(err) {
			return out, nil
		}
		return out, fmt.Errorf("read discovery stats series: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var point DiscoveryStatsSeriesPoint
		var computedAt time.Time
		if err := rows.Scan(&point.T, &computedAt, &point.V); err != nil {
			return out, fmt.Errorf("scan discovery stats series point: %w", err)
		}
		out.ComputedAt = &computedAt
		out.Points = append(out.Points, point)
	}
	if err := rows.Err(); err != nil {
		return out, fmt.Errorf("read discovery stats series rows: %w", err)
	}
	return out, nil
}

type languageSnapshotRow struct {
	Language string `json:"language"`
	Count    int64  `json:"count"`
}

// getTopLanguagesSnapshot reads the precomputed language chart. The
// projection already keeps the highest real language codes plus unmarked
// buckets. Return that row whole: discovery requests have no language-count
// parameter, and slicing here would drop codes the chart is supposed to show.
func (s *Read) getTopLanguagesSnapshot(ctx context.Context, label string) ([]LanguageSummary, time.Time, error) {
	var payload []byte
	var computedAt time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT payload, computed_at
		FROM relay_window_snapshots
		WHERE snapshot_label = $1
	`, label).Scan(&payload, &computedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) || isUndefinedRelationError(err) {
			return []LanguageSummary{}, time.Time{}, nil
		}
		return nil, time.Time{}, fmt.Errorf("read top languages snapshot %q: %w", label, err)
	}
	var rows []languageSnapshotRow
	if err := json.Unmarshal(payload, &rows); err != nil {
		return nil, time.Time{}, fmt.Errorf("decode top languages snapshot %q: %w", label, err)
	}
	out := make([]LanguageSummary, 0, len(rows))
	for _, r := range rows {
		out = append(out, LanguageSummary{Language: r.Language, Count: r.Count})
	}
	return out, computedAt, nil
}

type hashtagSnapshotRow struct {
	Hashtag       string `json:"hashtag"`
	EventCount    int64  `json:"event_count"`
	UniqueAuthors int64  `json:"unique_authors"`
}

// GetHomeTrendingHashtags pages the precomputed top_hashtags_24h / top_hashtags_7d
// snapshot. The topics tab and the homepage overview both use it, so a 24h
// or 7d request returns the same ranking on either surface. Offset past the
// materialized list (50) is empty rather than falling through to a
// differently ranked live aggregate.
func (s *Read) GetHomeTrendingHashtags(
	ctx context.Context,
	window time.Duration,
	lang string,
	limit int,
	offset int,
) ([]TrendingHashtag, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	label, err := resolveHomeHashtagsSnapshotLabel(window)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	var rows []TrendingHashtag
	if lang != "" {
		rows, err = s.languageHashtags(ctx, label+".lang", lang, 50, 0)
	} else {
		rows, _, err = s.getTopHashtagsSnapshot(ctx, label, 50)
	}
	if err != nil {
		return nil, err
	}
	if offset >= len(rows) {
		return []TrendingHashtag{}, nil
	}
	rows = rows[offset:]
	if len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

func (s *Read) readSnapshotPayload(ctx context.Context, label string) ([]byte, error) {
	var payload []byte
	err := s.pool.QueryRow(ctx, `
		SELECT payload
		FROM relay_window_snapshots
		WHERE snapshot_label = $1
	`, label).Scan(&payload)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) || isUndefinedRelationError(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read snapshot %q: %w", label, err)
	}
	return payload, nil
}

type homeWindowLanguageSnapshot struct {
	NoteVolume    int64 `json:"note_volume"`
	ActiveAuthors int64 `json:"active_authors"`
}

func (s *Read) homeWindowLanguage(ctx context.Context, label, lang string) (homeWindowLanguageSnapshot, error) {
	payload, err := s.readSnapshotPayload(ctx, label)
	if err != nil || len(payload) == 0 {
		return homeWindowLanguageSnapshot{}, err
	}
	var byLang map[string]homeWindowLanguageSnapshot
	if err := json.Unmarshal(payload, &byLang); err != nil {
		return homeWindowLanguageSnapshot{}, fmt.Errorf("decode home window languages %q: %w", label, err)
	}
	return byLang[lang], nil
}

func (s *Read) languageHashtags(ctx context.Context, label, lang string, limit, offset int) ([]TrendingHashtag, error) {
	payload, err := s.readSnapshotPayload(ctx, label)
	if err != nil || len(payload) == 0 {
		return []TrendingHashtag{}, err
	}
	var byLang map[string][]hashtagSnapshotRow
	if err := json.Unmarshal(payload, &byLang); err != nil {
		return nil, fmt.Errorf("decode hashtag languages %q: %w", label, err)
	}
	rows := byLang[lang]
	if offset >= len(rows) {
		return []TrendingHashtag{}, nil
	}
	if offset > 0 {
		rows = rows[offset:]
	}
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	out := make([]TrendingHashtag, 0, len(rows))
	for _, row := range rows {
		out = append(out, TrendingHashtag{
			Hashtag:       row.Hashtag,
			EventCount:    row.EventCount,
			UniqueAuthors: row.UniqueAuthors,
		})
	}
	return out, nil
}

func resolveHomeHashtagsSnapshotLabel(window time.Duration) (string, error) {
	switch window {
	case 24 * time.Hour:
		return relaySnapshotLabelTopHashtags24h, nil
	case 7 * 24 * time.Hour:
		return relaySnapshotLabelTopHashtags7d, nil
	case 30 * 24 * time.Hour:
		return relaySnapshotLabelTopHashtags30d, nil
	default:
		return "", fmt.Errorf("unsupported home trending hashtags window: %s", window)
	}
}

func (s *Read) getTopHashtagsSnapshot(ctx context.Context, label string, limit int) ([]TrendingHashtag, time.Time, error) {
	if limit <= 0 {
		limit = 10
	}
	if limit > 50 {
		limit = 50
	}
	var payload []byte
	var computedAt time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT payload, computed_at
		FROM relay_window_snapshots
		WHERE snapshot_label = $1
	`, label).Scan(&payload, &computedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) || isUndefinedRelationError(err) {
			return []TrendingHashtag{}, time.Time{}, nil
		}
		return nil, time.Time{}, fmt.Errorf("read top hashtags snapshot %q: %w", label, err)
	}
	var rows []hashtagSnapshotRow
	if err := json.Unmarshal(payload, &rows); err != nil {
		return nil, time.Time{}, fmt.Errorf("decode top hashtags snapshot %q: %w", label, err)
	}
	if len(rows) > limit {
		rows = rows[:limit]
	}
	out := make([]TrendingHashtag, 0, len(rows))
	for _, r := range rows {
		out = append(out, TrendingHashtag{
			Hashtag:       r.Hashtag,
			EventCount:    r.EventCount,
			UniqueAuthors: r.UniqueAuthors,
		})
	}
	return out, computedAt, nil
}

func isUndefinedRelationError(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P01"
}

func (s *Read) GetCuratedValues(ctx context.Context, tableName string, valueColumn string, limit int) ([]string, error) {
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

func (s *Read) GetCuratedRecommendedReads(ctx context.Context, limit int) ([]CuratedRecommendedRead, error) {
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

func (s *Read) GetCuratedReadsTopics(ctx context.Context, limit int) ([]CuratedReadsTopic, error) {
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

func (s *Read) GetCuratedFeaturedAuthors(ctx context.Context, limit int) ([]CuratedFeaturedAuthor, error) {
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

func (s *Read) GetCreatorPaidTiers(ctx context.Context, pubkey string) ([]json.RawMessage, error) {
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

func (s *Read) GetPubkeyByLNAddress(ctx context.Context, lnAddress string) (string, error) {
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
