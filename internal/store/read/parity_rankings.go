package read

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/xdzczk/nostrmash/internal/languagelabel"
)

const rankedProfileExistsSQL = `
		  AND (
			EXISTS (
				SELECT 1 FROM profiles_latest pl WHERE pl.pubkey = %s
			)
			OR EXISTS (
				SELECT 1 FROM events metadata
				WHERE metadata.pubkey = %s
				  AND metadata.kind = 0
			)
		  )`

// discoveryReadLimit is the most rows a discovery read returns. Public
// handlers ask for at most 100. Viewer-scoped ranking loads a larger
// candidate pool (default 300) before it blends and pages.
const discoveryReadLimit = 500

func clampRankPage(limit, offset int) (int, int) {
	if limit <= 0 {
		limit = 20
	}
	if limit > discoveryReadLimit {
		limit = discoveryReadLimit
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}

// GetBookmarkedNotes ranks kind-1 notes and kind-30023 articles by how many
// distinct people currently have them on a public bookmark list. The author
// saving their own note does not count. There is no time window: the count
// is whoever still has it saved.
func (s *Read) GetBookmarkedNotes(ctx context.Context, lang string, limit int, offset int) ([]TrendingNote, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	limit, offset = clampRankPage(limit, offset)
	query := fmt.Sprintf(`
		SELECT
			e.id,
			e.pubkey,
			e.created_at,
			e.content,
			e.kind,
			`+languagelabel.ChartSQL("s.primary_language", "s.language_source")+` AS language,
			COALESCE(s.reply_count, 0),
			COALESCE(s.repost_count, 0),
			COALESCE(s.reaction_count, 0),
			COALESCE(s.zap_count, 0),
			COALESCE(s.zap_msats, 0),
			b.bookmark_count::float8 AS score,
			b.bookmark_count
		FROM note_bookmark_stats b
		JOIN events e ON e.id = b.event_id
		LEFT JOIN note_discovery_stats s ON s.event_id = b.event_id
		WHERE b.bookmark_count > 0
		  AND e.kind IN (1, 30023)
		  %s
		  %s
		ORDER BY b.bookmark_count DESC, e.created_at DESC, e.id ASC
		LIMIT $1 OFFSET $2
	`, fmt.Sprintf(rankedProfileExistsSQL, "e.pubkey", "e.pubkey"), noteLanguageClause("s", "$3::text", lang))
	args := []any{limit, offset}
	if lang != "" {
		args = append(args, lang)
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get bookmarked notes: %w", err)
	}
	defer rows.Close()
	return scanRankedNotes(rows, true)
}

// GetMostZappedNotes ranks root notes and articles published in the window
// by sats received in that same window. The worker refreshes
// note_zap_window_stats; this read does not sum receipts. The created_at
// check drops a note that has aged out of the window since the last refresh.
func (s *Read) GetMostZappedNotes(ctx context.Context, window time.Duration, lang string, limit int, offset int) ([]TrendingNote, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	satsColumn, countColumn, err := zapWindowColumns(window)
	if err != nil {
		return nil, err
	}
	limit, offset = clampRankPage(limit, offset)
	publishedAfter := time.Now().UTC().Add(-window).Unix()
	query := fmt.Sprintf(`
		SELECT
			e.id,
			e.pubkey,
			e.created_at,
			e.content,
			e.kind,
			`+languagelabel.ChartSQL("s.primary_language", "s.language_source")+` AS language,
			COALESCE(s.reply_count, 0),
			COALESCE(s.repost_count, 0),
			COALESCE(s.reaction_count, 0),
			%s,
			%s * 1000,
			%s::float8 AS score,
			0::bigint
		FROM note_zap_window_stats z
		JOIN events e ON e.id = z.event_id
		LEFT JOIN note_discovery_stats s ON s.event_id = z.event_id
		WHERE %s > 0
		  AND e.created_at >= $3
		  AND e.kind IN (1, 30023)
		  AND NOT EXISTS (
			SELECT 1 FROM thread_edges te WHERE te.child_event_id = e.id
		  )
		  %s
		  %s
		ORDER BY %s DESC, e.created_at DESC, e.id ASC
		LIMIT $1 OFFSET $2
	`, countColumn, satsColumn, satsColumn, satsColumn,
		fmt.Sprintf(rankedProfileExistsSQL, "e.pubkey", "e.pubkey"),
		noteLanguageClause("s", "$4::text", lang),
		satsColumn)
	args := []any{limit, offset, publishedAfter}
	if lang != "" {
		args = append(args, lang)
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get most zapped notes: %w", err)
	}
	defer rows.Close()
	return scanRankedNotes(rows, true)
}

func zapWindowColumns(window time.Duration) (satsColumn, countColumn string, err error) {
	switch window {
	case 24 * time.Hour:
		return "z.sats_24h", "z.zap_count_24h", nil
	case 7 * 24 * time.Hour:
		return "z.sats_7d", "z.zap_count_7d", nil
	case 30 * 24 * time.Hour:
		return "z.sats_30d", "z.zap_count_30d", nil
	default:
		return "", "", fmt.Errorf("unsupported trending window: %s", window)
	}
}

func scanRankedNotes(rows interface {
	Next() bool
	Scan(...any) error
	Err() error
}, withBookmark bool) ([]TrendingNote, error) {
	out := make([]TrendingNote, 0)
	for rows.Next() {
		var row TrendingNote
		dest := []any{
			&row.EventID,
			&row.AuthorPubkey,
			&row.CreatedAt,
			&row.Content,
			&row.Kind,
			&row.Language,
			&row.ReplyCount,
			&row.RepostCount,
			&row.ReactionCount,
			&row.ZapCount,
			&row.ZapMSats,
			&row.Score,
		}
		if withBookmark {
			dest = append(dest, &row.BookmarkCount)
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("scan ranked note: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read ranked notes: %w", err)
	}
	return out, nil
}

// GetMostFollowedProfiles ranks accounts that have a profile by how many
// followers sit in the published trust graph. Dormant accounts and accounts
// under the raw follower floor stay in the list when that count is positive.
// A trusted count of zero does not rank. follower_count on the row stays the
// raw directory count.
func (s *Read) GetMostFollowedProfiles(ctx context.Context, limit int, offset int) ([]TrendingProfile, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	limit, offset = clampRankPage(limit, offset)
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT
			pps.pubkey,
			pps.trusted_follower_count::float8 AS score,
			0::bigint,
			0::bigint,
			0::bigint,
			0::bigint,
			0::bigint,
			0,
			pps.recent_activity_at,
			pps.follower_count,
			NULL::float8,
			NULL::float8,
			0::bigint,
			0::bigint,
			0::bigint,
			pps.trusted_follower_count
		FROM profile_public_stats pps
		WHERE pps.trusted_follower_count > 0
		  %s
		ORDER BY pps.trusted_follower_count DESC, pps.pubkey ASC
		LIMIT $1 OFFSET $2
	`, fmt.Sprintf(rankedProfileExistsSQL, "pps.pubkey", "pps.pubkey")), limit, offset)
	if err != nil {
		return nil, fmt.Errorf("get most followed profiles: %w", err)
	}
	defer rows.Close()
	return scanRankedProfiles(rows)
}

// GetMostSupportedProfiles ranks accounts by sats received in the window
// after each sender is capped. The worker refreshes profile_support_stats.
func (s *Read) GetMostSupportedProfiles(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingProfile, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	capped, raw, supporters, err := supportWindowColumns(window)
	if err != nil {
		return nil, err
	}
	limit, offset = clampRankPage(limit, offset)
	query := fmt.Sprintf(`
		SELECT
			p.pubkey,
			p.%s::float8 AS score,
			0::bigint,
			0::bigint,
			0::bigint,
			0::bigint,
			p.%s * 1000,
			0,
			pps.recent_activity_at,
			COALESCE(pps.follower_count, 0),
			NULL::float8,
			NULL::float8,
			p.%s,
			p.%s,
			p.%s,
			0::bigint
		FROM profile_support_stats p
		LEFT JOIN profile_public_stats pps ON pps.pubkey = p.pubkey
		WHERE p.%s > 0
		  %s
		ORDER BY p.%s DESC, p.%s DESC, p.pubkey ASC
		LIMIT $1 OFFSET $2
	`, capped, raw, supporters, raw, capped, capped,
		fmt.Sprintf(rankedProfileExistsSQL, "p.pubkey", "p.pubkey"),
		capped, supporters)
	rows, err := s.pool.Query(ctx, query, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("get most supported profiles: %w", err)
	}
	defer rows.Close()
	return scanRankedProfiles(rows)
}

func supportWindowColumns(window time.Duration) (capped, raw, supporters string, err error) {
	switch window {
	case 24 * time.Hour:
		return "capped_sats_24h", "raw_sats_24h", "supporters_24h", nil
	case 7 * 24 * time.Hour:
		return "capped_sats_7d", "raw_sats_7d", "supporters_7d", nil
	case 30 * 24 * time.Hour:
		return "capped_sats_30d", "raw_sats_30d", "supporters_30d", nil
	default:
		return "", "", "", fmt.Errorf("unsupported trending window: %s", window)
	}
}

func scanRankedProfiles(rows interface {
	Next() bool
	Scan(...any) error
	Err() error
}) ([]TrendingProfile, error) {
	out := make([]TrendingProfile, 0)
	for rows.Next() {
		var row TrendingProfile
		var scoredEngagement, scoredFollowers sql.NullFloat64
		if err := rows.Scan(
			&row.Pubkey,
			&row.Score,
			&row.RecentPostCount,
			&row.RecentReplyCount,
			&row.RecentEngagementReceived,
			&row.RecentNewFollowers,
			&row.RecentZapVolumeMSats,
			&row.RecentActiveDays,
			&row.RecentActivityAt,
			&row.FollowerCount,
			&scoredEngagement,
			&scoredFollowers,
			&row.SupporterCount,
			&row.RawSats,
			&row.CappedSats,
			&row.TrustedFollowerCount,
		); err != nil {
			return nil, fmt.Errorf("scan ranked profile: %w", err)
		}
		row.ScoredEngagementReceived = nullFloat64Ptr(scoredEngagement)
		row.ScoredNewFollowers = nullFloat64Ptr(scoredFollowers)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read ranked profiles: %w", err)
	}
	return out, nil
}
