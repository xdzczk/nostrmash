package read

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/xdzczk/nostrmash/internal/languagelabel"
)

// GetTrendingNotes returns trending short notes (kind 1 only). Long-form
// articles (kind 30023) are served separately by GetTrendingLongForm so the two
// surfaces never blend together.
func (s *Read) GetTrendingNotes(ctx context.Context, window time.Duration, lang string, limit int, offset int) ([]TrendingNote, error) {
	return s.getTrendingNotesForKinds(ctx, []int{1}, window, lang, limit, offset)
}

// GetTrendingLongForm returns trending long-form articles (kind 30023). It
// mirrors GetTrendingNotes but scopes the candidate set to articles.
func (s *Read) GetTrendingLongForm(ctx context.Context, window time.Duration, lang string, limit int, offset int) ([]TrendingNote, error) {
	return s.getTrendingNotesForKinds(ctx, []int{30023}, window, lang, limit, offset)
}

func (s *Read) getTrendingNotesForKinds(ctx context.Context, kinds []int, window time.Duration, lang string, limit int, offset int) ([]TrendingNote, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	scoreColumn, windowDuration, err := resolveTrendingWindow(window)
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
	minCreatedAt := time.Now().UTC().Add(-windowDuration).Unix()
	// Kind 1 is almost the entire score index, so that surface keeps walking
	// idx_note_discovery_stats_{24h,7d}_rank and filtering kind afterwards.
	// Long-form is a few thousand rows. Driving it from the same score index
	// scans every note in the window (and a profile lookup per note) and
	// trips the 15s statement_timeout. The handler then caches that failed
	// empty page. idx_events_kind_created already ranges kind 30023 by time,
	// so articles start there and only then join discovery stats.
	var rows pgx.Rows
	if longFormOnly(kinds) {
		query := fmt.Sprintf(`
			SELECT
				s.event_id,
				s.author_pubkey,
				s.created_at,
				e.content,
				`+languagelabel.ChartSQL("s.primary_language", "s.language_source")+` AS language,
				s.reply_count,
				s.repost_count,
				s.reaction_count,
				s.zap_count,
				s.zap_msats,
				%s AS score
			FROM events e
			JOIN note_discovery_stats s ON s.event_id = e.id
			WHERE e.kind = 30023
			  AND e.created_at >= $1
			  AND s.created_at >= $1
			  AND NOT EXISTS (
				SELECT 1
				FROM thread_edges te
				WHERE te.child_event_id = e.id
			  )
			  AND (
				EXISTS (
					SELECT 1
					FROM profiles_latest pl
					WHERE pl.pubkey = s.author_pubkey
				)
				OR EXISTS (
					SELECT 1
					FROM events metadata
					WHERE metadata.pubkey = s.author_pubkey
					  AND metadata.kind = 0
				)
			  )
			  %s
			ORDER BY score DESC, s.created_at DESC, s.event_id ASC
			LIMIT $2 OFFSET $3
		`, scoreColumn, noteLanguageClause("s", "$4::text", lang))
		args := []any{minCreatedAt, limit, offset}
		if lang != "" {
			args = append(args, lang)
		}
		rows, err = s.pool.Query(ctx, query, args...)
	} else {
		query := fmt.Sprintf(`
			SELECT
				s.event_id,
				s.author_pubkey,
				s.created_at,
				e.content,
				`+languagelabel.ChartSQL("s.primary_language", "s.language_source")+` AS language,
				s.reply_count,
				s.repost_count,
				s.reaction_count,
				s.zap_count,
				s.zap_msats,
				%s AS score
			FROM note_discovery_stats s
			JOIN events e ON e.id = s.event_id
			WHERE s.created_at >= $1
			  AND e.kind = ANY($4::int[])
			  AND NOT EXISTS (
				SELECT 1
				FROM thread_edges te
				WHERE te.child_event_id = s.event_id
			  )
			  AND (
				EXISTS (
					SELECT 1
					FROM profiles_latest pl
					WHERE pl.pubkey = s.author_pubkey
				)
				OR EXISTS (
					SELECT 1
					FROM events metadata
					WHERE metadata.pubkey = s.author_pubkey
					  AND metadata.kind = 0
				)
			  )
			  %s
			ORDER BY score DESC, s.created_at DESC, s.event_id ASC
			LIMIT $2 OFFSET $3
		`, scoreColumn, noteLanguageClause("s", "$5::text", lang))
		args := []any{minCreatedAt, limit, offset, kinds}
		if lang != "" {
			args = append(args, lang)
		}
		rows, err = s.pool.Query(ctx, query, args...)
	}
	if err != nil {
		return nil, fmt.Errorf("get trending notes: %w", err)
	}
	defer rows.Close()

	out := make([]TrendingNote, 0, limit)
	for rows.Next() {
		var row TrendingNote
		if err := rows.Scan(
			&row.EventID,
			&row.AuthorPubkey,
			&row.CreatedAt,
			&row.Content,
			&row.Language,
			&row.ReplyCount,
			&row.RepostCount,
			&row.ReactionCount,
			&row.ZapCount,
			&row.ZapMSats,
			&row.Score,
		); err != nil {
			return nil, fmt.Errorf("scan trending note row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read trending note rows: %w", err)
	}
	return out, nil
}

func noteLanguageClause(alias, placeholder, lang string) string {
	if strings.TrimSpace(lang) == "" {
		return ""
	}
	return "AND " + languagelabel.FilterSQL(alias, placeholder)
}

func longFormOnly(kinds []int) bool {
	return len(kinds) == 1 && kinds[0] == 30023
}

func resolveTrendingWindow(window time.Duration) (string, time.Duration, error) {
	switch window {
	case 24 * time.Hour:
		return "score_24h", 24 * time.Hour, nil
	case 7 * 24 * time.Hour:
		return "score_7d", 7 * 24 * time.Hour, nil
	default:
		return "", 0, fmt.Errorf("unsupported trending window: %s", strings.TrimSpace(window.String()))
	}
}

// profileScoredInputColumns picks the scored engagement / new-follower
// columns that match the requested ranking window so "why now" reasons
// cite the same inputs the score used.
func profileLanguageColumn(scoreColumn string) string {
	if strings.Contains(scoreColumn, "7d") {
		return "primary_language_7d"
	}
	return "primary_language_24h"
}

func profileScoredInputColumns(scoreColumn string) (engagementCol, newFollowersCol string) {
	if strings.Contains(scoreColumn, "7d") {
		return "scored_engagement_7d", "scored_new_followers_7d"
	}
	return "scored_engagement_24h", "scored_new_followers_24h"
}
