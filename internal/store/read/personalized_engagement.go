package read

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/xdzczk/nostrmash/internal/languagelabel"
	"github.com/xdzczk/nostrmash/internal/readmodel"
)

func (s *Read) GetCohortTrendingNotes(ctx context.Context, follows []string, kinds []int, window time.Duration, lang string, limit int) ([]readmodel.CohortNote, error) {
	if err := s.beginCohort(follows); err != nil {
		if emptyCohort(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(kinds) == 0 {
		return nil, fmt.Errorf("kinds are required")
	}
	scoreColumn, windowDuration, err := resolveTrendingWindow(window)
	if err != nil {
		return nil, err
	}
	limit = clampCohortLimit(limit)
	cutoff := time.Now().UTC().Add(-windowDuration).Unix()
	// target_pubkey is the engaged note's author, written at projection.
	// Comparing it here avoids joining events once per reaction. Kind, root,
	// and profile checks run on the shortlist, and content is read only for
	// the page that survives them.
	query := fmt.Sprintf(`
		WITH follows AS (
			SELECT unnest($1::text[]) AS pubkey
		),
		parts AS (
			SELECT r.target_event_id AS event_id,
			       0::float8 AS reply_weight,
			       0::float8 AS repost_weight,
			       COUNT(DISTINCT r.reactor_pubkey)::float8 AS reaction_weight,
			       0::float8 AS zap_weight,
			       0::float8 AS zap_msats
			FROM follows f
			JOIN reaction_events r
			  ON r.reactor_pubkey = f.pubkey
			 AND r.created_at >= $2
			WHERE r.target_pubkey IS NOT NULL
			  AND r.reactor_pubkey <> r.target_pubkey
			GROUP BY r.target_event_id
			UNION ALL
			SELECT p.target_event_id,
			       0::float8,
			       COUNT(DISTINCT p.reposter_pubkey)::float8,
			       0::float8,
			       0::float8,
			       0::float8
			FROM follows f
			JOIN repost_events p
			  ON p.reposter_pubkey = f.pubkey
			 AND p.created_at >= $2
			WHERE p.target_pubkey IS NOT NULL
			  AND p.reposter_pubkey <> p.target_pubkey
			GROUP BY p.target_event_id
			UNION ALL
			SELECT c.target_event_id,
			       COUNT(DISTINCT c.source_pubkey)::float8,
			       0::float8,
			       0::float8,
			       0::float8,
			       0::float8
			FROM follows f
			JOIN reply_count_contributions c
			  ON c.source_pubkey = f.pubkey
			 AND c.source_created_at >= $2
			WHERE c.target_pubkey IS NOT NULL
			  AND c.source_pubkey <> c.target_pubkey
			GROUP BY c.target_event_id
			UNION ALL
			SELECT z.event_id,
			       0::float8,
			       0::float8,
			       0::float8,
			       COUNT(DISTINCT z.sender_pubkey)::float8,
			       SUM(z.amount_sats)::float8 * 1000
			FROM follows f
			JOIN zap_receipts z
			  ON z.sender_pubkey = f.pubkey
			 AND z.created_at >= $2
			JOIN events te ON te.id = z.event_id
			WHERE z.sender_pubkey <> te.pubkey
			GROUP BY z.event_id
		),
		agg AS (
			SELECT event_id,
			       SUM(reply_weight) AS reply_weight,
			       SUM(repost_weight) AS repost_weight,
			       SUM(reaction_weight) AS reaction_weight,
			       SUM(zap_weight) AS zap_weight,
			       SUM(zap_msats) AS zap_msats
			FROM parts
			GROUP BY event_id
		),
		shortlist AS (
			SELECT event_id, reply_weight, repost_weight, reaction_weight, zap_weight, zap_msats
			FROM agg
			ORDER BY (reply_weight + repost_weight + reaction_weight + zap_weight) DESC, event_id ASC
			LIMIT $5
		),
		kept AS (
			SELECT
				shortlist.event_id,
				shortlist.reply_weight,
				shortlist.repost_weight,
				shortlist.reaction_weight,
				shortlist.zap_weight,
				shortlist.zap_msats
			FROM shortlist
			JOIN events e ON e.id = shortlist.event_id
			JOIN note_discovery_stats s ON s.event_id = e.id
			WHERE e.kind = ANY($4::int[])
			  AND s.created_at >= $2
			  AND NOT EXISTS (
				SELECT 1 FROM thread_edges te WHERE te.child_event_id = e.id
			  )
			  AND (
				EXISTS (SELECT 1 FROM profiles_latest pl WHERE pl.pubkey = s.author_pubkey)
				OR EXISTS (
					SELECT 1 FROM events metadata
					WHERE metadata.pubkey = s.author_pubkey AND metadata.kind = 0
				)
			  )
			  %s
			ORDER BY (shortlist.reply_weight + shortlist.repost_weight + shortlist.reaction_weight + shortlist.zap_weight) DESC,
			         e.created_at DESC, e.id ASC
			LIMIT $3
		)
		SELECT
			e.id,
			e.pubkey,
			e.created_at,
			e.content,
			e.kind,
			`+languagelabel.ChartSQL("s.primary_language", "s.language_source")+` AS language,
			s.reply_count,
			s.repost_count,
			s.reaction_count,
			s.zap_count,
			s.zap_msats,
			COALESCE(s.%s, 0),
			kept.reply_weight,
			kept.repost_weight,
			kept.reaction_weight,
			kept.zap_weight,
			kept.zap_msats
		FROM kept
		JOIN events e ON e.id = kept.event_id
		JOIN note_discovery_stats s ON s.event_id = kept.event_id
		ORDER BY (kept.reply_weight + kept.repost_weight + kept.reaction_weight + kept.zap_weight) DESC,
		         e.created_at DESC, e.id ASC
	`, noteLanguageClause("s", "$6::text", lang), scoreColumn)
	args := []any{follows, cutoff, limit, kinds, cohortShortlist(limit, 20, 8000)}
	if lang != "" {
		args = append(args, lang)
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get cohort trending notes: %w", err)
	}
	defer rows.Close()
	out := make([]readmodel.CohortNote, 0, limit)
	for rows.Next() {
		var row readmodel.CohortNote
		if err := rows.Scan(
			&row.Note.EventID,
			&row.Note.AuthorPubkey,
			&row.Note.CreatedAt,
			&row.Note.Content,
			&row.Note.Kind,
			&row.Note.Language,
			&row.Note.ReplyCount,
			&row.Note.RepostCount,
			&row.Note.ReactionCount,
			&row.Note.ZapCount,
			&row.Note.ZapMSats,
			&row.Note.Score,
			&row.ReplyWeight,
			&row.RepostWeight,
			&row.ReactionWeight,
			&row.ZapWeight,
			&row.ZapMSats,
		); err != nil {
			return nil, fmt.Errorf("scan cohort trending note: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read cohort trending notes: %w", err)
	}
	return out, nil
}

func (s *Read) GetCohortHotConversations(ctx context.Context, follows []string, window time.Duration, lang string, limit int) ([]readmodel.CohortConversation, error) {
	if err := s.beginCohort(follows); err != nil {
		if emptyCohort(err) {
			return nil, nil
		}
		return nil, err
	}
	repliesColumn, weightColumn, windowDuration, err := resolveConversationWindow(window)
	if err != nil {
		return nil, err
	}
	authorsColumn := "authors_24h"
	switch windowDuration {
	case 7 * 24 * time.Hour:
		authorsColumn = "authors_7d"
	case 30 * 24 * time.Hour:
		authorsColumn = "authors_30d"
	}
	limit = clampCohortLimit(limit)
	cutoff := time.Now().UTC().Add(-windowDuration).Unix()
	query := fmt.Sprintf(`
		WITH follows AS (
			SELECT unnest($1::text[]) AS pubkey
		),
		cohort AS (
			SELECT te.root_event_id,
			       COUNT(*)::float8 AS cohort_replies,
			       COUNT(DISTINCT child.pubkey)::float8 AS cohort_participants
			FROM follows f
			JOIN events child
			  ON child.pubkey = f.pubkey
			 AND child.created_at >= $2
			JOIN thread_edges te ON te.child_event_id = child.id
			JOIN events root ON root.id = te.root_event_id
			WHERE te.root_event_id IS NOT NULL
			  AND child.pubkey <> root.pubkey
			  AND root.kind = 1
			GROUP BY te.root_event_id
		)
		SELECT
			e.id,
			e.pubkey,
			e.created_at,
			e.content,
			COALESCE(ts.reply_count, 0),
			COALESCE(nds.repost_count, 0),
			COALESCE(nds.reaction_count, 0),
			COALESCE(nds.zap_count, 0),
			COALESCE(nds.zap_msats, 0),
			COALESCE(ts.participant_count, 1),
			COALESCE(ts.last_activity_at, e.created_at),
			COALESCE(ts.replies_24h, 0),
			COALESCE(ts.replies_7d, 0),
			COALESCE(ts.replies_30d, 0),
			COALESCE(ts.derivation_version, 0),
			COALESCE(ts.%s, 0),
			COALESCE(ts.%s, 0),
			COALESCE(ts.%s, 0),
			cohort.cohort_replies,
			cohort.cohort_participants
		FROM cohort
		JOIN events e ON e.id = cohort.root_event_id
		LEFT JOIN thread_summaries ts ON ts.root_event_id = cohort.root_event_id
		LEFT JOIN note_discovery_stats nds ON nds.event_id = cohort.root_event_id
		WHERE 1 = 1
		  %s
		ORDER BY cohort.cohort_replies DESC, e.id ASC
		LIMIT $3
	`, repliesColumn, weightColumn, authorsColumn, noteLanguageClause("nds", "$4::text", lang))
	args := []any{follows, cutoff, limit}
	if lang != "" {
		args = append(args, lang)
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get cohort hot conversations: %w", err)
	}
	defer rows.Close()
	out := make([]readmodel.CohortConversation, 0, limit)
	for rows.Next() {
		var row readmodel.CohortConversation
		var version int
		var replies int64
		var authors int
		var weight float64
		row.Conversation.Consistency = "eventual"
		if err := rows.Scan(
			&row.Conversation.RootEventID,
			&row.Conversation.AuthorPubkey,
			&row.Conversation.CreatedAt,
			&row.Conversation.Content,
			&row.Conversation.ReplyCount,
			&row.Conversation.RepostCount,
			&row.Conversation.ReactionCount,
			&row.Conversation.ZapCount,
			&row.Conversation.ZapMSats,
			&row.Conversation.ParticipantCount,
			&row.Conversation.LastActivityAt,
			&row.Conversation.Replies24h,
			&row.Conversation.Replies7d,
			&row.Conversation.Replies30d,
			&version,
			&replies,
			&weight,
			&authors,
			&row.CohortReplies,
			&row.CohortParticipants,
		); err != nil {
			return nil, fmt.Errorf("scan cohort conversation: %w", err)
		}
		row.Conversation.VelocityScore = cohortConversationVelocity(windowDuration, version, row.Conversation.ParticipantCount, replies, authors, weight)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read cohort conversations: %w", err)
	}
	return out, nil
}

func cohortConversationVelocity(window time.Duration, version int, participants int, replies int64, authors int, weight float64) float64 {
	broad := weight
	if version < 2 {
		broad = float64(replies)
	}
	broad = broad + math.Min(broad, conversationBoostCap)*conversationReplyBoost
	authorVersion := conversationWindowAuthorVersion
	if window == 30*24*time.Hour {
		authorVersion = 5
	}
	authorCount := participants
	if version >= authorVersion && authors > 0 {
		authorCount = authors
	}
	if authorCount <= 0 {
		authorCount = 1
	}
	return conversationVelocity(authorCount, replies, broad)
}

func (s *Read) GetCohortTrendingProfiles(ctx context.Context, follows []string, window time.Duration, lang string, limit int) ([]readmodel.CohortProfile, error) {
	return s.cohortProfiles(ctx, follows, window, lang, limit, false)
}

func (s *Read) GetCohortRisingProfiles(ctx context.Context, viewer string, follows []string, window time.Duration, lang string, limit int) ([]readmodel.CohortProfile, error) {
	if err := s.beginCohort(follows); err != nil {
		if emptyCohort(err) {
			return nil, nil
		}
		return nil, err
	}
	scoreColumn, windowDuration, err := profileWindowScoreColumn(window, true)
	if err != nil {
		return nil, err
	}
	limit = clampCohortLimit(limit)
	cutoff := time.Now().UTC().Add(-windowDuration).Unix()
	langClause := ""
	args := []any{follows, cutoff, viewer, limit, cohortShortlist(limit, 8, 2500)}
	if lang != "" {
		langClause = fmt.Sprintf("AND p.%s = $6", profileLanguageColumn(scoreColumn))
		args = append(args, lang)
	}
	query := fmt.Sprintf(`
		WITH follows AS (
			SELECT unnest($1::text[]) AS pubkey
		),
		cohort AS (
			SELECT g.followed_pubkey AS pubkey, COUNT(*)::float8 AS cohort_count
			FROM follows f
			JOIN follower_gain_events g
			  ON g.follower_pubkey = f.pubkey
			 AND g.gained_at >= $2
			WHERE g.followed_pubkey <> $3
			GROUP BY g.followed_pubkey
		),
		shortlist AS (
			SELECT pubkey, cohort_count
			FROM cohort
			ORDER BY cohort_count DESC, pubkey ASC
			LIMIT $5
		)
		SELECT
			shortlist.pubkey,
			shortlist.cohort_count,
			COALESCE(p.%s, 0),
			COALESCE(p.recent_post_count, 0),
			COALESCE(p.recent_reply_count, 0),
			COALESCE(p.recent_engagement_received, 0),
			COALESCE(p.recent_zap_volume_msats, 0),
			COALESCE(p.recent_active_days, 0),
			p.recent_activity_at,
			COALESCE(pps.follower_count, 0)
		FROM shortlist
		JOIN profiles_latest pl ON pl.pubkey = shortlist.pubkey
		LEFT JOIN profile_discovery_stats p ON p.pubkey = shortlist.pubkey
		LEFT JOIN profile_public_stats pps ON pps.pubkey = shortlist.pubkey
		WHERE 1 = 1
		  %s
		ORDER BY shortlist.cohort_count DESC, shortlist.pubkey ASC
		LIMIT $4
	`, scoreColumn, langClause)
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get cohort rising profiles: %w", err)
	}
	defer rows.Close()
	return scanCohortProfiles(rows)
}

func (s *Read) cohortProfiles(ctx context.Context, follows []string, window time.Duration, lang string, limit int, _ bool) ([]readmodel.CohortProfile, error) {
	if err := s.beginCohort(follows); err != nil {
		if emptyCohort(err) {
			return nil, nil
		}
		return nil, err
	}
	scoreColumn, windowDuration, err := profileWindowScoreColumn(window, false)
	if err != nil {
		return nil, err
	}
	limit = clampCohortLimit(limit)
	cutoff := time.Now().UTC().Add(-windowDuration).Unix()
	langClause := ""
	args := []any{follows, cutoff, limit, cohortShortlist(limit, 8, 2500)}
	if lang != "" {
		langClause = fmt.Sprintf("AND p.%s = $5", profileLanguageColumn(scoreColumn))
		args = append(args, lang)
	}
	rootFilter, err := s.cohortRootNoteFilter(ctx, "shortlist.pubkey")
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf(`
		WITH follows AS (
			SELECT unnest($1::text[]) AS pubkey
		),
		parts AS (
			SELECT r.target_pubkey AS pubkey, COUNT(DISTINCT r.reactor_pubkey)::float8 AS weight
			FROM follows f
			JOIN reaction_events r
			  ON r.reactor_pubkey = f.pubkey
			 AND r.created_at >= $2
			WHERE r.target_pubkey IS NOT NULL
			  AND r.reactor_pubkey <> r.target_pubkey
			GROUP BY r.target_pubkey
			UNION ALL
			SELECT p.target_pubkey, COUNT(DISTINCT p.reposter_pubkey)::float8
			FROM follows f
			JOIN repost_events p
			  ON p.reposter_pubkey = f.pubkey
			 AND p.created_at >= $2
			WHERE p.target_pubkey IS NOT NULL
			  AND p.reposter_pubkey <> p.target_pubkey
			GROUP BY p.target_pubkey
			UNION ALL
			SELECT c.target_pubkey, COUNT(DISTINCT c.source_pubkey)::float8
			FROM follows f
			JOIN reply_count_contributions c
			  ON c.source_pubkey = f.pubkey
			 AND c.source_created_at >= $2
			WHERE c.target_pubkey IS NOT NULL
			  AND c.source_pubkey <> c.target_pubkey
			GROUP BY c.target_pubkey
			UNION ALL
			SELECT z.receiver_pubkey, COUNT(DISTINCT z.sender_pubkey)::float8
			FROM follows f
			JOIN zap_receipts z
			  ON z.sender_pubkey = f.pubkey
			 AND z.created_at >= $2
			WHERE z.receiver_pubkey IS NOT NULL
			  AND z.sender_pubkey <> z.receiver_pubkey
			GROUP BY z.receiver_pubkey
		),
		cohort AS (
			SELECT pubkey, SUM(weight) AS weight
			FROM parts
			GROUP BY pubkey
		),
		shortlist AS (
			SELECT pubkey, weight
			FROM cohort
			ORDER BY weight DESC, pubkey ASC
			LIMIT $4
		)
		SELECT
			shortlist.pubkey,
			shortlist.weight,
			COALESCE(p.%s, 0),
			COALESCE(p.recent_post_count, 0),
			COALESCE(p.recent_reply_count, 0),
			COALESCE(p.recent_engagement_received, 0),
			COALESCE(p.recent_zap_volume_msats, 0),
			COALESCE(p.recent_active_days, 0),
			p.recent_activity_at,
			COALESCE(pps.follower_count, 0)
		FROM shortlist
		JOIN profiles_latest pl ON pl.pubkey = shortlist.pubkey
		LEFT JOIN profile_discovery_stats p ON p.pubkey = shortlist.pubkey
		LEFT JOIN profile_public_stats pps ON pps.pubkey = shortlist.pubkey
		WHERE %s
		  %s
		ORDER BY shortlist.weight DESC, shortlist.pubkey ASC
		LIMIT $3
	`, scoreColumn, rootFilter, langClause)
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get cohort trending profiles: %w", err)
	}
	defer rows.Close()
	return scanCohortProfiles(rows)
}

// cohortRootNoteFilter is true when pubkeyExpr published a root kind-1 note
// inside the window. $2 is the cutoff. After the follows-feed backfill, that
// is one index probe. Before it, the events probe stays, limited to the
// shortlist the caller already sliced.
func (s *Read) cohortRootNoteFilter(ctx context.Context, pubkeyExpr string) (string, error) {
	var complete bool
	if err := s.pool.QueryRow(ctx, `SELECT complete FROM author_feed_index_state WHERE id`).Scan(&complete); err != nil {
		return "", fmt.Errorf("author feed index state: %w", err)
	}
	if complete {
		return fmt.Sprintf(`EXISTS (
			SELECT 1 FROM author_feed_events feed
			WHERE feed.pubkey = %s AND feed.kind = 1 AND feed.created_at >= $2
		)`, pubkeyExpr), nil
	}
	return fmt.Sprintf(`EXISTS (
		SELECT 1 FROM events root_note
		WHERE root_note.pubkey = %s
		  AND root_note.kind = 1
		  AND root_note.created_at >= $2
		  AND NOT EXISTS (
			SELECT 1 FROM thread_edges te WHERE te.child_event_id = root_note.id
		  )
	)`, pubkeyExpr), nil
}

func profileWindowScoreColumn(window time.Duration, rising bool) (string, time.Duration, error) {
	column, duration, err := resolveTrendingWindow(window)
	if err != nil {
		return "", 0, err
	}
	if rising {
		switch column {
		case "score_24h":
			column = "rising_score_24h"
		case "score_7d":
			column = "rising_score_7d"
		case "score_30d":
			column = "rising_score_30d"
		}
	}
	return column, duration, nil
}

func scanCohortProfiles(rows interface {
	Next() bool
	Scan(...any) error
	Err() error
}) ([]readmodel.CohortProfile, error) {
	out := make([]readmodel.CohortProfile, 0)
	for rows.Next() {
		var row readmodel.CohortProfile
		if err := rows.Scan(
			&row.Profile.Pubkey,
			&row.CohortScore,
			&row.Profile.Score,
			&row.Profile.RecentPostCount,
			&row.Profile.RecentReplyCount,
			&row.Profile.RecentEngagementReceived,
			&row.Profile.RecentZapVolumeMSats,
			&row.Profile.RecentActiveDays,
			&row.Profile.RecentActivityAt,
			&row.Profile.FollowerCount,
		); err != nil {
			return nil, fmt.Errorf("scan cohort profile: %w", err)
		}
		row.CohortEvidence = row.CohortScore
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read cohort profiles: %w", err)
	}
	return out, nil
}
