package read

import (
	"context"
	"fmt"
	"time"

	"github.com/xdzczk/nostrmash/internal/languagelabel"
	"github.com/xdzczk/nostrmash/internal/readmodel"
)

func (s *Read) GetCohortBookmarkedNotes(ctx context.Context, follows []string, lang string, limit int) ([]readmodel.CohortNote, error) {
	if err := s.beginCohort(follows); err != nil {
		if emptyCohort(err) {
			return nil, nil
		}
		return nil, err
	}
	limit = clampCohortLimit(limit)
	query := fmt.Sprintf(`
		WITH cohort AS (
			SELECT be.target_event_id AS event_id,
			       COUNT(DISTINCT be.bookmarker_pubkey)::float8 AS cohort_count
			FROM bookmark_edges be
			JOIN events author_event ON author_event.id = be.target_event_id
			WHERE be.bookmarker_pubkey = ANY($1::text[])
			  AND be.target_event_id <> ''
			  AND be.bookmarker_pubkey <> author_event.pubkey
			  AND author_event.kind IN (1, 30023)
			GROUP BY be.target_event_id
		)
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
			COALESCE(b.bookmark_count, 0)::float8,
			COALESCE(b.bookmark_count, 0),
			cohort.cohort_count
		FROM cohort
		JOIN events e ON e.id = cohort.event_id
		LEFT JOIN note_bookmark_stats b ON b.event_id = e.id
		LEFT JOIN note_discovery_stats s ON s.event_id = e.id
		WHERE 1 = 1
		  %s
		  %s
		ORDER BY cohort.cohort_count DESC, e.created_at DESC, e.id ASC
		LIMIT $2
	`, fmt.Sprintf(rankedProfileExistsSQL, "e.pubkey", "e.pubkey"), noteLanguageClause("s", "$3::text", lang))
	args := []any{follows, limit}
	if lang != "" {
		args = append(args, lang)
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get cohort bookmarked notes: %w", err)
	}
	defer rows.Close()
	return scanCohortCountNotes(rows)
}

func (s *Read) GetCohortZappedNotes(ctx context.Context, follows []string, window time.Duration, lang string, limit int) ([]readmodel.CohortNote, error) {
	if err := s.beginCohort(follows); err != nil {
		if emptyCohort(err) {
			return nil, nil
		}
		return nil, err
	}
	satsColumn, countColumn, err := zapWindowColumns(window)
	if err != nil {
		return nil, err
	}
	limit = clampCohortLimit(limit)
	cutoff := time.Now().UTC().Add(-window).Unix()
	query := fmt.Sprintf(`
		WITH cohort AS (
			SELECT z.event_id,
			       SUM(z.amount_sats)::float8 AS cohort_sats
			FROM zap_receipts z
			JOIN events e ON e.id = z.event_id
			WHERE z.sender_pubkey = ANY($1::text[])
			  AND z.created_at >= $2
			  AND z.sender_pubkey <> e.pubkey
			  AND e.created_at >= $2
			  AND e.kind IN (1, 30023)
			  AND NOT EXISTS (
				SELECT 1 FROM thread_edges te WHERE te.child_event_id = e.id
			  )
			GROUP BY z.event_id
		)
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
			COALESCE(%s, 0),
			COALESCE(%s, 0) * 1000,
			COALESCE(%s, 0)::float8,
			0::bigint,
			cohort.cohort_sats
		FROM cohort
		JOIN events e ON e.id = cohort.event_id
		LEFT JOIN note_zap_window_stats z ON z.event_id = e.id
		LEFT JOIN note_discovery_stats s ON s.event_id = e.id
		WHERE 1 = 1
		  %s
		  %s
		ORDER BY cohort.cohort_sats DESC, e.created_at DESC, e.id ASC
		LIMIT $3
	`, countColumn, satsColumn, satsColumn,
		fmt.Sprintf(rankedProfileExistsSQL, "e.pubkey", "e.pubkey"),
		noteLanguageClause("s", "$4::text", lang))
	args := []any{follows, cutoff, limit}
	if lang != "" {
		args = append(args, lang)
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get cohort zapped notes: %w", err)
	}
	defer rows.Close()
	return scanCohortCountNotes(rows)
}

func scanCohortCountNotes(rows interface {
	Next() bool
	Scan(...any) error
	Err() error
	Close()
}) ([]readmodel.CohortNote, error) {
	defer rows.Close()
	out := make([]readmodel.CohortNote, 0)
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
			&row.Note.BookmarkCount,
			&row.CohortScore,
		); err != nil {
			return nil, fmt.Errorf("scan cohort note: %w", err)
		}
		row.CohortEvidence = row.CohortScore
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read cohort notes: %w", err)
	}
	return out, nil
}

func (s *Read) GetCohortFollowedProfiles(ctx context.Context, viewer string, follows []string, limit int) ([]readmodel.CohortProfile, error) {
	if err := s.beginCohort(follows); err != nil {
		if emptyCohort(err) {
			return nil, nil
		}
		return nil, err
	}
	limit = clampCohortLimit(limit)
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT
			fe.followed_pubkey,
			COUNT(DISTINCT fe.follower_pubkey)::float8,
			COALESCE(pps.trusted_follower_count, 0)::float8,
			COALESCE(pps.follower_count, 0),
			pps.recent_activity_at,
			(fe.followed_pubkey = ANY($1::text[]))
		FROM follower_edges fe
		LEFT JOIN profile_public_stats pps ON pps.pubkey = fe.followed_pubkey
		WHERE fe.follower_pubkey = ANY($1::text[])
		  AND fe.followed_pubkey <> $2
		  %s
		GROUP BY fe.followed_pubkey, pps.trusted_follower_count, pps.follower_count, pps.recent_activity_at
		ORDER BY COUNT(DISTINCT fe.follower_pubkey) DESC, fe.followed_pubkey ASC
		LIMIT $3
	`, fmt.Sprintf(rankedProfileExistsSQL, "fe.followed_pubkey", "fe.followed_pubkey")), follows, viewer, limit)
	if err != nil {
		return nil, fmt.Errorf("get cohort followed profiles: %w", err)
	}
	defer rows.Close()
	out := make([]readmodel.CohortProfile, 0, limit)
	for rows.Next() {
		var row readmodel.CohortProfile
		if err := rows.Scan(
			&row.Profile.Pubkey,
			&row.CohortScore,
			&row.Profile.Score,
			&row.Profile.FollowerCount,
			&row.Profile.RecentActivityAt,
			&row.ViewerFollows,
		); err != nil {
			return nil, fmt.Errorf("scan cohort followed profile: %w", err)
		}
		row.Profile.TrustedFollowerCount = int64(row.Profile.Score)
		row.CohortEvidence = row.CohortScore
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read cohort followed profiles: %w", err)
	}
	return out, nil
}

func (s *Read) GetCohortSupportedProfiles(ctx context.Context, follows []string, window time.Duration, senderCap int64, limit int) ([]readmodel.CohortProfile, error) {
	if err := s.beginCohort(follows); err != nil {
		if emptyCohort(err) {
			return nil, nil
		}
		return nil, err
	}
	capped, raw, supporters, err := supportWindowColumns(window)
	if err != nil {
		return nil, err
	}
	if senderCap <= 0 {
		senderCap = 100000
	}
	limit = clampCohortLimit(limit)
	cutoff := time.Now().UTC().Add(-window).Unix()
	query := fmt.Sprintf(`
		WITH capped AS (
			SELECT receiver_pubkey,
			       SUM(LEAST(sender_sats, $4::bigint))::float8 AS cohort_sats
			FROM (
				SELECT receiver_pubkey, sender_pubkey, SUM(amount_sats) AS sender_sats
				FROM zap_receipts
				WHERE sender_pubkey = ANY($1::text[])
				  AND created_at >= $2
				  AND receiver_pubkey IS NOT NULL
				  AND sender_pubkey <> receiver_pubkey
				GROUP BY receiver_pubkey, sender_pubkey
			) senders
			GROUP BY receiver_pubkey
		)
		SELECT
			capped.receiver_pubkey,
			capped.cohort_sats,
			COALESCE(p.%s, 0)::float8,
			COALESCE(p.%s, 0),
			COALESCE(p.%s, 0),
			COALESCE(pps.follower_count, 0),
			pps.recent_activity_at
		FROM capped
		LEFT JOIN profile_support_stats p ON p.pubkey = capped.receiver_pubkey
		LEFT JOIN profile_public_stats pps ON pps.pubkey = capped.receiver_pubkey
		WHERE 1 = 1
		  %s
		ORDER BY capped.cohort_sats DESC, capped.receiver_pubkey ASC
		LIMIT $3
	`, capped, raw, supporters, fmt.Sprintf(rankedProfileExistsSQL, "capped.receiver_pubkey", "capped.receiver_pubkey"))
	rows, err := s.pool.Query(ctx, query, follows, cutoff, limit, senderCap)
	if err != nil {
		return nil, fmt.Errorf("get cohort supported profiles: %w", err)
	}
	defer rows.Close()
	out := make([]readmodel.CohortProfile, 0, limit)
	for rows.Next() {
		var row readmodel.CohortProfile
		if err := rows.Scan(
			&row.Profile.Pubkey,
			&row.CohortScore,
			&row.Profile.Score,
			&row.Profile.RawSats,
			&row.Profile.SupporterCount,
			&row.Profile.FollowerCount,
			&row.Profile.RecentActivityAt,
		); err != nil {
			return nil, fmt.Errorf("scan cohort supported profile: %w", err)
		}
		row.Profile.CappedSats = int64(row.Profile.Score)
		row.Profile.RecentZapVolumeMSats = row.Profile.RawSats * 1000
		row.CohortEvidence = row.CohortScore
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read cohort supported profiles: %w", err)
	}
	return out, nil
}

func (s *Read) GetCohortTrendingHashtags(ctx context.Context, follows []string, window time.Duration, lang string, limit int) ([]readmodel.TrendingHashtag, error) {
	if err := s.beginCohort(follows); err != nil {
		if emptyCohort(err) {
			return nil, nil
		}
		return nil, err
	}
	if window <= 0 {
		return nil, fmt.Errorf("window must be positive")
	}
	limit = clampCohortLimit(limit)
	cutoff := time.Now().UTC().Add(-window).Unix()
	args := []any{follows, cutoff, limit}
	query := `
		SELECT hashtag,
		       COUNT(DISTINCT event_id),
		       COUNT(DISTINCT author_pubkey)
		FROM event_hashtags
		WHERE author_pubkey = ANY($1::text[])
		  AND created_at >= $2
		  AND hashtag !~ '^[0-9]+$'
		GROUP BY hashtag
		ORDER BY COUNT(DISTINCT author_pubkey) DESC, COUNT(DISTINCT event_id) DESC, hashtag ASC
		LIMIT $3
	`
	if lang != "" {
		args = append(args, lang)
		query = `
		SELECT event_hashtags.hashtag,
		       COUNT(DISTINCT event_hashtags.event_id),
		       COUNT(DISTINCT event_hashtags.author_pubkey)
		FROM event_hashtags
		JOIN note_discovery_stats nds ON nds.event_id = event_hashtags.event_id
		WHERE event_hashtags.author_pubkey = ANY($1::text[])
		  AND event_hashtags.created_at >= $2
		  AND event_hashtags.hashtag !~ '^[0-9]+$'
		  AND ` + languagelabel.FilterSQL("nds", "$4::text") + `
		GROUP BY event_hashtags.hashtag
		ORDER BY COUNT(DISTINCT event_hashtags.author_pubkey) DESC,
		         COUNT(DISTINCT event_hashtags.event_id) DESC,
		         event_hashtags.hashtag ASC
		LIMIT $3
	`
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get cohort trending hashtags: %w", err)
	}
	defer rows.Close()
	out := make([]readmodel.TrendingHashtag, 0, limit)
	for rows.Next() {
		var row readmodel.TrendingHashtag
		if err := rows.Scan(&row.Hashtag, &row.EventCount, &row.UniqueAuthors); err != nil {
			return nil, fmt.Errorf("scan cohort hashtag: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read cohort hashtags: %w", err)
	}
	return out, nil
}

func (s *Read) GetCohortTrendingDomains(ctx context.Context, follows []string, window time.Duration, lang string, limit int) ([]readmodel.CohortDomain, error) {
	if err := s.beginCohort(follows); err != nil {
		if emptyCohort(err) {
			return nil, nil
		}
		return nil, err
	}
	if window <= 0 {
		return nil, fmt.Errorf("window must be positive")
	}
	limit = clampCohortLimit(limit)
	cutoff := time.Now().UTC().Add(-window).Unix()
	args := []any{follows, cutoff, limit}
	languageJoin := ""
	languageClause := ""
	if lang != "" {
		languageJoin = "JOIN note_discovery_stats nds ON nds.event_id = event_urls.event_id"
		languageClause = "AND " + languagelabel.FilterSQL("nds", "$4::text")
		args = append(args, lang)
	}
	query := fmt.Sprintf(`
		SELECT event_urls.canonical_domain,
		       MAX(event_urls.created_at),
		       COUNT(*),
		       COUNT(DISTINCT event_urls.event_id),
		       COUNT(DISTINCT event_urls.author_pubkey)
		FROM event_urls
		%s
		WHERE event_urls.author_pubkey = ANY($1::text[])
		  AND event_urls.created_at >= $2
		  AND %s
		  %s
		GROUP BY event_urls.canonical_domain
		ORDER BY COUNT(DISTINCT event_urls.author_pubkey) DESC,
		         COUNT(DISTINCT event_urls.event_id) DESC,
		         event_urls.canonical_domain ASC
		LIMIT $3
	`, languageJoin, domainMediaURLFilterClause, languageClause)
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get cohort trending domains: %w", err)
	}
	defer rows.Close()
	out := make([]readmodel.CohortDomain, 0, limit)
	for rows.Next() {
		var row readmodel.CohortDomain
		if err := rows.Scan(&row.Domain, &row.LatestEventAt, &row.LinkCount, &row.NoteCount, &row.UniqueAuthors); err != nil {
			return nil, fmt.Errorf("scan cohort domain: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read cohort domains: %w", err)
	}
	return out, nil
}
