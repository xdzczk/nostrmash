package read

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/xdzczk/nostrmash/internal/domainnorm"
)

const domainMediaURLFilterClause = `NOT (url ~* '\.(png|jpe?g|gif|webp|svg|bmp|ico|tiff?|avif|heic|mp4|mov|webm|m4v|avi|mkv|wmv|flv|mp3|wav|ogg|m4a|flac|aac|opus)(\?|#|$)')`

func (s *Read) GetEventLinkedDomains(
	ctx context.Context,
	eventID string,
	limit int,
) ([]EventDomainLinkProjection, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, fmt.Errorf("event id is required")
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	rows, err := s.pool.Query(ctx, `
		SELECT event_id, url, domain
		FROM event_urls
		WHERE event_id = $1
		ORDER BY domain ASC, url ASC
		LIMIT $2
	`, eventID, limit)
	if err != nil {
		return nil, fmt.Errorf("get event linked domains: %w", err)
	}
	defer rows.Close()

	out := make([]EventDomainLinkProjection, 0, limit)
	for rows.Next() {
		var row EventDomainLinkProjection
		if err := rows.Scan(&row.EventID, &row.URL, &row.Domain); err != nil {
			return nil, fmt.Errorf("scan event linked domain row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read event linked domain rows: %w", err)
	}
	return out, nil
}

func (s *Read) GetTopDomainsByAuthor(
	ctx context.Context,
	pubkey string,
	window time.Duration,
	limit int,
	offset int,
) ([]DomainStatProjection, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	return s.getTopDomains(ctx, window, limit, offset, &pubkey)
}

func (s *Read) GetTopDomains(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
) ([]DomainStatProjection, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	return s.getTopDomains(ctx, window, limit, offset, nil)
}

func (s *Read) getTopDomains(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
	pubkey *string,
) ([]DomainStatProjection, error) {
	if window <= 0 {
		return nil, fmt.Errorf("window must be positive")
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
	minCreatedAt := time.Now().UTC().Add(-window).Unix()
	if pubkey == nil {
		query := fmt.Sprintf(`
			SELECT
				domain,
				COUNT(*)::bigint AS link_count,
				COUNT(DISTINCT event_id)::bigint AS note_count,
				COUNT(DISTINCT author_pubkey)::bigint AS unique_authors
			FROM event_urls
			WHERE created_at >= $1
			  AND %s
			GROUP BY domain
			ORDER BY
				unique_authors DESC,
				(COUNT(DISTINCT author_pubkey))::double precision / GREATEST(COUNT(DISTINCT event_id), 1) DESC,
				note_count DESC,
				link_count DESC,
				domain ASC
			LIMIT $2 OFFSET $3
		`, domainMediaURLFilterClause)
		rows, err := s.pool.Query(ctx, query, minCreatedAt, limit, offset)
		if err != nil {
			return nil, fmt.Errorf("get top discovery domains: %w", err)
		}
		defer rows.Close()
		return scanDomainStatRows(rows)
	}
	query := fmt.Sprintf(`
		SELECT
			domain,
			COUNT(*)::bigint AS link_count,
			COUNT(DISTINCT event_id)::bigint AS note_count,
			COUNT(DISTINCT author_pubkey)::bigint AS unique_authors
		FROM event_urls
		WHERE author_pubkey = $1
		  AND created_at >= $2
		  AND %s
		GROUP BY domain
		ORDER BY
			note_count DESC,
			link_count DESC,
			domain ASC
		LIMIT $3 OFFSET $4
	`, domainMediaURLFilterClause)
	rows, err := s.pool.Query(ctx, query, *pubkey, minCreatedAt, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("get top domains by author: %w", err)
	}
	defer rows.Close()
	return scanDomainStatRows(rows)
}

func (s *Read) GetTrendingDomains(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
) ([]DomainSummaryProjection, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if window <= 0 {
		return nil, fmt.Errorf("window must be positive")
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

	nowUnix := time.Now().UTC().Unix()
	day := int64((24 * time.Hour) / time.Second)
	windowFloor := nowUnix - int64(window/time.Second)
	query := fmt.Sprintf(`
		SELECT
			domain,
			MAX(created_at),
			COUNT(*) FILTER (WHERE created_at >= $1),
			COUNT(DISTINCT event_id) FILTER (WHERE created_at >= $1),
			COUNT(DISTINCT author_pubkey) FILTER (WHERE created_at >= $1),
			COUNT(*) FILTER (WHERE created_at >= $2),
			COUNT(DISTINCT event_id) FILTER (WHERE created_at >= $2),
			COUNT(DISTINCT author_pubkey) FILTER (WHERE created_at >= $2)
		FROM event_urls
		WHERE created_at >= $3
		  AND %s
		GROUP BY domain
		ORDER BY
			COUNT(DISTINCT author_pubkey) FILTER (WHERE created_at >= $3) DESC,
			(COUNT(DISTINCT author_pubkey) FILTER (WHERE created_at >= $3))::double precision /
				GREATEST(COUNT(DISTINCT event_id) FILTER (WHERE created_at >= $3), 1) DESC,
			COUNT(DISTINCT event_id) FILTER (WHERE created_at >= $3) DESC,
			COUNT(*) FILTER (WHERE created_at >= $3) DESC,
			domain ASC
		LIMIT $4 OFFSET $5
	`, domainMediaURLFilterClause)
	rows, err := s.pool.Query(ctx, query, nowUnix-day, nowUnix-(7*day), windowFloor, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("get trending domains: %w", err)
	}
	defer rows.Close()

	out := make([]DomainSummaryProjection, 0, limit)
	for rows.Next() {
		var row DomainSummaryProjection
		if err := rows.Scan(
			&row.Domain,
			&row.LatestEventAt,
			&row.Activity.Last24h.LinkCount,
			&row.Activity.Last24h.NoteCount,
			&row.Activity.Last24h.UniqueAuthors,
			&row.Activity.Last7d.LinkCount,
			&row.Activity.Last7d.NoteCount,
			&row.Activity.Last7d.UniqueAuthors,
		); err != nil {
			return nil, fmt.Errorf("scan trending domain row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read trending domain rows: %w", err)
	}
	return out, nil
}

func (s *Read) GetDomainSummary(
	ctx context.Context,
	domain string,
	recentLimit int,
	topLimit int,
) (DomainSummaryProjection, error) {
	if s == nil || s.pool == nil {
		return DomainSummaryProjection{}, fmt.Errorf("store is not initialized")
	}
	normalized := domainnorm.NormalizeLookupValue(domain)
	if normalized == "" {
		return DomainSummaryProjection{}, ErrNotFound
	}
	if recentLimit <= 0 {
		recentLimit = 5
	}
	if recentLimit > 20 {
		recentLimit = 20
	}
	if topLimit <= 0 {
		topLimit = 5
	}
	if topLimit > 20 {
		topLimit = 20
	}

	var summary DomainSummaryProjection
	nowUnix := time.Now().UTC().Unix()
	day := int64((24 * time.Hour) / time.Second)
	query := fmt.Sprintf(`
		SELECT
			MAX(created_at),
			COUNT(*) FILTER (WHERE created_at >= $2),
			COUNT(DISTINCT event_id) FILTER (WHERE created_at >= $2),
			COUNT(DISTINCT author_pubkey) FILTER (WHERE created_at >= $2),
			COUNT(*) FILTER (WHERE created_at >= $3),
			COUNT(DISTINCT event_id) FILTER (WHERE created_at >= $3),
			COUNT(DISTINCT author_pubkey) FILTER (WHERE created_at >= $3),
			COUNT(*) FILTER (WHERE created_at >= $4),
			COUNT(DISTINCT event_id) FILTER (WHERE created_at >= $4),
			COUNT(DISTINCT author_pubkey) FILTER (WHERE created_at >= $4),
			COUNT(*),
			COUNT(DISTINCT event_id),
			COUNT(DISTINCT author_pubkey)
		FROM event_urls
		WHERE domain = $1
		  AND %s
	`, domainMediaURLFilterClause)
	if err := s.pool.QueryRow(ctx, query, normalized, nowUnix-day, nowUnix-(7*day), nowUnix-(30*day)).Scan(
		&summary.LatestEventAt,
		&summary.Activity.Last24h.LinkCount,
		&summary.Activity.Last24h.NoteCount,
		&summary.Activity.Last24h.UniqueAuthors,
		&summary.Activity.Last7d.LinkCount,
		&summary.Activity.Last7d.NoteCount,
		&summary.Activity.Last7d.UniqueAuthors,
		&summary.Activity.Last30d.LinkCount,
		&summary.Activity.Last30d.NoteCount,
		&summary.Activity.Last30d.UniqueAuthors,
		&summary.Activity.All.LinkCount,
		&summary.Activity.All.NoteCount,
		&summary.Activity.All.UniqueAuthors,
	); err != nil {
		return DomainSummaryProjection{}, fmt.Errorf("get domain summary: %w", err)
	}
	if summary.Activity.All.LinkCount == 0 {
		return DomainSummaryProjection{}, ErrNotFound
	}
	summary.Domain = normalized

	recent, err := s.GetDomainNotes(ctx, normalized, "latest", "30d", recentLimit, 0)
	if err != nil {
		return DomainSummaryProjection{}, err
	}
	top, err := s.GetDomainNotes(ctx, normalized, "top", "30d", topLimit, 0)
	if err != nil {
		return DomainSummaryProjection{}, err
	}
	summary.RecentNotes = recent
	summary.TopNotes = top
	return summary, nil
}

func (s *Read) GetDomainNotes(
	ctx context.Context,
	domain string,
	sort string,
	window string,
	limit int,
	offset int,
) ([]TrendingNote, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	normalized := domainnorm.NormalizeLookupValue(domain)
	if normalized == "" {
		return nil, ErrNotFound
	}
	exists, err := s.hasDomain(ctx, normalized)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrNotFound
	}

	sort = strings.ToLower(strings.TrimSpace(sort))
	if sort == "" {
		sort = "latest"
	}
	if sort != "latest" && sort != "top" {
		return nil, fmt.Errorf("unsupported domain notes sort: %s", sort)
	}
	window = strings.ToLower(strings.TrimSpace(window))
	if window == "" {
		window = "24h"
	}
	filterClause, scoreExpr, err := resolveDomainNotesWindow(window)
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

	orderBy := "s.created_at DESC, s.event_id DESC"
	if sort == "top" {
		orderBy = fmt.Sprintf("%s DESC, s.created_at DESC, s.event_id DESC", scoreExpr)
	}
	query := fmt.Sprintf(`
		SELECT
			s.event_id,
			s.author_pubkey,
			s.created_at,
			e.content,
			s.reply_count,
			s.repost_count,
			s.reaction_count,
			s.zap_count,
			s.zap_msats,
			%s AS score
		FROM (
			SELECT DISTINCT event_id
			FROM event_urls
			WHERE domain = $1
			  AND %s
		) d
		JOIN note_discovery_stats s ON s.event_id = d.event_id
		JOIN events e ON e.id = s.event_id
		WHERE %s
		ORDER BY %s
		LIMIT $2 OFFSET $3
	`, scoreExpr, domainMediaURLFilterClause, filterClause, orderBy)

	rows, err := s.pool.Query(ctx, query, normalized, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("get domain notes: %w", err)
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
			&row.ReplyCount,
			&row.RepostCount,
			&row.ReactionCount,
			&row.ZapCount,
			&row.ZapMSats,
			&row.Score,
		); err != nil {
			return nil, fmt.Errorf("scan domain note row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read domain note rows: %w", err)
	}
	return out, nil
}

func (s *Read) hasDomain(ctx context.Context, domain string) (bool, error) {
	var exists bool
	query := fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM event_urls WHERE domain = $1 AND %s)`, domainMediaURLFilterClause)
	if err := s.pool.QueryRow(ctx, query, domain).Scan(&exists); err != nil {
		return false, fmt.Errorf("check domain existence: %w", err)
	}
	return exists, nil
}

func resolveDomainNotesWindow(window string) (string, string, error) {
	switch window {
	case "24h":
		return "s.created_at >= extract(epoch from now())::bigint - (24 * 60 * 60)", "s.score_24h", nil
	case "7d":
		return "s.created_at >= extract(epoch from now())::bigint - (7 * 24 * 60 * 60)", "s.score_7d", nil
	case "30d":
		return "s.created_at >= extract(epoch from now())::bigint - (30 * 24 * 60 * 60)",
			"((s.reply_count * 3.0) + (s.repost_count * 2.0) + s.reaction_count + (s.zap_count * 2.0) + (s.zap_msats / 100000.0)) / (1.0 + ((extract(epoch from now())::bigint - s.created_at) / (30.0 * 24 * 60 * 60)))",
			nil
	case "all":
		return "TRUE", "((s.reply_count * 3.0) + (s.repost_count * 2.0) + s.reaction_count + (s.zap_count * 2.0) + (s.zap_msats / 100000.0))", nil
	default:
		return "", "", fmt.Errorf("unsupported domain notes window: %s", window)
	}
}

func scanDomainStatRows(rows pgx.Rows) ([]DomainStatProjection, error) {
	out := make([]DomainStatProjection, 0)
	for rows.Next() {
		var row DomainStatProjection
		if err := rows.Scan(&row.Domain, &row.LinkCount, &row.NoteCount, &row.UniqueAuthors); err != nil {
			return nil, fmt.Errorf("scan top domain row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read top domain rows: %w", err)
	}
	return out, nil
}
