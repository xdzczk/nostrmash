package derivation

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xdzczk/nostrmash/internal/metrics"
)

// ErrBackgroundBusy means another worker already holds the rollup lock.
// The caller should wait and try again; it is not a failed rollup.
var ErrBackgroundBusy = errors.New("background rollup is busy")

const discoveryDailyLock = int64(814201)

// AdvanceDiscoveryDailyRollup seals one historical day of hashtag or domain
// author buckets. more is false once both cursors have reached today; the
// snapshot refresh then re-rolls yesterday and today itself.
func (h *Handlers) AdvanceDiscoveryDailyRollup(ctx context.Context) (more bool, err error) {
	if h == nil || h.pool == nil {
		return false, fmt.Errorf("handlers are not initialized")
	}
	started := time.Now()
	defer func() {
		metrics.ObserveDBOperation("advance_discovery_daily_rollup", dbResultLabel(err), time.Since(started))
	}()
	err = pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		locked, err := lockDiscoveryDaily(ctx, tx)
		if err != nil {
			return err
		}
		if !locked {
			return ErrBackgroundBusy
		}
		today := utcDay(time.Now())
		var hashtagNext, domainNext *time.Time
		if err := tx.QueryRow(ctx, `
			SELECT hashtag_next, domain_next
			FROM discovery_daily_rollup_state
			WHERE id
			FOR UPDATE
		`).Scan(&hashtagNext, &domainNext); err != nil {
			return fmt.Errorf("read discovery daily state: %w", err)
		}
		if dayBefore(hashtagNext, today) {
			day := coalesceDay(hashtagNext, today.AddDate(0, 0, -31))
			if err := rollHashtagDayTx(ctx, tx, day); err != nil {
				return err
			}
			more = true
			if _, err := tx.Exec(ctx, `
				UPDATE discovery_daily_rollup_state SET hashtag_next = $1::date WHERE id
			`, day.AddDate(0, 0, 1)); err != nil {
				return fmt.Errorf("advance hashtag daily cursor: %w", err)
			}
			return nil
		}
		if dayBefore(domainNext, today) {
			day := coalesceDay(domainNext, today.AddDate(0, 0, -31))
			if err := rollDomainDayTx(ctx, tx, day); err != nil {
				return err
			}
			more = true
			if _, err := tx.Exec(ctx, `
				UPDATE discovery_daily_rollup_state SET domain_next = $1::date WHERE id
			`, day.AddDate(0, 0, 1)); err != nil {
				return fmt.Errorf("advance domain daily cursor: %w", err)
			}
		}
		return nil
	})
	return more, err
}

func (h *Handlers) prepareDiscoveryDailySnapshots(ctx context.Context) bool {
	ready, err := h.discoveryDailyReady(ctx)
	if err != nil || !ready {
		return false
	}
	if err := h.refreshDiscoveryDailyRecent(ctx); err != nil {
		return false
	}
	// Baselines are a once-a-day read of the buckets just sealed. A failure
	// here leaves yesterday's medians in place and does not skip snapshots.
	_ = h.rebuildEntityBaselines(ctx)
	return true
}

func (h *Handlers) discoveryDailyReady(ctx context.Context) (bool, error) {
	var hashtagNext, domainNext *time.Time
	err := h.pool.QueryRow(ctx, `
		SELECT hashtag_next, domain_next FROM discovery_daily_rollup_state WHERE id
	`).Scan(&hashtagNext, &domainNext)
	if err != nil {
		return false, fmt.Errorf("read discovery daily readiness: %w", err)
	}
	today := utcDay(time.Now())
	return !dayBefore(hashtagNext, today) && !dayBefore(domainNext, today), nil
}

func (h *Handlers) refreshDiscoveryDailyRecent(ctx context.Context) error {
	return pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		locked, err := lockDiscoveryDaily(ctx, tx)
		if err != nil {
			return err
		}
		if !locked {
			return ErrBackgroundBusy
		}
		today := utcDay(time.Now())
		for _, day := range []time.Time{today.AddDate(0, 0, -1), today} {
			if err := rollHashtagDayTx(ctx, tx, day); err != nil {
				return err
			}
			if err := rollDomainDayTx(ctx, tx, day); err != nil {
				return err
			}
		}
		cutoff := today.AddDate(0, 0, -40)
		if _, err := tx.Exec(ctx, `DELETE FROM hashtag_author_daily WHERE day < $1::date`, cutoff); err != nil {
			return fmt.Errorf("prune hashtag daily: %w", err)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM domain_author_daily WHERE day < $1::date`, cutoff); err != nil {
			return fmt.Errorf("prune domain daily: %w", err)
		}
		return nil
	})
}

func lockDiscoveryDaily(ctx context.Context, tx pgx.Tx) (bool, error) {
	if _, err := tx.Exec(ctx, `SET LOCAL statement_timeout = '60s'`); err != nil {
		return false, fmt.Errorf("set statement_timeout: %w", err)
	}
	var locked bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, discoveryDailyLock).Scan(&locked); err != nil {
		return false, fmt.Errorf("lock discovery daily rollup: %w", err)
	}
	return locked, nil
}

func rollHashtagDayTx(ctx context.Context, tx pgx.Tx, day time.Time) error {
	if _, err := tx.Exec(ctx, `DELETE FROM hashtag_author_daily WHERE day = $1::date`, day); err != nil {
		return fmt.Errorf("clear hashtag day: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO hashtag_author_daily (day, hashtag, author_pubkey, language, events)
		SELECT $1::date,
		       h.hashtag,
		       h.author_pubkey,
		       COALESCE(nds.primary_language, ''),
		       COUNT(*)::int
		FROM event_hashtags h
		LEFT JOIN note_discovery_stats nds ON nds.event_id = h.event_id
		WHERE h.created_at >= EXTRACT(EPOCH FROM $1::date)::bigint
		  AND h.created_at < EXTRACT(EPOCH FROM $1::date + interval '1 day')::bigint
		GROUP BY h.hashtag, h.author_pubkey, COALESCE(nds.primary_language, '')
	`, day); err != nil {
		return fmt.Errorf("rollup hashtag day: %w", err)
	}
	return nil
}

func rollDomainDayTx(ctx context.Context, tx pgx.Tx, day time.Time) error {
	if _, err := tx.Exec(ctx, `DELETE FROM domain_author_daily WHERE day = $1::date`, day); err != nil {
		return fmt.Errorf("clear domain day: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO domain_author_daily (
			day, canonical_domain, author_pubkey, language, links, notes, latest_created_at
		)
		SELECT $1::date,
		       u.canonical_domain,
		       u.author_pubkey,
		       COALESCE(nds.primary_language, ''),
		       COUNT(*)::int,
		       COUNT(DISTINCT u.event_id)::int,
		       MAX(u.created_at)
		FROM event_urls u
		LEFT JOIN note_discovery_stats nds ON nds.event_id = u.event_id
		WHERE NOT u.is_media
		  AND u.created_at >= EXTRACT(EPOCH FROM $1::date)::bigint
		  AND u.created_at < EXTRACT(EPOCH FROM $1::date + interval '1 day')::bigint
		GROUP BY u.canonical_domain, u.author_pubkey, COALESCE(nds.primary_language, '')
	`, day); err != nil {
		return fmt.Errorf("rollup domain day: %w", err)
	}
	return nil
}

func utcDay(now time.Time) time.Time {
	utc := now.UTC()
	return time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
}

func dayBefore(day *time.Time, today time.Time) bool {
	if day == nil {
		return true
	}
	return utcDay(*day).Before(today)
}

func coalesceDay(day *time.Time, fallback time.Time) time.Time {
	if day == nil {
		return fallback
	}
	return utcDay(*day)
}

func computeTopHashtagsFromDaily(ctx context.Context, tx pgx.Tx, minCreatedAt int64, limit int) ([]hashtagRow, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := tx.Query(ctx, `
		SELECT h.hashtag,
		       SUM(h.events)::bigint AS event_count,
		       COUNT(DISTINCT h.author_pubkey)::bigint AS unique_authors
		FROM hashtag_author_daily h
		INNER JOIN trust_graph_snapshot trusted ON trusted.pubkey = h.author_pubkey
		WHERE h.day >= $1::date
		  AND h.hashtag !~ '^[0-9]+$'
		GROUP BY h.hashtag
		ORDER BY
			COUNT(DISTINCT h.author_pubkey) DESC,
			COUNT(DISTINCT h.author_pubkey)::double precision / GREATEST(SUM(h.events), 1) DESC,
			SUM(h.events) DESC,
			h.hashtag ASC
		LIMIT $2
	`, time.Unix(minCreatedAt, 0).UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("query top hashtags from daily: %w", err)
	}
	defer rows.Close()
	out := make([]hashtagRow, 0, limit)
	for rows.Next() {
		var row hashtagRow
		if err := rows.Scan(&row.Hashtag, &row.EventCount, &row.UniqueAuthors); err != nil {
			return nil, fmt.Errorf("scan daily hashtag: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read daily hashtags: %w", err)
	}
	return out, nil
}

func computeTopHashtagsByLanguageFromDaily(ctx context.Context, tx pgx.Tx, minCreatedAt int64, languages []string, limit int) (map[string][]hashtagRow, error) {
	out := map[string][]hashtagRow{}
	if len(languages) == 0 {
		return out, nil
	}
	if limit <= 0 {
		limit = 50
	}
	rows, err := tx.Query(ctx, `
		SELECT language, hashtag, event_count, unique_authors
		FROM (
			SELECT h.language AS language,
			       h.hashtag,
			       SUM(h.events)::bigint AS event_count,
			       COUNT(DISTINCT h.author_pubkey)::bigint AS unique_authors,
			       ROW_NUMBER() OVER (
			           PARTITION BY h.language
			           ORDER BY
			               COUNT(DISTINCT h.author_pubkey) DESC,
			               COUNT(DISTINCT h.author_pubkey)::double precision / GREATEST(SUM(h.events), 1) DESC,
			               SUM(h.events) DESC,
			               h.hashtag ASC
			       ) AS rn
			FROM hashtag_author_daily h
			INNER JOIN trust_graph_snapshot trusted ON trusted.pubkey = h.author_pubkey
			WHERE h.day >= $1::date
			  AND h.language = ANY($2::text[])
			  AND h.language <> ''
			  AND h.hashtag !~ '^[0-9]+$'
			GROUP BY h.language, h.hashtag
		) ranked
		WHERE rn <= $3
		ORDER BY language ASC, rn ASC
	`, time.Unix(minCreatedAt, 0).UTC(), languages, limit)
	if err != nil {
		return nil, fmt.Errorf("query daily hashtags by language: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var language string
		var row hashtagRow
		if err := rows.Scan(&language, &row.Hashtag, &row.EventCount, &row.UniqueAuthors); err != nil {
			return nil, fmt.Errorf("scan daily hashtag language: %w", err)
		}
		out[language] = append(out[language], row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read daily hashtag languages: %w", err)
	}
	return out, nil
}

const discoveryDailyCandidateLimit = 80

func computeTopDomains30dFromDaily(
	ctx context.Context,
	tx pgx.Tx,
	cutoff24h int64,
	cutoff7d int64,
	cutoff30d int64,
	limit int,
) ([]domainRow, error) {
	if limit <= 0 {
		limit = 50
	}
	candidates, err := dailyDomainCandidates(ctx, tx, cutoff30d, nil, discoveryDailyCandidateLimit)
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return []domainRow{}, nil
	}
	exact, err := exactDomainActivity(ctx, tx, cutoff24h, cutoff7d, cutoff30d, candidates)
	if err != nil {
		return nil, err
	}
	sortDomainRows(exact)
	if len(exact) > limit {
		exact = exact[:limit]
	}
	return exact, nil
}

func computeTopDomains30dByLanguageFromDaily(
	ctx context.Context,
	tx pgx.Tx,
	cutoff24h int64,
	cutoff7d int64,
	cutoff30d int64,
	languages []string,
	limit int,
) (map[string][]domainRow, error) {
	out := map[string][]domainRow{}
	if len(languages) == 0 {
		return out, nil
	}
	if limit <= 0 {
		limit = 50
	}
	candidates, err := dailyDomainCandidates(ctx, tx, cutoff30d, languages, discoveryDailyCandidateLimit)
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return out, nil
	}
	rows, err := tx.Query(ctx, `
		SELECT nds.primary_language,
		       u.canonical_domain,
		       MAX(u.created_at),
		       COUNT(*) FILTER (WHERE u.created_at >= $1),
		       COUNT(DISTINCT u.event_id) FILTER (WHERE u.created_at >= $1),
		       COUNT(DISTINCT u.author_pubkey) FILTER (WHERE u.created_at >= $1),
		       COUNT(*) FILTER (WHERE u.created_at >= $2),
		       COUNT(DISTINCT u.event_id) FILTER (WHERE u.created_at >= $2),
		       COUNT(DISTINCT u.author_pubkey) FILTER (WHERE u.created_at >= $2),
		       COUNT(*),
		       COUNT(DISTINCT u.event_id),
		       COUNT(DISTINCT u.author_pubkey)
		FROM event_urls u
		INNER JOIN note_discovery_stats nds ON nds.event_id = u.event_id
		INNER JOIN trust_graph_snapshot trusted ON trusted.pubkey = u.author_pubkey
		WHERE u.created_at >= $3
		  AND NOT u.is_media
		  AND u.canonical_domain = ANY($4::text[])
		  AND nds.primary_language = ANY($5::text[])
		GROUP BY nds.primary_language, u.canonical_domain
	`, cutoff24h, cutoff7d, cutoff30d, candidates, languages)
	if err != nil {
		return nil, fmt.Errorf("query exact domains by language: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var language string
		var row domainRow
		if err := rows.Scan(
			&language,
			&row.Domain,
			&row.LatestEventAt,
			&row.Activity24h.LinkCount,
			&row.Activity24h.NoteCount,
			&row.Activity24h.UniqueAuthors,
			&row.Activity7d.LinkCount,
			&row.Activity7d.NoteCount,
			&row.Activity7d.UniqueAuthors,
			&row.Activity30d.LinkCount,
			&row.Activity30d.NoteCount,
			&row.Activity30d.UniqueAuthors,
		); err != nil {
			return nil, fmt.Errorf("scan exact domain language: %w", err)
		}
		out[language] = append(out[language], row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read exact domain languages: %w", err)
	}
	for language, rows := range out {
		sortDomainRows(rows)
		if len(rows) > limit {
			rows = rows[:limit]
		}
		out[language] = rows
	}
	return out, nil
}

func dailyDomainCandidates(ctx context.Context, tx pgx.Tx, cutoff30d int64, languages []string, limit int) ([]string, error) {
	var (
		rows pgx.Rows
		err  error
	)
	if len(languages) == 0 {
		rows, err = tx.Query(ctx, `
			SELECT d.canonical_domain
			FROM domain_author_daily d
			INNER JOIN trust_graph_snapshot trusted ON trusted.pubkey = d.author_pubkey
			WHERE d.day >= $1::date
			GROUP BY d.canonical_domain
			ORDER BY COUNT(DISTINCT d.author_pubkey) DESC, d.canonical_domain ASC
			LIMIT $2
		`, time.Unix(cutoff30d, 0).UTC(), limit)
	} else {
		rows, err = tx.Query(ctx, `
			SELECT canonical_domain
			FROM (
				SELECT d.canonical_domain,
				       ROW_NUMBER() OVER (
				           PARTITION BY d.language
				           ORDER BY COUNT(DISTINCT d.author_pubkey) DESC, d.canonical_domain ASC
				       ) AS rn
				FROM domain_author_daily d
				INNER JOIN trust_graph_snapshot trusted ON trusted.pubkey = d.author_pubkey
				WHERE d.day >= $1::date
				  AND d.language = ANY($3::text[])
				  AND d.language <> ''
				GROUP BY d.language, d.canonical_domain
			) ranked
			WHERE rn <= $2
		`, time.Unix(cutoff30d, 0).UTC(), limit, languages)
	}
	if err != nil {
		return nil, fmt.Errorf("query daily domain candidates: %w", err)
	}
	defer rows.Close()
	seen := map[string]struct{}{}
	out := make([]string, 0, limit)
	for rows.Next() {
		var domain string
		if err := rows.Scan(&domain); err != nil {
			return nil, fmt.Errorf("scan daily domain candidate: %w", err)
		}
		if _, ok := seen[domain]; ok {
			continue
		}
		seen[domain] = struct{}{}
		out = append(out, domain)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read daily domain candidates: %w", err)
	}
	return out, nil
}

func exactDomainActivity(ctx context.Context, tx pgx.Tx, cutoff24h, cutoff7d, cutoff30d int64, domains []string) ([]domainRow, error) {
	rows, err := tx.Query(ctx, `
		SELECT u.canonical_domain,
		       MAX(u.created_at),
		       COUNT(*) FILTER (WHERE u.created_at >= $1),
		       COUNT(DISTINCT u.event_id) FILTER (WHERE u.created_at >= $1),
		       COUNT(DISTINCT u.author_pubkey) FILTER (WHERE u.created_at >= $1),
		       COUNT(*) FILTER (WHERE u.created_at >= $2),
		       COUNT(DISTINCT u.event_id) FILTER (WHERE u.created_at >= $2),
		       COUNT(DISTINCT u.author_pubkey) FILTER (WHERE u.created_at >= $2),
		       COUNT(*),
		       COUNT(DISTINCT u.event_id),
		       COUNT(DISTINCT u.author_pubkey)
		FROM event_urls u
		INNER JOIN trust_graph_snapshot trusted ON trusted.pubkey = u.author_pubkey
		WHERE u.created_at >= $3
		  AND NOT u.is_media
		  AND u.canonical_domain = ANY($4::text[])
		GROUP BY u.canonical_domain
	`, cutoff24h, cutoff7d, cutoff30d, domains)
	if err != nil {
		return nil, fmt.Errorf("query exact domain activity: %w", err)
	}
	defer rows.Close()
	out := make([]domainRow, 0, len(domains))
	for rows.Next() {
		var row domainRow
		if err := rows.Scan(
			&row.Domain,
			&row.LatestEventAt,
			&row.Activity24h.LinkCount,
			&row.Activity24h.NoteCount,
			&row.Activity24h.UniqueAuthors,
			&row.Activity7d.LinkCount,
			&row.Activity7d.NoteCount,
			&row.Activity7d.UniqueAuthors,
			&row.Activity30d.LinkCount,
			&row.Activity30d.NoteCount,
			&row.Activity30d.UniqueAuthors,
		); err != nil {
			return nil, fmt.Errorf("scan exact domain activity: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read exact domain activity: %w", err)
	}
	return out, nil
}

func sortDomainRows(rows []domainRow) {
	sort.SliceStable(rows, func(i, j int) bool {
		ai, aj := rows[i].Activity30d, rows[j].Activity30d
		if ai.UniqueAuthors != aj.UniqueAuthors {
			return ai.UniqueAuthors > aj.UniqueAuthors
		}
		di := float64(ai.UniqueAuthors) / float64(maxInt64(ai.NoteCount, 1))
		dj := float64(aj.UniqueAuthors) / float64(maxInt64(aj.NoteCount, 1))
		if di != dj {
			return di > dj
		}
		if ai.NoteCount != aj.NoteCount {
			return ai.NoteCount > aj.NoteCount
		}
		if ai.LinkCount != aj.LinkCount {
			return ai.LinkCount > aj.LinkCount
		}
		return rows[i].Domain < rows[j].Domain
	})
}
