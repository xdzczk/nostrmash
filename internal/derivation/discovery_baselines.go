package derivation

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// rebuildEntityBaselines refreshes topic and link medians, and emerging
// domains, at most once per UTC day. The source is the author-day buckets,
// so this does not scan event_hashtags or event_urls. A busy lock is left
// for the next tick; the snapshot refresh does not wait on it.
func (h *Handlers) rebuildEntityBaselines(ctx context.Context) error {
	if h == nil || h.pool == nil {
		return nil
	}
	today := utcDay(time.Now())
	var rebuilt *time.Time
	err := h.pool.QueryRow(ctx, `SELECT rebuilt_on FROM entity_baseline_state WHERE id`).Scan(&rebuilt)
	if err != nil {
		if isUndefinedTable(err) {
			return nil
		}
		return fmt.Errorf("read baseline state: %w", err)
	}
	if rebuilt != nil && !utcDay(*rebuilt).Before(today) {
		return nil
	}
	err = pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		locked, err := lockDiscoveryDaily(ctx, tx)
		if err != nil {
			return err
		}
		if !locked {
			return ErrBackgroundBusy
		}
		if err := rebuildHashtagBaselinesTx(ctx, tx, today); err != nil {
			return err
		}
		if err := rebuildDomainBaselinesTx(ctx, tx, today); err != nil {
			return err
		}
		if err := rebuildDomainMomentumTx(ctx, tx, today); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE entity_baseline_state SET rebuilt_on = $1::date WHERE id
		`, today); err != nil {
			return fmt.Errorf("mark baselines rebuilt: %w", err)
		}
		return nil
	})
	if errors.Is(err, ErrBackgroundBusy) {
		return nil
	}
	return err
}

func rebuildHashtagBaselinesTx(ctx context.Context, tx pgx.Tx, today time.Time) error {
	if _, err := tx.Exec(ctx, `
		DELETE FROM entity_baseline
		WHERE entity_type = 'hashtag' AND metric = 'unique_authors'
	`); err != nil {
		return fmt.Errorf("clear hashtag baselines: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO entity_baseline (entity_type, entity_key, metric, typical, sample_days, updated_at)
		SELECT 'hashtag', hashtag, 'unique_authors',
		       percentile_cont(0.5) WITHIN GROUP (ORDER BY authors),
		       count(*)::int,
		       now()
		FROM (
			SELECT h.hashtag, h.day, COUNT(DISTINCT h.author_pubkey)::float8 AS authors
			FROM hashtag_author_daily h
			INNER JOIN trust_graph_snapshot trusted ON trusted.pubkey = h.author_pubkey
			WHERE h.day >= $1::date - 30
			  AND h.day < $1::date
			  AND h.hashtag !~ '^[0-9]+$'
			GROUP BY h.hashtag, h.day
		) daily
		GROUP BY hashtag
		HAVING count(*) >= 7
	`, today); err != nil {
		return fmt.Errorf("rebuild hashtag baselines: %w", err)
	}
	return nil
}

func rebuildDomainBaselinesTx(ctx context.Context, tx pgx.Tx, today time.Time) error {
	if _, err := tx.Exec(ctx, `
		DELETE FROM entity_baseline
		WHERE entity_type = 'domain' AND metric = 'unique_authors'
	`); err != nil {
		return fmt.Errorf("clear domain baselines: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO entity_baseline (entity_type, entity_key, metric, typical, sample_days, updated_at)
		SELECT 'domain', domain, 'unique_authors',
		       percentile_cont(0.5) WITHIN GROUP (ORDER BY authors),
		       count(*)::int,
		       now()
		FROM (
			SELECT d.canonical_domain AS domain,
			       d.day,
			       COUNT(DISTINCT d.author_pubkey)::float8 AS authors
			FROM domain_author_daily d
			INNER JOIN trust_graph_snapshot trusted ON trusted.pubkey = d.author_pubkey
			WHERE d.day >= $1::date - 30
			  AND d.day < $1::date
			GROUP BY d.canonical_domain, d.day
		) daily
		GROUP BY domain
		HAVING count(*) >= 7
	`, today); err != nil {
		return fmt.Errorf("rebuild domain baselines: %w", err)
	}
	return nil
}

// A domain is emerging only after the prior week already had a pace. A first
// appearance has nothing to be faster than.
func rebuildDomainMomentumTx(ctx context.Context, tx pgx.Tx, today time.Time) error {
	if _, err := tx.Exec(ctx, `DELETE FROM domain_momentum`); err != nil {
		return fmt.Errorf("clear domain momentum: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO domain_momentum (domain, z_score_7d, status, half_life_days, updated_at)
		SELECT domain,
		       CASE WHEN prior = 0 THEN recent ELSE (recent - prior)::float8 / GREATEST(prior, 1) END,
		       CASE
		           WHEN prior >= 8 AND recent >= prior * 2 AND recent >= 8 THEN 'emerging'
		           WHEN prior > 0 AND recent * 2 < prior THEN 'fading'
		           ELSE 'steady'
		       END,
		       0,
		       now()
		FROM (
			SELECT d.canonical_domain AS domain,
			       COUNT(DISTINCT d.author_pubkey) FILTER (
			           WHERE d.day >= $1::date - 7 AND d.day < $1::date
			       ) AS recent,
			       COUNT(DISTINCT d.author_pubkey) FILTER (
			           WHERE d.day >= $1::date - 14 AND d.day < $1::date - 7
			       ) AS prior
			FROM domain_author_daily d
			INNER JOIN trust_graph_snapshot trusted ON trusted.pubkey = d.author_pubkey
			WHERE d.day >= $1::date - 14 AND d.day < $1::date
			GROUP BY d.canonical_domain
		) windows
		WHERE recent >= 8 OR prior >= 8
	`, today); err != nil {
		return fmt.Errorf("rebuild domain momentum: %w", err)
	}
	return nil
}
