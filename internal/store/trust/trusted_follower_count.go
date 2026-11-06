package trust

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TrustedFollowerPolicy is the inclusion gate for trusted_follower_count.
// MaxHops and UntrustedWeight match discovery engagement weighting.
// TrustWeighted is not a gate: membership in the published trust graph is
// the bar even when discovery scoring is still counting every engager as 1.
// A positive UntrustedWeight counts every non-self follower as one person.
// Zero counts only followers whose published hop is within MaxHops.
// Self-follows never count.
type TrustedFollowerPolicy struct {
	MaxHops         int
	UntrustedWeight float64
}

func (p TrustedFollowerPolicy) normalized() TrustedFollowerPolicy {
	if p.MaxHops <= 0 {
		p.MaxHops = 3
	}
	if p.UntrustedWeight < 0 {
		p.UntrustedWeight = 0
	}
	return p
}

// SetTrustedFollowerUntrustedWeight stores the weight RefreshTrustGraphSnapshot
// uses when it recounts trusted followers. The hop limit is the maxHops
// argument of that refresh, which is the same TRUST_MAX_HOPS value.
func (s *Trust) SetTrustedFollowerUntrustedWeight(weight float64) {
	if s == nil {
		return
	}
	if weight < 0 {
		weight = 0
	}
	s.trustedFollowerUntrustedWeight = weight
}

func (s *Trust) trustedFollowerPolicy(maxHops int) TrustedFollowerPolicy {
	weight := 0.0
	if s != nil {
		weight = s.trustedFollowerUntrustedWeight
	}
	return TrustedFollowerPolicy{MaxHops: maxHops, UntrustedWeight: weight}
}

// RecountTrustedFollowerCounts rewrites trusted_follower_count from
// follower_edges and the published trust graph. It runs off the request
// path: a pubkey entering the graph can follow many accounts, and that
// recount must not sit inside an HTTP handler or the snapshot transaction.
// The write is absolute, so repeating it heals drift from an in-flight
// contact-list delta.
func RecountTrustedFollowerCounts(ctx context.Context, pool *pgxpool.Pool, policy TrustedFollowerPolicy) error {
	if pool == nil {
		return fmt.Errorf("recount trusted followers: pool is nil")
	}
	policy = policy.normalized()
	var err error
	for attempt := 1; attempt <= 3; attempt++ {
		err = recountTrustedFollowerCountsOnce(ctx, pool, policy)
		if !isDeadlock(err) || attempt == 3 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt) * 50 * time.Millisecond):
		}
	}
	return err
}

func isDeadlock(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "40P01"
}

func recountTrustedFollowerCountsOnce(ctx context.Context, pool *pgxpool.Pool, policy TrustedFollowerPolicy) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin trusted follower recount: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setTrustHeavyStatementTimeout(ctx, tx); err != nil {
		return err
	}

	// Keep this predicate aligned with trustedFollowerCountSQL in
	// internal/derivation/projection_profile_public_stats.go and with
	// followerInTrustedAudience. Placeholders here are $1 weight, $2 hops.
	if _, err := tx.Exec(ctx, `
		CREATE TEMP TABLE trusted_follower_counted (
			pubkey text PRIMARY KEY,
			n bigint NOT NULL
		) ON COMMIT DROP
	`); err != nil {
		return fmt.Errorf("create trusted follower recount table: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO trusted_follower_counted (pubkey, n)
		SELECT fe.followed_pubkey, COUNT(*)::bigint
		FROM follower_edges fe
		LEFT JOIN trust_pubkeys_latest t ON t.pubkey = fe.follower_pubkey
		WHERE fe.follower_pubkey <> fe.followed_pubkey
		  AND (
		    $1::double precision > 0
		    OR (t.min_hops IS NOT NULL AND t.min_hops <= $2::int)
		  )
		GROUP BY fe.followed_pubkey
	`, policy.UntrustedWeight, policy.MaxHops); err != nil {
		return fmt.Errorf("count trusted followers: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE profile_public_stats pps
		SET trusted_follower_count = 0,
		    updated_at = now()
		WHERE pps.trusted_follower_count <> 0
		  AND NOT EXISTS (
		    SELECT 1 FROM trusted_follower_counted c WHERE c.pubkey = pps.pubkey
		  )
	`); err != nil {
		return fmt.Errorf("clear trusted followers absent from the recount: %w", err)
	}
	// New rows take the active profile_public_stats version. Existing rows
	// keep theirs: this recount does not mean the other counters were rebuilt.
	if _, err := tx.Exec(ctx, `
		INSERT INTO profile_public_stats (
			pubkey,
			follower_count,
			following_count,
			note_count,
			reply_count,
			trusted_follower_count,
			derivation_version
		)
		SELECT
			c.pubkey,
			0,
			0,
			0,
			0,
			c.n,
			COALESCE(
				(SELECT active_version
				 FROM derivation_active_versions
				 WHERE derivation_name = 'profile_public_stats'),
				1
			)
		FROM trusted_follower_counted c
		ORDER BY c.pubkey
		ON CONFLICT (pubkey) DO UPDATE
		SET trusted_follower_count = EXCLUDED.trusted_follower_count,
		    updated_at = now()
		WHERE profile_public_stats.trusted_follower_count IS DISTINCT FROM EXCLUDED.trusted_follower_count
	`); err != nil {
		return fmt.Errorf("upsert trusted follower counts: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit trusted follower recount: %w", err)
	}
	return nil
}
