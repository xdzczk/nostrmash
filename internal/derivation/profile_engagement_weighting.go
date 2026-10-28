package derivation

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// profileWeightedScoreInputs is what profile trending and rising actually
// score. Raw recent_* counters stay on the analytics path.
//
// Trending engagement is one trust-weighted vote per (signal, engager) on
// root notes published in the window. One person liking ten of those notes
// counts once per signal, and a like on an older note does not count.
// Rising engagement is one vote per engager whose event falls in the window,
// at any note age: attention arriving now, without letting one account walk
// an old timeline into a pile of votes. motionAt is the newest in-window
// root note or the latest engagement on one, whichever is later.
type profileWeightedScoreInputs struct {
	trendingEngagement24h float64
	trendingEngagement7d  float64
	trendingEngagement30d float64
	risingEngagement24h   float64
	risingEngagement7d    float64
	risingEngagement30d   float64
	trendingZapMSats24h   float64
	trendingZapMSats7d    float64
	trendingZapMSats30d   float64
	newFollowers24h       float64
	newFollowers7d        float64
	newFollowers30d       float64
	motionAt24h           *int64
	motionAt7d            *int64
	motionAt30d           *int64
}

// loadProfileWeightedScoreInputsTx scans windowed engagement toward one
// profile and aggregates the trending and rising score inputs. Trust
// weighting scales each vote; with it off, every non-author engager counts
// 1.0. Either way the population rules are the same, so a like on an old
// note cannot become "engagement per post".
//
// Engagement branches start from the denormalized target_pubkey indexes
// (migration 000082) and only then look up the target note by primary key.
// The previous plan joined events first to discover the author and timed
// out. Rows with NULL target_pubkey are skipped. The authored-note scan is
// bounded to the 30d window on (pubkey, kind, created_at).
func loadProfileWeightedScoreInputsTx(
	ctx context.Context,
	tx pgx.Tx,
	pubkey string,
	nowUnix int64,
	weighting EngagementWeightingOptions,
) (profileWeightedScoreInputs, error) {
	weighting = weighting.normalized()
	cutoff24h := nowUnix - int64((24*time.Hour)/time.Second)
	cutoff7d := nowUnix - int64((7*24*time.Hour)/time.Second)
	cutoff30d := nowUnix - int64((30*24*time.Hour)/time.Second)
	// $4/$5/$6 stay the shared engager-weight binds. $2 is the 30d floor,
	// $3 is the 24h cutoff, $7 is the 7d cutoff, and $8 is the sane created_at
	// ceiling. Follower gains do not use $8.
	baseArgs := []any{pubkey, cutoff30d, cutoff24h, weighting.TrustWeighted, weighting.MaxHops, weighting.UntrustedWeight, cutoff7d}
	noteArgs := append(append([]any{}, baseArgs...), maxSaneUnixCreatedAt)

	var out profileWeightedScoreInputs

	// Trending votes require the target to be a root note published in the
	// window, then collapse to one vote per (signal, engager). Rising votes
	// collapse to one vote per engager across every signal and note age.
	// Profile zaps have no note, so they can move rising and not trending.
	if err := tx.QueryRow(ctx, `
		WITH raw AS (
			SELECT 'reply'::text AS src, c.source_pubkey AS engager, c.target_event_id AS target, c.source_created_at AS ts
			FROM reply_count_contributions c
			WHERE c.target_pubkey = $1
			  AND c.source_pubkey <> $1
			  AND c.source_created_at >= $2
			  AND c.source_created_at <= $8
			UNION ALL
			SELECT 'repost', r.reposter_pubkey, r.target_event_id, r.created_at
			FROM repost_events r
			WHERE r.target_pubkey = $1
			  AND r.reposter_pubkey <> $1
			  AND r.created_at >= $2
			  AND r.created_at <= $8
			UNION ALL
			SELECT 'reaction', r.reactor_pubkey, r.target_event_id, r.created_at
			FROM reaction_events r
			WHERE r.target_pubkey = $1
			  AND r.reactor_pubkey <> $1
			  AND r.created_at >= $2
			  AND r.created_at <= $8
			UNION ALL
			SELECT 'zap', zr.sender_pubkey, NULLIF(zr.event_id, ''), zr.created_at
			FROM zap_receipts zr
			WHERE zr.receiver_pubkey = $1
			  AND zr.sender_pubkey <> $1
			  AND zr.created_at >= $2
			  AND zr.created_at <= $8
		),
		root_notes AS (
			SELECT r.src, r.engager, r.ts, e.created_at AS note_created
			FROM raw r
			JOIN events e ON e.id = r.target
			WHERE e.kind = 1
			  AND e.pubkey = $1
			  AND e.created_at <= $8
			  AND NOT EXISTS (
				SELECT 1 FROM thread_edges te WHERE te.child_event_id = e.id
			  )
		),
		trending_votes AS (
			SELECT src, engager,
				bool_or(note_created >= $3) AS in_24h,
				bool_or(note_created >= $7) AS in_7d
			FROM root_notes
			WHERE note_created >= $2
			GROUP BY src, engager
		),
		rising_votes AS (
			SELECT engager,
				bool_or(ts >= $3) AS in_24h,
				bool_or(ts >= $7) AS in_7d
			FROM raw
			GROUP BY engager
		),
		authored AS (
			SELECT
				MAX(e.created_at) FILTER (WHERE e.created_at >= $3) AS at_24h,
				MAX(e.created_at) FILTER (WHERE e.created_at >= $7) AS at_7d,
				MAX(e.created_at) AS at_30d
			FROM events e
			WHERE e.pubkey = $1
			  AND e.kind = 1
			  AND e.created_at >= $2
			  AND e.created_at <= $8
			  AND NOT EXISTS (
				SELECT 1 FROM thread_edges te WHERE te.child_event_id = e.id
			  )
		),
		engaged AS (
			SELECT
				MAX(ts) FILTER (WHERE note_created >= $3) AS at_24h,
				MAX(ts) FILTER (WHERE note_created >= $7) AS at_7d,
				MAX(ts) FILTER (WHERE note_created >= $2) AS at_30d
			FROM root_notes
		),
		motion AS (
			SELECT
				GREATEST(a.at_24h, g.at_24h) AS at_24h,
				GREATEST(a.at_7d, g.at_7d) AS at_7d,
				GREATEST(a.at_30d, g.at_30d) AS at_30d
			FROM authored a
			CROSS JOIN engaged g
		)
		SELECT
			COALESCE((SELECT SUM(w) FROM (
				SELECT `+engagerWeightCaseSQL+` AS w
				FROM trending_votes v
				LEFT JOIN trust_pubkeys_latest t ON t.pubkey = v.engager
				WHERE v.in_24h
			) s), 0),
			COALESCE((SELECT SUM(w) FROM (
				SELECT `+engagerWeightCaseSQL+` AS w
				FROM trending_votes v
				LEFT JOIN trust_pubkeys_latest t ON t.pubkey = v.engager
				WHERE v.in_7d
			) s), 0),
			COALESCE((SELECT SUM(w) FROM (
				SELECT `+engagerWeightCaseSQL+` AS w
				FROM trending_votes v
				LEFT JOIN trust_pubkeys_latest t ON t.pubkey = v.engager
			) s), 0),
			COALESCE((SELECT SUM(w) FROM (
				SELECT `+engagerWeightCaseSQL+` AS w
				FROM rising_votes v
				LEFT JOIN trust_pubkeys_latest t ON t.pubkey = v.engager
				WHERE v.in_24h
			) s), 0),
			COALESCE((SELECT SUM(w) FROM (
				SELECT `+engagerWeightCaseSQL+` AS w
				FROM rising_votes v
				LEFT JOIN trust_pubkeys_latest t ON t.pubkey = v.engager
				WHERE v.in_7d
			) s), 0),
			COALESCE((SELECT SUM(w) FROM (
				SELECT `+engagerWeightCaseSQL+` AS w
				FROM rising_votes v
				LEFT JOIN trust_pubkeys_latest t ON t.pubkey = v.engager
			) s), 0),
			(SELECT at_24h FROM motion),
			(SELECT at_7d FROM motion),
			(SELECT at_30d FROM motion)
	`, noteArgs...).Scan(
		&out.trendingEngagement24h, &out.trendingEngagement7d, &out.trendingEngagement30d,
		&out.risingEngagement24h, &out.risingEngagement7d, &out.risingEngagement30d,
		&out.motionAt24h, &out.motionAt7d, &out.motionAt30d,
	); err != nil {
		return out, fmt.Errorf("load weighted profile engagement: %w", err)
	}

	// Zap volume counts only sats on root notes published in the window.
	// A zap on an old note must not raise the per-post score, and a profile
	// zap (no note) is not a post either. Each sat is still scaled by the
	// sender's trust weight.
	if err := tx.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(v) FILTER (WHERE note_created >= $3), 0),
			COALESCE(SUM(v) FILTER (WHERE note_created >= $7), 0),
			COALESCE(SUM(v), 0)
		FROM (
			SELECT e.created_at AS note_created, zr.amount_sats * 1000 * `+engagerWeightCaseSQL+` AS v
			FROM zap_receipts zr
			JOIN events e ON e.id = zr.event_id
			LEFT JOIN trust_pubkeys_latest t ON t.pubkey = zr.sender_pubkey
			WHERE zr.receiver_pubkey = $1
			  AND zr.sender_pubkey <> $1
			  AND zr.created_at >= $2
			  AND zr.created_at <= $8
			  AND e.kind = 1
			  AND e.pubkey = $1
			  AND e.created_at >= $2
			  AND e.created_at <= $8
			  AND NOT EXISTS (
				SELECT 1 FROM thread_edges te WHERE te.child_event_id = e.id
			  )
		) weighted
	`, noteArgs...).Scan(&out.trendingZapMSats24h, &out.trendingZapMSats7d, &out.trendingZapMSats30d); err != nil {
		return out, fmt.Errorf("load weighted profile zap volume: %w", err)
	}

	// New followers: one weighted vote per follower gained in the window.
	// follower_gain_events holds true kind=3 edge-diff gains (self-follows
	// excluded at write), so contact-list rewrites by existing followers buy
	// nothing, and a ring of fresh untrusted accounts following a bot buys
	// zero rising momentum.
	if err := tx.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(w) FILTER (WHERE ts >= $3), 0),
			COALESCE(SUM(w) FILTER (WHERE ts >= $7), 0),
			COALESCE(SUM(w), 0)
		FROM (
			SELECT fge.gained_at AS ts, `+engagerWeightCaseSQL+` AS w
			FROM follower_gain_events fge
			LEFT JOIN trust_pubkeys_latest t ON t.pubkey = fge.follower_pubkey
			WHERE fge.followed_pubkey = $1
			  AND fge.gained_at >= $2
		) weighted
	`, baseArgs...).Scan(&out.newFollowers24h, &out.newFollowers7d, &out.newFollowers30d); err != nil {
		return out, fmt.Errorf("load weighted profile new followers: %w", err)
	}
	return out, nil
}
