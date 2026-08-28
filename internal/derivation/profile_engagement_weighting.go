package derivation

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// profileWeightedScoreInputs replaces the raw engagement-received /
// zap-volume / new-follower score inputs of profile trending and rising
// scores when trust-weighted discovery engagement is enabled.
//
// Engagement is deduplicated per (signal, engager, target event): a bot
// reacting to the same note a thousand times — or replying to every one of
// its own alt's posts — counts once per note per signal, and each vote is
// scaled by the engager's trust-graph proximity (engagerWeightCaseSQL).
// Zap volume is scaled per receipt by the sender's weight. New followers are
// weighted per distinct follower. Self-engagement never counts.
//
// The raw incremental counters (author_hourly_activity and friends) are
// deliberately left untouched: they feed analytics displays and the
// incremental-vs-full reconciliation, so their semantics must not fork.
// Only the discovery score inputs swap to the weighted values.
type profileWeightedScoreInputs struct {
	engagement24h   float64
	engagement7d    float64
	zapMSats24h     float64
	zapMSats7d      float64
	newFollowers24h float64
	newFollowers7d  float64
}

// loadProfileWeightedScoreInputsTx scans windowed engagement toward one
// profile and aggregates deduplicated, self-excluded, trust-weighted score
// inputs. This rescans raw engagement tables bounded by the 7d window, so it
// only runs when TRUST_DISCOVERY_ENGAGEMENT_WEIGHTING is enabled.
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
	args := []any{pubkey, cutoff7d, cutoff24h, weighting.TrustWeighted, weighting.MaxHops, weighting.UntrustedWeight}

	var out profileWeightedScoreInputs

	// One weighted vote per (signal, engager, target event). last_ts decides
	// 24h membership: a tuple counts in the 24h window when any of its raw
	// events falls inside it. Zaps without an event target (profile zaps)
	// collapse to one tuple per sender.
	if err := tx.QueryRow(ctx, `
		WITH tuples AS (
			SELECT src, engager, MAX(ts) AS last_ts
			FROM (
				SELECT 'reply'::text AS src, source_event.pubkey AS engager, c.target_event_id AS target, source_event.created_at AS ts
				FROM reply_count_contributions c
				JOIN events source_event ON source_event.id = c.source_event_id
				JOIN events target_event ON target_event.id = c.target_event_id
				WHERE target_event.pubkey = $1
				  AND source_event.pubkey <> $1
				  AND source_event.created_at >= $2
				UNION ALL
				SELECT 'repost', r.reposter_pubkey, r.target_event_id, r.created_at
				FROM repost_events r
				JOIN events target_event ON target_event.id = r.target_event_id
				WHERE target_event.pubkey = $1
				  AND r.reposter_pubkey <> $1
				  AND r.created_at >= $2
				UNION ALL
				SELECT 'reaction', r.reactor_pubkey, r.target_event_id, r.created_at
				FROM reaction_events r
				JOIN events target_event ON target_event.id = r.target_event_id
				WHERE target_event.pubkey = $1
				  AND r.reactor_pubkey <> $1
				  AND r.created_at >= $2
				UNION ALL
				SELECT 'zap', zr.sender_pubkey, COALESCE(zr.event_id, ''), zr.created_at
				FROM zap_receipts zr
				WHERE zr.receiver_pubkey = $1
				  AND zr.sender_pubkey <> $1
				  AND zr.created_at >= $2
			) raw
			GROUP BY src, engager, target
		)
		SELECT
			COALESCE(SUM(w) FILTER (WHERE last_ts >= $3), 0),
			COALESCE(SUM(w), 0)
		FROM (
			SELECT tu.last_ts, `+engagerWeightCaseSQL+` AS w
			FROM tuples tu
			LEFT JOIN trust_pubkeys_latest t ON t.pubkey = tu.engager
		) weighted
	`, args...).Scan(&out.engagement24h, &out.engagement7d); err != nil {
		return out, fmt.Errorf("load weighted profile engagement: %w", err)
	}

	// Zap volume: every sat counts (each is a distinct cost) but scaled by
	// the sender's trust weight, so circular sybil zaps buy no zap signal.
	if err := tx.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(v) FILTER (WHERE ts >= $3), 0),
			COALESCE(SUM(v), 0)
		FROM (
			SELECT zr.created_at AS ts, zr.amount_sats * 1000 * `+engagerWeightCaseSQL+` AS v
			FROM zap_receipts zr
			LEFT JOIN trust_pubkeys_latest t ON t.pubkey = zr.sender_pubkey
			WHERE zr.receiver_pubkey = $1
			  AND zr.sender_pubkey <> $1
			  AND zr.created_at >= $2
		) weighted
	`, args...).Scan(&out.zapMSats24h, &out.zapMSats7d); err != nil {
		return out, fmt.Errorf("load weighted profile zap volume: %w", err)
	}

	// New followers: one weighted vote per distinct follower whose current
	// edge appeared in the window. A ring of fresh untrusted accounts
	// following a bot buys zero rising momentum.
	if err := tx.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(w) FILTER (WHERE ts >= $3), 0),
			COALESCE(SUM(w), 0)
		FROM (
			SELECT fe.contact_list_created_at AS ts, `+engagerWeightCaseSQL+` AS w
			FROM follower_edges fe
			LEFT JOIN trust_pubkeys_latest t ON t.pubkey = fe.follower_pubkey
			WHERE fe.followed_pubkey = $1
			  AND fe.follower_pubkey <> $1
			  AND fe.contact_list_created_at >= $2
		) weighted
	`, args...).Scan(&out.newFollowers24h, &out.newFollowers7d); err != nil {
		return out, fmt.Errorf("load weighted profile new followers: %w", err)
	}
	return out, nil
}
