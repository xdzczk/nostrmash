package derivation

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// EngagementWeightingOptions controls how much each engager pubkey
// contributes to note discovery trending scores.
//
// Regardless of these options, each pubkey counts at most once per note per
// signal (reply / repost / reaction / zap count) and the note author's own
// engagement never counts — counting raw events let a single account react
// its way to the top of Discovery.
//
// With TrustWeighted enabled, an engager's vote is scaled by trust-graph
// proximity from trust_pubkeys_latest instead of counting 1.0: hops <= 1 →
// 1.0, hops 2 → 0.5, hops 3..MaxHops → 0.25, outside the trust graph →
// UntrustedWeight. Zap msats are scaled per receipt by the sender's weight.
// This bounds engagement-farming rings by the trust edges real users extend
// to them: identities and events are free, trust-graph proximity is not.
type EngagementWeightingOptions struct {
	// TrustWeighted enables trust-graph proximity weighting. Off (default)
	// every non-author engager counts 1.0, preserving trust-free scoring.
	TrustWeighted bool
	// UntrustedWeight is the vote of an engager absent from the trust graph
	// (or beyond MaxHops) when TrustWeighted is on. 0 ignores them entirely.
	UntrustedWeight float64
	// MaxHops bounds the trusted hop ladder; values <= 0 fall back to 3,
	// matching the TRUST_MAX_HOPS default.
	MaxHops int
}

func (o EngagementWeightingOptions) normalized() EngagementWeightingOptions {
	if o.MaxHops <= 0 {
		o.MaxHops = 3
	}
	if o.UntrustedWeight < 0 {
		o.UntrustedWeight = 0
	}
	return o
}

// engagerWeightCaseSQL scores one engager row given a LEFT JOIN alias t on
// trust_pubkeys_latest. Placeholders: $4 = trust weighting enabled,
// $5 = max trusted hops, $6 = untrusted weight.
const engagerWeightCaseSQL = `
	CASE
		WHEN NOT $4::boolean THEN 1.0
		WHEN t.min_hops IS NULL OR t.min_hops > $5::int THEN $6::double precision
		WHEN t.min_hops <= 1 THEN 1.0
		WHEN t.min_hops = 2 THEN 0.5
		ELSE 0.25
	END`

type windowedEngagementWeights struct {
	ReplyWeight    float64
	RepostWeight   float64
	ReactionWeight float64
	ZapWeight      float64
	ZapMSats       float64
}

// loadWindowedEngagementWeights aggregates deduplicated, author-excluded,
// optionally trust-weighted engagement for one note within a time window.
func loadWindowedEngagementWeights(
	ctx context.Context,
	tx pgx.Tx,
	noteID string,
	authorPubkey string,
	nowUnix int64,
	window time.Duration,
	weighting EngagementWeightingOptions,
) (windowedEngagementWeights, error) {
	weighting = weighting.normalized()
	cutoff := nowUnix - int64(window/time.Second)
	args := []any{noteID, cutoff, authorPubkey, weighting.TrustWeighted, weighting.MaxHops, weighting.UntrustedWeight}

	var out windowedEngagementWeights
	var err error
	out.ReplyWeight, err = queryFloat64Tx(ctx, tx, `
		SELECT COALESCE(SUM(`+engagerWeightCaseSQL+`), 0)
		FROM (
			SELECT DISTINCT e.pubkey
			FROM reply_count_contributions c
			JOIN events e ON e.id = c.source_event_id
			WHERE c.target_event_id = $1
			  AND e.created_at >= $2
			  AND e.pubkey <> $3
		) g
		LEFT JOIN trust_pubkeys_latest t ON t.pubkey = g.pubkey
	`, args...)
	if err != nil {
		return out, fmt.Errorf("load windowed reply weight: %w", err)
	}
	out.RepostWeight, err = queryFloat64Tx(ctx, tx, `
		SELECT COALESCE(SUM(`+engagerWeightCaseSQL+`), 0)
		FROM (
			SELECT DISTINCT reposter_pubkey AS pubkey
			FROM repost_events
			WHERE target_event_id = $1
			  AND created_at >= $2
			  AND reposter_pubkey <> $3
		) g
		LEFT JOIN trust_pubkeys_latest t ON t.pubkey = g.pubkey
	`, args...)
	if err != nil {
		return out, fmt.Errorf("load windowed repost weight: %w", err)
	}
	out.ReactionWeight, err = queryFloat64Tx(ctx, tx, `
		SELECT COALESCE(SUM(`+engagerWeightCaseSQL+`), 0)
		FROM (
			SELECT DISTINCT reactor_pubkey AS pubkey
			FROM reaction_events
			WHERE target_event_id = $1
			  AND created_at >= $2
			  AND reactor_pubkey <> $3
		) g
		LEFT JOIN trust_pubkeys_latest t ON t.pubkey = g.pubkey
	`, args...)
	if err != nil {
		return out, fmt.Errorf("load windowed reaction weight: %w", err)
	}
	out.ZapWeight, err = queryFloat64Tx(ctx, tx, `
		SELECT COALESCE(SUM(`+engagerWeightCaseSQL+`), 0)
		FROM (
			SELECT DISTINCT sender_pubkey AS pubkey
			FROM zap_receipts
			WHERE event_id = $1
			  AND created_at >= $2
			  AND sender_pubkey <> $3
		) g
		LEFT JOIN trust_pubkeys_latest t ON t.pubkey = g.pubkey
	`, args...)
	if err != nil {
		return out, fmt.Errorf("load windowed zap weight: %w", err)
	}
	// Zap amounts are not deduplicated (each sat is a distinct cost), but each
	// receipt is scaled by its sender's weight: self-zaps and circular zaps
	// from outside the trust graph are nearly free to an attacker, so they
	// must not buy score.
	out.ZapMSats, err = queryFloat64Tx(ctx, tx, `
		SELECT COALESCE(SUM(z.amount_sats * 1000 * `+engagerWeightCaseSQL+`), 0)
		FROM zap_receipts z
		LEFT JOIN trust_pubkeys_latest t ON t.pubkey = z.sender_pubkey
		WHERE z.event_id = $1
		  AND z.created_at >= $2
		  AND z.sender_pubkey <> $3
	`, args...)
	if err != nil {
		return out, fmt.Errorf("load windowed zap msats weight: %w", err)
	}
	return out, nil
}

// loadWindowedThreadReplyWeight is the thread-root variant of the reply
// weight: unique repliers anywhere in the thread, same exclusion/weighting.
func loadWindowedThreadReplyWeight(
	ctx context.Context,
	tx pgx.Tx,
	rootEventID string,
	authorPubkey string,
	nowUnix int64,
	window time.Duration,
	weighting EngagementWeightingOptions,
) (float64, error) {
	weighting = weighting.normalized()
	cutoff := nowUnix - int64(window/time.Second)
	weight, err := queryFloat64Tx(ctx, tx, `
		SELECT COALESCE(SUM(`+engagerWeightCaseSQL+`), 0)
		FROM (
			SELECT DISTINCT child.pubkey
			FROM thread_edges te
			JOIN events child ON child.id = te.child_event_id
			WHERE te.root_event_id = $1
			  AND te.child_created_at >= $2
			  AND child.pubkey <> $3
		) g
		LEFT JOIN trust_pubkeys_latest t ON t.pubkey = g.pubkey
	`, rootEventID, cutoff, authorPubkey, weighting.TrustWeighted, weighting.MaxHops, weighting.UntrustedWeight)
	if err != nil {
		return 0, fmt.Errorf("load windowed thread reply weight: %w", err)
	}
	return weight, nil
}

func hasThreadSummary(ctx context.Context, tx pgx.Tx, noteID string) (bool, error) {
	var exists bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM thread_summaries WHERE root_event_id = $1)
	`, noteID).Scan(&exists); err != nil {
		return false, fmt.Errorf("check thread summary existence: %w", err)
	}
	return exists, nil
}
