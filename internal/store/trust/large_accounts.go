package trust

import (
	"context"
	"fmt"
	"math"
	"strings"
)

// LargeAccountFollowerFloor is the follower count at which an account is
// treated as in-network for ingest even when it sits outside the seed hop
// limit. The live firehose subscribes by kind and time only, so a relay that
// truncates the stream drops notes and nothing retries them. Popular accounts
// are who people open, and a 2-hop web of trust often misses them.
//
// The partial index in migrations/000105_profile_large_follower_index.sql
// uses this same floor. Queries must inline the number (not a bind parameter)
// so Postgres can match that index predicate.
const LargeAccountFollowerFloor int64 = 1000

// largeAccountFrontierRank sorts ahead of every non-negative trust rank.
// Claim and promotion both use ORDER BY trust_rank ASC.
func largeAccountFrontierRank(followers int64) int64 {
	if followers <= 0 {
		return 0
	}
	return -followers
}

// DemotedLargeAccountFrontierRank is assigned to a frontier row that used to
// be fetched as a large account and has since fallen below the floor, so it
// no longer jumps the queue.
const DemotedLargeAccountFrontierRank int64 = math.MaxInt64

func largeAccountFollowerPredicate() string {
	return fmt.Sprintf("follower_count >= %d", LargeAccountFollowerFloor)
}

// mergeFrontierCandidates reserves half of the frontier for trust-ranked
// pubkeys and fills the rest with large accounts, largest first. Large
// accounts already present in the trust list keep the large-account rank so
// they are fetched first. Leftover slots go back to whichever list still has
// room.
func mergeFrontierCandidates(trustRows, popular []TrustPubkeyCandidate, limit int) []TrustPubkeyCandidate {
	if limit <= 0 {
		return nil
	}
	reservedForTrust := limit / 2
	seen := make(map[string]struct{}, limit)
	out := make([]TrustPubkeyCandidate, 0, limit)

	appendUnique := func(rows []TrustPubkeyCandidate, cap int) {
		for _, candidate := range rows {
			if len(out) >= cap {
				return
			}
			pubkey := strings.TrimSpace(candidate.Pubkey)
			if pubkey == "" {
				continue
			}
			if _, ok := seen[pubkey]; ok {
				continue
			}
			seen[pubkey] = struct{}{}
			candidate.Pubkey = pubkey
			out = append(out, candidate)
		}
	}

	appendUnique(popular, limit-reservedForTrust)
	appendUnique(trustRows, limit)
	appendUnique(popular, limit)
	return out
}

// ListLargeAccountFrontierCandidates returns accounts at or above
// LargeAccountFollowerFloor, largest first, with a negative trust rank so
// frontier claim order fetches them before ordinary trust ranks.
func (s *Trust) ListLargeAccountFrontierCandidates(ctx context.Context, limit int) ([]TrustPubkeyCandidate, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		return nil, nil
	}
	query := fmt.Sprintf(`
		SELECT pubkey, follower_count
		FROM profile_public_stats
		WHERE %s
		ORDER BY follower_count DESC, pubkey ASC
		LIMIT $1
	`, largeAccountFollowerPredicate())
	rows, err := s.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("list large account frontier candidates: %w", err)
	}
	defer rows.Close()
	out := make([]TrustPubkeyCandidate, 0, limit)
	for rows.Next() {
		var pubkey string
		var followers int64
		if err := rows.Scan(&pubkey, &followers); err != nil {
			return nil, fmt.Errorf("scan large account frontier candidate: %w", err)
		}
		pubkey = strings.TrimSpace(pubkey)
		if pubkey == "" || followers < LargeAccountFollowerFloor {
			continue
		}
		out = append(out, TrustPubkeyCandidate{
			Pubkey: pubkey,
			Rank:   largeAccountFrontierRank(followers),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read large account frontier candidates: %w", err)
	}
	return out, nil
}
