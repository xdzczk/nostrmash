package derivation

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// recordFollowerGainEventsTx persists one follower_gain_events row per true
// edge-diff gain in a kind=3 contact-list rewrite: pubkeys on the new list
// that were absent from the author's previous follower_edges set. This is
// the source every "new followers" reader uses (the discovery payload's
// recent_new_followers and both the raw and trust-weighted rising-score
// inputs), replacing the old follower_edges.contact_list_created_at window
// scan under which every list rewrite re-counted all of the author's
// existing follows as "new followers".
//
// Unlike the profile_public_stats follower deltas (gated behind
// WORKER_INCREMENTAL_PROFILE_PUBLIC_STATS), gains are recorded
// unconditionally so the readers work regardless of the incremental-stats
// flags. The applied_stat_deltas claim keys writes per event, making
// re-projection of the same contact list exactly-once; the (followed,
// follower) primary key with DO NOTHING dedupes unfollow/refollow churn
// inside the retention horizon (the original gained_at is kept). Self
// follows never count, matching every other discovery signal.
func (h *Handlers) recordFollowerGainEventsTx(
	ctx context.Context,
	tx pgx.Tx,
	eventID, authorPubkey string,
	previousFollowed, contacts []string,
	contactListCreatedAt int64,
	writeVersion int,
) error {
	claimed, err := claimStatDeltaTx(ctx, tx, eventID, statDeltaFollowerGainEvents)
	if err != nil || !claimed {
		return err
	}

	prev := make(map[string]struct{}, len(previousFollowed))
	for _, pubkey := range previousFollowed {
		pubkey = strings.TrimSpace(pubkey)
		if pubkey == "" {
			continue
		}
		prev[pubkey] = struct{}{}
	}
	gained := make([]string, 0)
	for _, followed := range contacts {
		followed = strings.TrimSpace(followed)
		if followed == "" || followed == authorPubkey {
			continue
		}
		if _, ok := prev[followed]; ok {
			continue
		}
		gained = append(gained, followed)
	}
	if len(gained) == 0 {
		return nil
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO follower_gain_events (
			followed_pubkey, follower_pubkey, gained_at, derivation_version
		)
		SELECT DISTINCT followed, $2::text, $3::bigint, $4::integer
		FROM unnest($1::text[]) AS followed
		WHERE followed <> $2::text
		ON CONFLICT (followed_pubkey, follower_pubkey) DO NOTHING
	`, gained, authorPubkey, contactListCreatedAt, writeVersion); err != nil {
		return fmt.Errorf("record follower gain events for %s: %w", authorPubkey, err)
	}
	return nil
}
