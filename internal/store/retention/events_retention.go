package retention

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store/retention/retentiondb"
)

// giftWrapMaxAge is how long a kind-1059 gift wrap stays after we first
// stored it. The shared engagement horizon is much longer; ciphertext
// from an ephemeral author should not wait that long.
const giftWrapMaxAge = 48 * time.Hour

// PurgeExpiredEngagementEvents deletes a bounded batch of raw engagement events
// (kinds 6/7/9735) whose author-claimed created_at is older than createdBefore,
// in ascending created_at order. Kind 1059 is included in that set and is
// also deleted once first_seen_at is older than giftWrapMaxAge, whichever
// comes first. Cascade FKs clean the dependent
// contribution/interaction rows; lifetime aggregate counters in
// reaction_counts/repost_counts have no FK to events and therefore survive.
//
// Derivation safety: an event is skipped while its derive_event_bundle job is
// pending or running, and also while that job is dead but was last updated
// after deadGraceBefore (i.e. within the operator's dead-grace window). Dead
// jobs older than deadGraceBefore no longer block the purge so a permanently
// broken derivation cannot pin disk forever.
//
// The (kind, created_at) scan is served by idx_events_kind_created; the
// per-candidate jobs lookup is served by the unique index on
// jobs.idempotency_key.
//
// Before deleting, each candidate's incremental author-stat deltas are
// reversed (see IncrementalStatsReverser) so profile_public_stats /
// author_activity_daily / author_hashtag_daily / author_media_daily /
// author_hourly_activity counters don't drift upward as engagement events
// age out. The candidate selection and the delete happen in the same
// transaction, so the exact set of ids that was reversed is the exact set
// deleted.
func (s *Retention) PurgeExpiredEngagementEvents(
	ctx context.Context,
	createdBefore time.Time,
	deadGraceBefore time.Time,
	limit int,
) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		return 0, fmt.Errorf("limit must be > 0")
	}
	if createdBefore.IsZero() {
		return 0, fmt.Errorf("createdBefore is required")
	}
	if deadGraceBefore.IsZero() {
		return 0, fmt.Errorf("deadGraceBefore is required")
	}

	started := time.Now()
	giftWrapBefore := started.UTC().Add(-giftWrapMaxAge)
	var rows int64
	err := s.guardedWithTx(ctx, func(tx pgx.Tx, q *retentiondb.Queries) error {
		ids, err := q.SelectExpiredGiftWrapCandidates(ctx, retentiondb.SelectExpiredGiftWrapCandidatesParams{
			SeenBefore:      tsz(giftWrapBefore),
			DeadGraceBefore: tsz(deadGraceBefore.UTC()),
			RowLimit:        int32(limit),
		})
		if err != nil {
			return fmt.Errorf("select expired gift wrap candidates: %w", err)
		}
		remaining := limit - len(ids)
		if remaining > 0 {
			more, err := q.SelectExpiredEngagementEventCandidates(ctx, retentiondb.SelectExpiredEngagementEventCandidatesParams{
				CreatedBeforeUnix: createdBefore.UTC().Unix(),
				DeadGraceBefore:   tsz(deadGraceBefore.UTC()),
				RowLimit:          int32(remaining),
			})
			if err != nil {
				return fmt.Errorf("select expired engagement event candidates: %w", err)
			}
			ids = appendUniqueIDs(ids, more)
		}
		rows, err = s.reverseAndDeleteTx(ctx, tx, q, ids)
		return err
	})
	metrics.ObserveDBOperation("purge_expired_engagement_events", dbResultFromErr(err), time.Since(started))
	if err != nil {
		return 0, fmt.Errorf("purge expired engagement events: %w", err)
	}
	return rows, nil
}

func appendUniqueIDs(dst, more []string) []string {
	if len(more) == 0 {
		return dst
	}
	seen := make(map[string]struct{}, len(dst)+len(more))
	for _, id := range dst {
		seen[id] = struct{}{}
	}
	for _, id := range more {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		dst = append(dst, id)
	}
	return dst
}
