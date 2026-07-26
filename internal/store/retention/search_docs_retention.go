package retention

import (
	"context"
	"fmt"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store/retention/retentiondb"
)

// GroomSearchDocuments bounds the body-heavy search_documents projection with
// two passes executed in one call:
//
//  1. Body trim: note documents whose freshness is older than freshnessBefore
//     keep only the first maxBodyChars characters of body. The generated
//     search_tsv column shrinks with the body, so the GIN index shrinks too.
//     If the source note is re-indexed later, the full body is restored by
//     the normal sync path.
//  2. Orphan prune: note documents whose source event no longer exists
//     (e.g. removed by untrusted-author or engagement retention) are deleted.
//
// Profile / hashtag / identity / relay documents are small and bounded by
// their source cardinality, so both passes touch only entity_type = 'note'.
func (s *Retention) GroomSearchDocuments(
	ctx context.Context,
	freshnessBefore time.Time,
	maxBodyChars int,
	batchLimit int,
) (trimmed int64, pruned int64, err error) {
	if s == nil || s.pool == nil {
		return 0, 0, fmt.Errorf("store is not initialized")
	}
	if freshnessBefore.IsZero() {
		return 0, 0, fmt.Errorf("freshnessBefore is required")
	}
	if maxBodyChars <= 0 || batchLimit <= 0 {
		return 0, 0, fmt.Errorf("maxBodyChars and batchLimit must be > 0")
	}

	trimStarted := time.Now()
	err = s.guarded(ctx, func(q *retentiondb.Queries) error {
		var err error
		trimmed, err = q.GroomSearchDocumentsTrim(ctx, retentiondb.GroomSearchDocumentsTrimParams{
			MaxBodyChars:    int32(maxBodyChars),
			FreshnessBefore: tsz(freshnessBefore.UTC()),
			RowLimit:        int32(batchLimit),
		})
		return err
	})
	metrics.ObserveDBOperation("groom_search_documents_trim", dbResultFromErr(err), time.Since(trimStarted))
	if err != nil {
		return 0, 0, fmt.Errorf("trim stale search document bodies: %w", err)
	}

	pruneStarted := time.Now()
	err = s.guarded(ctx, func(q *retentiondb.Queries) error {
		var err error
		pruned, err = q.GroomSearchDocumentsPrune(ctx, int32(batchLimit))
		return err
	})
	metrics.ObserveDBOperation("groom_search_documents_prune", dbResultFromErr(err), time.Since(pruneStarted))
	if err != nil {
		return trimmed, 0, fmt.Errorf("prune orphaned search documents: %w", err)
	}
	return trimmed, pruned, nil
}
