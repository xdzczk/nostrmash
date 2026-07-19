package retention

import (
	"context"
	"fmt"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
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
	trimTag, err := s.pool.Exec(ctx, `
		WITH candidates AS (
			SELECT entity_type, entity_id
			FROM search_documents
			WHERE entity_type = 'note'
			  AND freshness < $1
			  AND length(body) > $2
			LIMIT $3
		)
		UPDATE search_documents sd
		SET body = left(sd.body, $2),
		    updated_at = now()
		FROM candidates c
		WHERE sd.entity_type = c.entity_type
		  AND sd.entity_id = c.entity_id
	`, freshnessBefore.UTC(), maxBodyChars, batchLimit)
	metrics.ObserveDBOperation("groom_search_documents_trim", dbResultFromErr(err), time.Since(trimStarted))
	if err != nil {
		return 0, 0, fmt.Errorf("trim stale search document bodies: %w", err)
	}
	trimmed = trimTag.RowsAffected()

	pruneStarted := time.Now()
	pruneTag, err := s.pool.Exec(ctx, `
		WITH candidates AS (
			SELECT entity_type, entity_id
			FROM search_documents sd
			WHERE sd.entity_type = 'note'
			  AND NOT EXISTS (
				SELECT 1 FROM events e WHERE e.id = sd.entity_id
			  )
			LIMIT $1
		)
		DELETE FROM search_documents sd
		USING candidates c
		WHERE sd.entity_type = c.entity_type
		  AND sd.entity_id = c.entity_id
	`, batchLimit)
	metrics.ObserveDBOperation("groom_search_documents_prune", dbResultFromErr(err), time.Since(pruneStarted))
	if err != nil {
		return trimmed, 0, fmt.Errorf("prune orphaned search documents: %w", err)
	}
	return trimmed, pruneTag.RowsAffected(), nil
}
