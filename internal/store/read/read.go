// Package read owns the read bounded context: parity queries, curated feeds,
// discovery/trending, analytics, and search-document reads. It is composed into
// the top-level store.PostgresStore via embedding so callers keep a single
// store handle. Read models are the neutral types from internal/readmodel; the
// aliases below let the moved query files keep referencing them unqualified.
package read

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

// Read is the read-context data-access surface backed by a shared pool.
type Read struct {
	pool *pgxpool.Pool
}

// New builds a read Store over the shared connection pool.
func New(pool *pgxpool.Pool) *Read {
	return &Read{pool: pool}
}

// ErrNotFound mirrors the store sentinel so read queries can return it directly.
var ErrNotFound = readmodel.ErrNotFound

// Neutral read-model aliases used by the moved read files. They resolve to the
// same underlying types as the store-package aliases, so callers observe one
// coherent type regardless of which package they reach it through.
type (
	EventOrderCursor                     = readmodel.EventOrderCursor
	ProfileProjection                    = readmodel.ProfileProjection
	HotConversation                      = readmodel.HotConversation
	SearchDocumentProjection             = readmodel.SearchDocumentProjection
	AuthorAnalyticsSummaryProjection     = readmodel.AuthorAnalyticsSummaryProjection
	AuthorRelayFootprintProjection       = readmodel.AuthorRelayFootprintProjection
	AuthorTopicStatsProjection           = readmodel.AuthorTopicStatsProjection
	AuthorMediaMixStatsProjection        = readmodel.AuthorMediaMixStatsProjection
	AuthorActivityWindowBucketProjection = readmodel.AuthorActivityWindowBucketProjection
	AuthorPostingPatternBucketProjection = readmodel.AuthorPostingPatternBucketProjection
	AuthorTopNoteProjection              = readmodel.AuthorTopNoteProjection
	AuthorRecycleCandidateProjection     = readmodel.AuthorRecycleCandidateProjection
	AuthorPerformanceAggregateProjection = readmodel.AuthorPerformanceAggregateProjection
	QuoteRepostActivityProjection        = readmodel.QuoteRepostActivityProjection
	NoteQuoteRepostLinkageProjection     = readmodel.NoteQuoteRepostLinkageProjection
	EventDomainLinkProjection            = readmodel.EventDomainLinkProjection
	DomainStatProjection                 = readmodel.DomainStatProjection
	DomainSummaryProjection              = readmodel.DomainSummaryProjection
	DomainActivityProjection             = readmodel.DomainActivityProjection
	DomainActivityStatsProjection        = readmodel.DomainActivityStatsProjection
	GroupedNoteAnalyticsQuery            = readmodel.GroupedNoteAnalyticsQuery
	GroupedNoteAnalyticsProjection       = readmodel.GroupedNoteAnalyticsProjection
	GroupedTopNoteProjection             = readmodel.GroupedTopNoteProjection
	GroupedTopicSummaryProjection        = readmodel.GroupedTopicSummaryProjection
	TrustQualificationPolicy             = readmodel.TrustQualificationPolicy
)

// dbResultFromErr maps an error to the {ok,not_found,error} metric label.
// Mirrors the parent-store helper; kept local so read has no back-dependency.
func dbResultFromErr(err error) string {
	if err == nil {
		return "ok"
	}
	if errors.Is(err, ErrNotFound) {
		return "not_found"
	}
	return "error"
}

// eventExists reports whether a canonical event row exists. Read-side note and
// thread queries use it to distinguish "unknown id" from "no related rows".
func (s *Read) eventExists(ctx context.Context, eventID string) (bool, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM events WHERE id = $1)
	`, eventID).Scan(&exists); err != nil {
		return false, fmt.Errorf("check event existence: %w", err)
	}
	return exists, nil
}
