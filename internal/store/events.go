package store

import (
	"errors"

	"github.com/xdzczk/nostrmash/internal/readmodel"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/store/account"
	storeread "github.com/xdzczk/nostrmash/internal/store/read"
	"github.com/xdzczk/nostrmash/internal/store/retention"
	storetrust "github.com/xdzczk/nostrmash/internal/store/trust"
)

// PostgresStore persists Layer 1 ingest records into Postgres.
//
// Bounded-context data access is being carved into sub-packages (see
// internal/store/account) that PostgresStore embeds so its public method set
// and interface satisfaction stay intact for existing callers.
type PostgresStore struct {
	pool         *pgxpool.Pool
	jobPublisher jobs.CanonicalEventPublisher

	*account.Accounts
	*retention.Retention
	*storetrust.Trust
	*storeread.Read
}

// CanonicalInsertResult exposes idempotent upsert outcomes for metrics.
type CanonicalInsertResult struct {
	EventInserted bool
}

// ErrNotFound is re-exported from readmodel so query-layer callers can compare
// against it without importing the concrete store package.
var ErrNotFound = readmodel.ErrNotFound

// ProfileProjection is the latest projected profile metadata for one pubkey.
type ProfileProjection = readmodel.ProfileProjection

// ProfilePublicStatsProjection captures denormalized public-facing profile counters.
type ProfilePublicStatsProjection = readmodel.ProfilePublicStatsProjection

type EventCounts = readmodel.EventCounts

type ThreadSummaryProjection = readmodel.ThreadSummaryProjection

type HotConversation = readmodel.HotConversation

type EventWithProvenance = readmodel.EventWithProvenance

type EventOrderCursor = readmodel.EventOrderCursor

type AuthorAnalyticsSummaryProjection = readmodel.AuthorAnalyticsSummaryProjection

type AuthorRelayFootprintProjection = readmodel.AuthorRelayFootprintProjection

type AuthorQuoteRepostWindowProjection = readmodel.AuthorQuoteRepostWindowProjection

type QuoteRepostLinkedNoteProjection = readmodel.QuoteRepostLinkedNoteProjection

type QuoteRepostActivityProjection = readmodel.QuoteRepostActivityProjection

type NoteQuoteRepostLinkageProjection = readmodel.NoteQuoteRepostLinkageProjection

type AuthorTopicStatsProjection = readmodel.AuthorTopicStatsProjection

type EventDomainLinkProjection = readmodel.EventDomainLinkProjection

type DomainStatProjection = readmodel.DomainStatProjection

type DomainActivityProjection = readmodel.DomainActivityProjection

type DomainActivityStatsProjection = readmodel.DomainActivityStatsProjection

type DomainSummaryProjection = readmodel.DomainSummaryProjection

type AuthorMediaMixStatsProjection = readmodel.AuthorMediaMixStatsProjection

type AuthorActivityWindowBucketProjection = readmodel.AuthorActivityWindowBucketProjection

type AuthorPostingPatternBucketProjection = readmodel.AuthorPostingPatternBucketProjection

type AuthorTopNoteProjection = readmodel.AuthorTopNoteProjection

type AuthorRecycleCandidateProjection = readmodel.AuthorRecycleCandidateProjection

type AuthorPerformanceAggregateProjection = readmodel.AuthorPerformanceAggregateProjection

type GroupedNoteAnalyticsQuery = readmodel.GroupedNoteAnalyticsQuery

type GroupedEngagementTotalsProjection = readmodel.GroupedEngagementTotalsProjection

type GroupedMediaSummaryProjection = readmodel.GroupedMediaSummaryProjection

type GroupedTopNoteProjection = readmodel.GroupedTopNoteProjection

type GroupedTopicSummaryProjection = readmodel.GroupedTopicSummaryProjection

type GroupedNoteAnalyticsProjection = readmodel.GroupedNoteAnalyticsProjection

type SearchDocumentProjection = readmodel.SearchDocumentProjection

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{
		pool:         pool,
		jobPublisher: jobs.NewQueuePublisher(5),
		Accounts:     account.New(pool),
		Retention:    retention.New(pool),
		Trust:        storetrust.New(pool),
		Read:         storeread.New(pool),
	}
}

// SetCanonicalEventJobPublisher overrides canonical-event downstream publication.
func (s *PostgresStore) SetCanonicalEventJobPublisher(publisher jobs.CanonicalEventPublisher) {
	if s == nil {
		return
	}
	s.jobPublisher = publisher
}

func dbResultFromErr(err error) string {
	if err == nil {
		return "ok"
	}
	if errors.Is(err, ErrNotFound) {
		return "not_found"
	}
	return "error"
}
