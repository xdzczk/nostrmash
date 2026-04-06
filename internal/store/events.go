package store

import (
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/model"
)

// PostgresStore persists Layer 1 ingest records into Postgres.
type PostgresStore struct {
	pool         *pgxpool.Pool
	jobPublisher jobs.CanonicalEventPublisher
}

// CanonicalInsertResult exposes idempotent upsert outcomes for metrics.
type CanonicalInsertResult struct {
	EventInserted bool
}

var ErrNotFound = errors.New("not found")

// ProfileProjection is the latest projected profile metadata for one pubkey.
type ProfileProjection struct {
	Pubkey            string
	MetadataEventID   string
	MetadataCreatedAt int64
	ProfileJSON       json.RawMessage
}

type EventCounts struct {
	EventID       string
	ReplyCount    int64
	ReactionCount int64
	RepostCount   int64
	Consistency   string
}

type EventWithProvenance struct {
	Event  json.RawMessage
	Relays []model.EventRelay
}

type EventOrderCursor struct {
	CreatedAt int64
	ID        string
}

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{
		pool:         pool,
		jobPublisher: jobs.NewQueuePublisher(5),
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
