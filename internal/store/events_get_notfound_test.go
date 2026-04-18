package store

import (
	"context"
	"errors"
	"testing"
)

func TestGetEventRawByID_NotFound(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	s := NewPostgresStore(pool)
	_, err := s.GetEventRawByID(ctx, "missing_event")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
