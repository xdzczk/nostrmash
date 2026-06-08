package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/hydration"
	"github.com/xdzczk/nostrmash/internal/ingestor/live"
	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/nostr"
	"github.com/xdzczk/nostrmash/internal/store"
)

// hydrationRelaySource resolves the relay set for hydration: explicit config
// relays when set, otherwise the most recently active relays known locally.
type hydrationRelaySource struct {
	cfg   config.HydrationConfig
	store *store.PostgresStore
}

func (s hydrationRelaySource) Relays(ctx context.Context) ([]string, error) {
	if len(s.cfg.Relays) > 0 {
		return s.cfg.Relays, nil
	}
	limit := s.cfg.MaxRelays
	if limit <= 0 {
		limit = 16
	}
	return s.store.ListRecentRelayURLs(ctx, limit)
}

// buildHydrationService constructs the on-demand hydration service. The live
// processor is built WITHOUT a trust gate, so the explicitly-requested author
// is accepted by construction and the fetched notes are not dropped.
func buildHydrationService(log Logger, cfg config.WorkerConfig, pool *pgxpool.Pool, pgStore *store.PostgresStore) (*hydration.Service, error) {
	slogLogger, ok := any(log).(*slog.Logger)
	if !ok {
		slogLogger = slog.Default()
	}
	processor, err := live.NewProcessor(slogLogger, store.NewPostgresStore(pool), nostr.Options{})
	if err != nil {
		return nil, fmt.Errorf("build hydration processor: %w", err)
	}
	fetcher := hydration.WebsocketFetcher{
		ConnectTimeout: cfg.Hydration.ConnectTimeout,
		IdleTimeout:    cfg.Hydration.IdleTimeout,
	}
	source := hydrationRelaySource{cfg: cfg.Hydration, store: pgStore}
	return hydration.NewService(slogLogger, cfg.Hydration, pgStore, processor, fetcher, source), nil
}

// processHydrateAccountJob handles a hydrate_account job by running the bounded
// hydration service for the payload pubkey.
func processHydrateAccountJob(ctx context.Context, svc *hydration.Service, job jobs.Job) error {
	if svc == nil {
		return fmt.Errorf("hydration service unavailable")
	}
	var payload jobs.HydrateAccountPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return fmt.Errorf("decode hydrate payload: %w", err)
	}
	reason := payload.Reason
	if reason == "" {
		reason = "job"
	}
	if _, err := svc.Hydrate(ctx, payload.Pubkey, reason); err != nil {
		return fmt.Errorf("hydrate account: %w", err)
	}
	return nil
}
