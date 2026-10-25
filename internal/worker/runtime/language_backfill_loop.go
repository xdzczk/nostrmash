package runtime

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/meili"
)

const (
	languageBackfillTick         = 5 * time.Second
	languageBackfillMeiliRetry   = 5 * time.Minute
	languageBackfillMeiliTimeout = 45 * time.Minute
	languageBackfillLogStride    = 5000
	languageBackfillMeiliBatch   = 1000
)

// RunLanguageBackfillLoop drains the 7-day language cursor. A missing or
// finished row is a no-op so the loop can stay up for a later admin start.
// Search sync runs once, after notes and profile languages are written.
func RunLanguageBackfillLoop(ctx context.Context, log Logger, handlers *derivation.Handlers, client *meili.Client, pool *pgxpool.Pool) {
	if handlers == nil {
		log.Error("language_backfill_no_handlers")
		return
	}
	if derivation.LanguageBackfillAutostart() {
		if err := handlers.EnsureLanguageBackfill(ctx); err != nil {
			log.Error("language_backfill_ensure_failed", "error", err)
		}
	}
	var lastPhase string
	var lastLogged int64
	ticker := time.NewTicker(languageBackfillTick)
	defer ticker.Stop()
	for {
		runLanguageBackfillTick(ctx, log, handlers, client, pool, &lastPhase, &lastLogged)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func runLanguageBackfillTick(
	ctx context.Context,
	log Logger,
	handlers *derivation.Handlers,
	client *meili.Client,
	pool *pgxpool.Pool,
	lastPhase *string,
	lastLogged *int64,
) {
	defer func() {
		if recovered := recover(); recovered != nil {
			log.Error("language_backfill_panicked", "panic", recovered)
		}
	}()
	status, ok, err := handlers.LanguageBackfillStatus(ctx)
	if err != nil {
		log.Error("language_backfill_status_failed", "error", err)
		return
	}
	if !ok || status.Phase == derivation.LanguageBackfillPhaseDone {
		return
	}
	if status.Phase == derivation.LanguageBackfillPhaseMeili {
		runLanguageBackfillMeili(ctx, log, handlers, client, pool, status)
		return
	}
	next, err := handlers.RunLanguageBackfillBatch(ctx)
	if err != nil {
		log.Error("language_backfill_batch_failed", "phase", status.Phase, "error", err)
		return
	}
	if next.Phase != *lastPhase || next.Scanned-*lastLogged >= languageBackfillLogStride {
		log.Info("language_backfill_progress", "phase", next.Phase, "scanned", next.Scanned, "updated", next.Updated)
		*lastPhase = next.Phase
		*lastLogged = next.Scanned
	}
}

func runLanguageBackfillMeili(
	ctx context.Context,
	log Logger,
	handlers *derivation.Handlers,
	client *meili.Client,
	pool *pgxpool.Pool,
	status derivation.LanguageBackfillStatus,
) {
	if status.LastError != "" && time.Since(status.UpdatedAt) < languageBackfillMeiliRetry {
		return
	}
	if client == nil || !client.Enabled() {
		if err := handlers.FinishLanguageBackfill(ctx); err != nil {
			log.Error("language_backfill_finish_failed", "error", err)
		}
		return
	}
	syncCtx, cancel := context.WithTimeout(ctx, languageBackfillMeiliTimeout)
	defer cancel()
	if _, err := client.FullSync(syncCtx, pool, languageBackfillMeiliBatch); err != nil {
		log.Error("language_backfill_meili_failed", "error", err)
		if recordErr := handlers.NoteLanguageBackfillError(ctx, err); recordErr != nil {
			log.Error("language_backfill_error_record_failed", "error", recordErr)
		}
		return
	}
	if err := handlers.FinishLanguageBackfill(ctx); err != nil {
		log.Error("language_backfill_finish_failed", "error", err)
	}
}
