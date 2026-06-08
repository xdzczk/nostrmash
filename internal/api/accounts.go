package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/account"
	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/store"
)

// accountCoverageStaleAfter is how old the last successful hydration may be
// before coverage is reported as stale.
const accountCoverageStaleAfter = 7 * 24 * time.Hour

type accountCoverageResponse struct {
	Pubkey                    string     `json:"pubkey"`
	State                     string     `json:"state"`
	Completeness              string     `json:"completeness"`
	ObservedCount             int64      `json:"observed_count"`
	FirstTrackedAt            *time.Time `json:"first_tracked_at,omitempty"`
	LastHydratedAt            *time.Time `json:"last_hydrated_at,omitempty"`
	LastSuccessfulHydrationAt *time.Time `json:"last_successful_hydration_at,omitempty"`
	OldestKnownNoteAt         *time.Time `json:"oldest_known_note_at,omitempty"`
	NewestKnownNoteAt         *time.Time `json:"newest_known_note_at,omitempty"`
	CoverageWindowDays        *int       `json:"coverage_window_days,omitempty"`
}

type accountHydrateResponse struct {
	Pubkey string `json:"pubkey"`
	Status string `json:"status"`
}

// GetAccountStatus reports what NostrMash knows about an account: its lifecycle
// state and a completeness classification (complete/partial/stale/hydrating/
// not_tracked). Public, read-only.
func (h Handlers) GetAccountStatus(w http.ResponseWriter, r *http.Request) {
	pubkey, ok := normalizePubkeyParam(r.PathValue("pubkey"))
	if !ok {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey must be 64 hex characters")
		return
	}
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "dependency_unavailable", "account state is not available")
		return
	}
	resp, err := buildAccountCoverage(r.Context(), h.pool, pubkey, h.hydration)
	if err != nil {
		writeInternalError(r.Context(), w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// HydrateAccount is the public, rate-limited, deduped on-demand hydration
// trigger. It is only functional when HYDRATION_PUBLIC_ENABLED; otherwise it
// returns 403 (operators use the authenticated admin trigger).
func (h Handlers) HydrateAccount(w http.ResponseWriter, r *http.Request) {
	pubkey, ok := normalizePubkeyParam(r.PathValue("pubkey"))
	if !ok {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey must be 64 hex characters")
		return
	}
	if !h.hydration.Enabled {
		writeError(r.Context(), w, http.StatusForbidden, "hydration_disabled", "hydration is disabled")
		return
	}
	if !h.hydration.PublicEnabled {
		writeError(r.Context(), w, http.StatusForbidden, "hydration_public_disabled", "public hydration is disabled")
		return
	}
	if h.hydrationLimiter != nil && !h.hydrationLimiter.Allow() {
		writeError(r.Context(), w, http.StatusTooManyRequests, "rate_limited", "hydration rate limit exceeded")
		return
	}
	if h.pool == nil {
		writeError(r.Context(), w, http.StatusServiceUnavailable, "dependency_unavailable", "hydration is not available")
		return
	}
	status, err := enqueueHydration(r.Context(), h.pool, h.hydration, pubkey, "public_api", "public")
	if err != nil {
		writeInternalError(r.Context(), w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, accountHydrateResponse{Pubkey: pubkey, Status: status})
}

// buildAccountCoverage assembles the coverage view for a pubkey from
// account_states plus in-flight hydration presence.
func buildAccountCoverage(ctx context.Context, pool *pgxpool.Pool, pubkey string, cfg config.HydrationConfig) (accountCoverageResponse, error) {
	pg := store.NewPostgresStore(pool)
	row, err := pg.GetAccountState(ctx, pubkey)
	if err != nil {
		return accountCoverageResponse{}, err
	}
	inflight, err := pg.HasInflightHydration(ctx, pubkey)
	if err != nil {
		return accountCoverageResponse{}, err
	}
	state, _ := account.Parse(row.State)
	fullCoverageDays := cfg.MaxLookbackDays
	if fullCoverageDays <= 0 {
		fullCoverageDays = 90
	}
	completeness := account.ResolveCompleteness(account.CompletenessInputs{
		State:                     state,
		Exists:                    row.Exists,
		InFlightHydration:         inflight,
		LastHydratedAt:            row.LastHydratedAt,
		LastSuccessfulHydrationAt: row.LastSuccessfulHydrationAt,
		CoverageWindowDays:        row.CoverageWindowDays,
		StaleAfter:                accountCoverageStaleAfter,
		FullCoverageDays:          fullCoverageDays,
		Now:                       time.Now().UTC(),
	})
	return accountCoverageResponse{
		Pubkey:                    pubkey,
		State:                     string(state),
		Completeness:              string(completeness),
		ObservedCount:             row.ObservedCount,
		FirstTrackedAt:            row.FirstTrackedAt,
		LastHydratedAt:            row.LastHydratedAt,
		LastSuccessfulHydrationAt: row.LastSuccessfulHydrationAt,
		OldestKnownNoteAt:         row.OldestKnownNoteAt,
		NewestKnownNoteAt:         row.NewestKnownNoteAt,
		CoverageWindowDays:        row.CoverageWindowDays,
	}, nil
}

// enqueueHydration enqueues a hydrate_account job after dedup/cooldown checks.
// It returns the resulting status: "queued", "hydrating" (already in flight),
// "cooldown" (hydrated too recently), or "disabled" (storage pressure).
func enqueueHydration(ctx context.Context, pool *pgxpool.Pool, cfg config.HydrationConfig, pubkey, reason, requestedBy string) (string, error) {
	pg := store.NewPostgresStore(pool)

	if st, err := pg.GetStoragePressureState(ctx); err == nil {
		if st.Level >= int(config.PressureDisableHydration) {
			return "disabled", nil
		}
	}

	inflight, err := pg.HasInflightHydration(ctx, pubkey)
	if err != nil {
		return "", err
	}
	if inflight {
		return "hydrating", nil
	}

	if cfg.Cooldown > 0 {
		if row, err := pg.GetAccountState(ctx, pubkey); err == nil {
			if row.LastHydratedAt != nil && time.Since(*row.LastHydratedAt) < cfg.Cooldown {
				return "cooldown", nil
			}
		}
	}

	payload, err := json.Marshal(jobs.HydrateAccountPayload{
		Pubkey:      pubkey,
		Reason:      reason,
		RequestedBy: requestedBy,
	})
	if err != nil {
		return "", err
	}
	// No idempotency key: dedup is handled by the HasInflightHydration check
	// above plus the per-account cooldown. A static key would otherwise pin to a
	// completed-but-not-yet-purged job and silently block all re-hydration until
	// job retention swept it.
	queue := jobs.NewQueue(pool)
	if _, err := queue.Enqueue(ctx, jobs.EnqueueParams{
		JobType:     jobs.JobTypeHydrateAccount,
		Payload:     payload,
		MaxAttempts: 3,
	}); err != nil {
		return "", err
	}
	return "queued", nil
}

func normalizePubkeyParam(raw string) (string, bool) {
	pubkey := strings.ToLower(strings.TrimSpace(raw))
	if len(pubkey) != 64 {
		return "", false
	}
	for _, c := range pubkey {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return "", false
		}
	}
	return pubkey, true
}

// minuteRateLimiter is a simple fixed-window per-minute limiter for public
// hydration triggers.
type minuteRateLimiter struct {
	mu          sync.Mutex
	limit       int
	windowStart time.Time
	count       int
}

func newMinuteRateLimiter(limit int) *minuteRateLimiter {
	return &minuteRateLimiter{limit: limit, windowStart: time.Now()}
}

func (l *minuteRateLimiter) Allow() bool {
	if l == nil || l.limit <= 0 {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if now.Sub(l.windowStart) >= time.Minute {
		l.windowStart = now
		l.count = 0
	}
	if l.count >= l.limit {
		return false
	}
	l.count++
	return true
}
