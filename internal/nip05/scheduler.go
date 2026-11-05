package nip05

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Config controls the verification loop.
type Config struct {
	Enabled         bool
	Interval        time.Duration
	Batch           int
	Timeout         time.Duration
	RecheckInterval time.Duration
}

// RunLoop verifies due NIP-05 rows until ctx is cancelled.
func RunLoop(ctx context.Context, pool *pgxpool.Pool, cfg Config, log func(string, ...any)) {
	if pool == nil || !cfg.Enabled {
		if log != nil {
			log("nip05_verification_disabled")
		}
		return
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Minute
	}
	if cfg.Batch <= 0 {
		cfg.Batch = 25
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 8 * time.Second
	}
	if cfg.RecheckInterval <= 0 {
		cfg.RecheckInterval = 7 * 24 * time.Hour
	}
	fetcher := NewFetcher(cfg.Timeout)
	if log != nil {
		log("nip05_verification_enabled", "interval", cfg.Interval.String(), "batch", cfg.Batch)
	}
	runOnce(ctx, pool, fetcher, cfg)
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runOnce(ctx, pool, fetcher, cfg)
		}
	}
}

type dueRow struct {
	Pubkey       string
	NIP05        string
	Domain       string
	FailureCount int
}

func runOnce(ctx context.Context, pool *pgxpool.Pool, fetcher *Fetcher, cfg Config) {
	rows, err := claimDue(ctx, pool, cfg.Batch)
	if err != nil || len(rows) == 0 {
		return
	}
	for _, row := range rows {
		local, domain, ok := splitNIP05(row.NIP05)
		if !ok {
			domain = row.Domain
		}
		checkCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
		result := fetcher.Check(checkCtx, local, domain, row.Pubkey)
		cancel()
		_ = storeResult(ctx, pool, row, result, cfg.RecheckInterval)
	}
}

func claimDue(ctx context.Context, pool *pgxpool.Pool, batch int) ([]dueRow, error) {
	rows, err := pool.Query(ctx, `
		UPDATE nip05_verifications AS v
		SET next_check_at = now() + interval '2 minutes',
		    updated_at = now()
		WHERE v.pubkey IN (
			SELECT pubkey
			FROM nip05_verifications
			WHERE next_check_at <= now()
			  AND status <> 'blocked'
			ORDER BY next_check_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING v.pubkey, v.nip05, v.domain, v.failure_count
	`, batch)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]dueRow, 0, batch)
	for rows.Next() {
		var row dueRow
		if err := rows.Scan(&row.Pubkey, &row.NIP05, &row.Domain, &row.FailureCount); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func storeResult(ctx context.Context, pool *pgxpool.Pool, row dueRow, result Result, recheck time.Duration) error {
	failures := row.FailureCount
	status := result.Status
	next := time.Now().UTC().Add(recheck)
	switch status {
	case StatusVerified, StatusMismatch:
		failures = 0
	case StatusNotFound:
		failures = 0
		next = time.Now().UTC().Add(24 * time.Hour)
	case StatusInvalid:
		failures++
		next = time.Now().UTC().Add(24 * time.Hour)
	default:
		failures++
		backoff := 15 * time.Minute
		for i := 1; i < failures && backoff < 30*24*time.Hour; i++ {
			backoff *= 2
		}
		if backoff > 30*24*time.Hour {
			backoff = 30 * 24 * time.Hour
		}
		next = time.Now().UTC().Add(backoff)
		if failures >= 8 {
			status = StatusBlocked
			next = time.Now().UTC().Add(30 * 24 * time.Hour)
		} else if status == "" {
			status = StatusUnreachable
		}
	}
	_, err := pool.Exec(ctx, `
		UPDATE nip05_verifications
		SET status = $2,
		    checked_at = now(),
		    next_check_at = $3,
		    failure_count = $4,
		    last_error_code = $5,
		    updated_at = now()
		WHERE pubkey = $1
	`, row.Pubkey, status, next, failures, result.ErrorCode)
	return err
}

func splitNIP05(value string) (local, domain string, ok bool) {
	value = strings.TrimSpace(strings.ToLower(value))
	local, domain, ok = strings.Cut(value, "@")
	if !ok || local == "" || domain == "" || strings.Contains(domain, "@") {
		return "", "", false
	}
	return local, domain, true
}

// SplitIdentifier parses a NIP-05 identifier. "_" is a valid local part.
func SplitIdentifier(value string) (local, domain string, ok bool) {
	return splitNIP05(value)
}

// ScheduleSQL is documented so callers know the pending-row contract.
func SchedulePending(ctx context.Context, pool *pgxpool.Pool, pubkey, identifier string) error {
	if pool == nil {
		return fmt.Errorf("pool is nil")
	}
	local, domain, ok := splitNIP05(identifier)
	if !ok {
		_, err := pool.Exec(ctx, `DELETE FROM nip05_verifications WHERE pubkey = $1`, pubkey)
		return err
	}
	_ = local
	_, err := pool.Exec(ctx, `
		INSERT INTO nip05_verifications (pubkey, nip05, domain, status, next_check_at, failure_count)
		VALUES ($1, $2, $3, 'pending', now(), 0)
		ON CONFLICT (pubkey) DO UPDATE
		SET nip05 = EXCLUDED.nip05,
		    domain = EXCLUDED.domain,
		    status = 'pending',
		    next_check_at = now(),
		    failure_count = 0,
		    last_error_code = '',
		    updated_at = now()
		WHERE nip05_verifications.nip05 IS DISTINCT FROM EXCLUDED.nip05
	`, pubkey, strings.ToLower(strings.TrimSpace(identifier)), domain)
	return err
}
