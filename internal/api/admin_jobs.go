package api

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type adminJobsResponse struct {
	Counts         map[string]int64 `json:"counts"`
	DueNow         int64            `json:"due_now"`
	OldestPendingS int64            `json:"oldest_pending_s"`
	Recent         []adminJobItem   `json:"recent"`
}

type adminJobItem struct {
	ID          int64           `json:"id"`
	JobType     string          `json:"job_type"`
	Status      string          `json:"status"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	RunAfter    time.Time       `json:"run_after"`
	LockedBy    *string         `json:"locked_by,omitempty"`
	LastError   *string         `json:"last_error,omitempty"`
	Payload     json.RawMessage `json:"payload"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

func (s *adminService) GetJobs(ctx context.Context, limit int) (adminJobsResponse, error) {
	resp := adminJobsResponse{
		Counts: map[string]int64{
			"pending":   0,
			"running":   0,
			"succeeded": 0,
			"dead":      0,
		},
		Recent: make([]adminJobItem, 0),
	}
	countRows, err := s.pool.Query(ctx, `
		SELECT status, COUNT(*)
		FROM jobs
		GROUP BY status
	`)
	if err != nil {
		return resp, fmt.Errorf("count jobs by status: %w", err)
	}
	for countRows.Next() {
		var status string
		var count int64
		if err := countRows.Scan(&status, &count); err != nil {
			countRows.Close()
			return resp, fmt.Errorf("scan jobs count row: %w", err)
		}
		resp.Counts[strings.TrimSpace(status)] = count
	}
	if err := countRows.Err(); err != nil {
		countRows.Close()
		return resp, fmt.Errorf("read jobs count rows: %w", err)
	}
	countRows.Close()

	if err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM jobs
		WHERE status = 'pending' AND run_after <= now()
	`).Scan(&resp.DueNow); err != nil {
		return resp, fmt.Errorf("count due jobs: %w", err)
	}

	var oldestPendingSeconds *float64
	if err := s.pool.QueryRow(ctx, `
		SELECT EXTRACT(EPOCH FROM (now() - MIN(run_after)))
		FROM jobs
		WHERE status = 'pending'
	`).Scan(&oldestPendingSeconds); err != nil {
		return resp, fmt.Errorf("get oldest pending age: %w", err)
	}
	if oldestPendingSeconds != nil && *oldestPendingSeconds > 0 {
		resp.OldestPendingS = int64(*oldestPendingSeconds)
	}

	recentRows, err := s.pool.Query(ctx, `
		SELECT id, job_type, status, attempts, max_attempts, run_after, locked_by, last_error, payload, created_at, updated_at
		FROM jobs
		ORDER BY created_at DESC, id DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return resp, fmt.Errorf("list recent jobs: %w", err)
	}
	defer recentRows.Close()
	for recentRows.Next() {
		var item adminJobItem
		var payloadBytes []byte
		if err := recentRows.Scan(
			&item.ID,
			&item.JobType,
			&item.Status,
			&item.Attempts,
			&item.MaxAttempts,
			&item.RunAfter,
			&item.LockedBy,
			&item.LastError,
			&payloadBytes,
			&item.CreatedAt,
			&item.UpdatedAt,
		); err != nil {
			return resp, fmt.Errorf("scan recent job: %w", err)
		}
		item.Payload = append(json.RawMessage(nil), payloadBytes...)
		item.RunAfter = item.RunAfter.UTC()
		item.CreatedAt = item.CreatedAt.UTC()
		item.UpdatedAt = item.UpdatedAt.UTC()
		resp.Recent = append(resp.Recent, item)
	}
	if err := recentRows.Err(); err != nil {
		return resp, fmt.Errorf("read recent jobs: %w", err)
	}
	return resp, nil
}

type adminInvalidEventsResponse struct {
	Total       int64               `json:"total"`
	Last24Hours int64               `json:"last_24h"`
	ByErrorCode map[string]int64    `json:"by_error_code"`
	Items       []adminInvalidEvent `json:"items"`
}

type adminInvalidEvent struct {
	ID           int64            `json:"id"`
	SourceRelay  *string          `json:"source_relay,omitempty"`
	ErrorCode    string           `json:"error_code"`
	ErrorMessage string           `json:"error_message"`
	RawPayload   *json.RawMessage `json:"raw_payload,omitempty"`
	SeenAt       time.Time        `json:"seen_at"`
}

func (s *adminService) GetInvalidEvents(ctx context.Context, limit int) (adminInvalidEventsResponse, error) {
	resp := adminInvalidEventsResponse{
		ByErrorCode: make(map[string]int64),
		Items:       make([]adminInvalidEvent, 0),
	}
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM invalid_events`).Scan(&resp.Total); err != nil {
		return resp, fmt.Errorf("count invalid events: %w", err)
	}
	if err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM invalid_events
		WHERE seen_at >= now() - interval '24 hours'
	`).Scan(&resp.Last24Hours); err != nil {
		return resp, fmt.Errorf("count invalid events last 24h: %w", err)
	}

	byCodeRows, err := s.pool.Query(ctx, `
		SELECT error_code, COUNT(*)
		FROM invalid_events
		GROUP BY error_code
		ORDER BY COUNT(*) DESC, error_code ASC
	`)
	if err != nil {
		return resp, fmt.Errorf("group invalid events by code: %w", err)
	}
	for byCodeRows.Next() {
		var code string
		var count int64
		if err := byCodeRows.Scan(&code, &count); err != nil {
			byCodeRows.Close()
			return resp, fmt.Errorf("scan invalid events by code: %w", err)
		}
		resp.ByErrorCode[code] = count
	}
	if err := byCodeRows.Err(); err != nil {
		byCodeRows.Close()
		return resp, fmt.Errorf("read invalid events by code: %w", err)
	}
	byCodeRows.Close()

	itemsRows, err := s.pool.Query(ctx, `
		SELECT id, source_relay, error_code, error_message, raw_payload, seen_at
		FROM invalid_events
		ORDER BY seen_at DESC, id DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return resp, fmt.Errorf("list invalid events: %w", err)
	}
	defer itemsRows.Close()
	for itemsRows.Next() {
		var item adminInvalidEvent
		var payloadBytes []byte
		if err := itemsRows.Scan(
			&item.ID,
			&item.SourceRelay,
			&item.ErrorCode,
			&item.ErrorMessage,
			&payloadBytes,
			&item.SeenAt,
		); err != nil {
			return resp, fmt.Errorf("scan invalid event row: %w", err)
		}
		item.SeenAt = item.SeenAt.UTC()
		if len(payloadBytes) > 0 {
			payload := json.RawMessage(append([]byte(nil), payloadBytes...))
			item.RawPayload = &payload
		}
		resp.Items = append(resp.Items, item)
	}
	if err := itemsRows.Err(); err != nil {
		return resp, fmt.Errorf("read invalid event rows: %w", err)
	}
	return resp, nil
}
