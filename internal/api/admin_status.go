package api

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"
)

const (
	adminStatusTrustModeOpen = "open"

	adminIngestFreshnessThreshold      = 10 * time.Minute
	adminProjectionFreshnessThreshold  = 20 * time.Minute
	adminDiscoveryFreshnessThreshold   = 15 * time.Minute
	adminSearchFreshnessThreshold      = 15 * time.Minute
	adminMinimumTrustFreshnessDeadline = 10 * time.Minute
)

type adminFreshnessSignal struct {
	Name             string     `json:"name"`
	LastUpdatedAt    *time.Time `json:"last_updated_at,omitempty"`
	LagSeconds       *int64     `json:"lag_seconds,omitempty"`
	ThresholdSeconds int64      `json:"threshold_seconds"`
	Status           string     `json:"status"`
	Stale            bool       `json:"stale"`
	RowCount         *int64     `json:"row_count,omitempty"`
	Details          string     `json:"details,omitempty"`
}

type adminProjectionStatusResponse struct {
	NowUTC           time.Time              `json:"now_utc"`
	TrustAwarePolicy bool                   `json:"trust_aware_policy"`
	Ingest           adminFreshnessSignal   `json:"ingest"`
	Subsystems       []adminFreshnessSignal `json:"subsystems"`
	StaleSubsystems  []string               `json:"stale_subsystems"`
	Healthy          bool                   `json:"healthy"`
}

type adminDiscoveryStatusResponse struct {
	NowUTC           time.Time              `json:"now_utc"`
	TrustAwarePolicy bool                   `json:"trust_aware_policy"`
	Signals          []adminFreshnessSignal `json:"signals"`
	StaleSubsystems  []string               `json:"stale_subsystems"`
	Ready            bool                   `json:"ready"`
}

type adminSearchStatusResponse struct {
	NowUTC           time.Time              `json:"now_utc"`
	TrustAwarePolicy bool                   `json:"trust_aware_policy"`
	Signals          []adminFreshnessSignal `json:"signals"`
	StaleSubsystems  []string               `json:"stale_subsystems"`
	Ready            bool                   `json:"ready"`
}

func (s *adminService) GetProjectionStatus(ctx context.Context) (adminProjectionStatusResponse, error) {
	var (
		ingestCount               int64
		ingestUpdatedAt           *time.Time
		profilesCount             int64
		profilesUpdatedAt         *time.Time
		notesDiscoveryCount       int64
		notesDiscoveryUpdatedAt   *time.Time
		profileDiscoveryCount     int64
		profileDiscoveryUpdatedAt *time.Time
		replyCountRows            int64
		replyCountsUpdatedAt      *time.Time
		reactionCountRows         int64
		reactionCountsUpdatedAt   *time.Time
		repostCountRows           int64
		repostCountsUpdatedAt     *time.Time
	)
	if err := s.pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM ingest_checkpoints),
			(SELECT MAX(updated_at) FROM ingest_checkpoints),
			(SELECT COUNT(*) FROM profiles_latest),
			(SELECT MAX(updated_at) FROM profiles_latest),
			(SELECT COUNT(*) FROM note_discovery_stats),
			(SELECT MAX(projected_at) FROM note_discovery_stats),
			(SELECT COUNT(*) FROM profile_discovery_stats),
			(SELECT MAX(projected_at) FROM profile_discovery_stats),
			(SELECT COUNT(*) FROM reply_counts),
			(SELECT MAX(updated_at) FROM reply_counts),
			(SELECT COUNT(*) FROM reaction_counts),
			(SELECT MAX(updated_at) FROM reaction_counts),
			(SELECT COUNT(*) FROM repost_counts),
			(SELECT MAX(updated_at) FROM repost_counts)
	`).Scan(
		&ingestCount,
		&ingestUpdatedAt,
		&profilesCount,
		&profilesUpdatedAt,
		&notesDiscoveryCount,
		&notesDiscoveryUpdatedAt,
		&profileDiscoveryCount,
		&profileDiscoveryUpdatedAt,
		&replyCountRows,
		&replyCountsUpdatedAt,
		&reactionCountRows,
		&reactionCountsUpdatedAt,
		&repostCountRows,
		&repostCountsUpdatedAt,
	); err != nil {
		return adminProjectionStatusResponse{}, fmt.Errorf("read projection status snapshot: %w", err)
	}

	now := time.Now().UTC()
	ingest := buildFreshnessSignal("ingest", ingestUpdatedAt, adminIngestFreshnessThreshold, now, &ingestCount)
	subsystems := []adminFreshnessSignal{
		buildFreshnessSignal("profiles_latest", profilesUpdatedAt, adminProjectionFreshnessThreshold, now, &profilesCount),
		buildFreshnessSignal("note_discovery_stats", notesDiscoveryUpdatedAt, adminProjectionFreshnessThreshold, now, &notesDiscoveryCount),
		buildFreshnessSignal("profile_discovery_stats", profileDiscoveryUpdatedAt, adminProjectionFreshnessThreshold, now, &profileDiscoveryCount),
		buildFreshnessSignal(
			"event_engagement_counts",
			latestNonNilTimestamp(replyCountsUpdatedAt, reactionCountsUpdatedAt, repostCountsUpdatedAt),
			adminProjectionFreshnessThreshold,
			now,
			int64Ptr(replyCountRows+reactionCountRows+repostCountRows),
		),
	}
	if s.anyTrustPolicyEnabled() {
		snapshotUpdatedAt, snapshotCount, err := s.loadTrustSnapshotStatus(ctx)
		if err != nil {
			return adminProjectionStatusResponse{}, err
		}
		subsystems = append(subsystems, buildFreshnessSignal(
			"trust_graph_snapshot",
			snapshotUpdatedAt,
			s.trustFreshnessThreshold(),
			now,
			&snapshotCount,
		))
	}

	stale := staleSignalNames(append([]adminFreshnessSignal{ingest}, subsystems...))
	return adminProjectionStatusResponse{
		NowUTC:           now,
		TrustAwarePolicy: s.anyTrustPolicyEnabled(),
		Ingest:           ingest,
		Subsystems:       subsystems,
		StaleSubsystems:  stale,
		Healthy:          len(stale) == 0,
	}, nil
}

func (s *adminService) GetDiscoveryStatus(ctx context.Context) (adminDiscoveryStatusResponse, error) {
	var (
		ingestCount                 int64
		ingestUpdatedAt             *time.Time
		noteDiscoveryCount          int64
		noteDiscoveryProjectedAt    *time.Time
		profileDiscoveryCount       int64
		profileDiscoveryProjectedAt *time.Time
	)
	if err := s.pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM ingest_checkpoints),
			(SELECT MAX(updated_at) FROM ingest_checkpoints),
			(SELECT COUNT(*) FROM note_discovery_stats),
			(SELECT MAX(projected_at) FROM note_discovery_stats),
			(SELECT COUNT(*) FROM profile_discovery_stats),
			(SELECT MAX(projected_at) FROM profile_discovery_stats)
	`).Scan(
		&ingestCount,
		&ingestUpdatedAt,
		&noteDiscoveryCount,
		&noteDiscoveryProjectedAt,
		&profileDiscoveryCount,
		&profileDiscoveryProjectedAt,
	); err != nil {
		return adminDiscoveryStatusResponse{}, fmt.Errorf("read discovery status snapshot: %w", err)
	}

	now := time.Now().UTC()
	signals := []adminFreshnessSignal{
		buildFreshnessSignal("ingest", ingestUpdatedAt, adminIngestFreshnessThreshold, now, &ingestCount),
		buildFreshnessSignal("note_discovery_stats", noteDiscoveryProjectedAt, adminDiscoveryFreshnessThreshold, now, &noteDiscoveryCount),
		buildFreshnessSignal("profile_discovery_stats", profileDiscoveryProjectedAt, adminDiscoveryFreshnessThreshold, now, &profileDiscoveryCount),
	}

	if s.discoveryTrustPolicyEnabled() {
		snapshotUpdatedAt, snapshotCount, err := s.loadTrustSnapshotStatus(ctx)
		if err != nil {
			return adminDiscoveryStatusResponse{}, err
		}
		noteStateUpdatedAt, noteStateCount, profileStateUpdatedAt, profileStateCount, err := s.loadTrustedDiscoveryProjectionState(ctx)
		if err != nil {
			return adminDiscoveryStatusResponse{}, err
		}
		trustThreshold := s.trustFreshnessThreshold()
		signals = append(signals,
			buildFreshnessSignal("trust_graph_snapshot", snapshotUpdatedAt, trustThreshold, now, &snapshotCount),
			buildFreshnessSignal("trusted_note_discovery_candidates", noteStateUpdatedAt, trustThreshold, now, &noteStateCount),
			buildFreshnessSignal("trusted_profile_discovery_candidates", profileStateUpdatedAt, trustThreshold, now, &profileStateCount),
		)
	}

	stale := staleSignalNames(signals)
	return adminDiscoveryStatusResponse{
		NowUTC:           now,
		TrustAwarePolicy: s.discoveryTrustPolicyEnabled(),
		Signals:          signals,
		StaleSubsystems:  stale,
		Ready:            len(stale) == 0,
	}, nil
}

func (s *adminService) GetSearchStatus(ctx context.Context) (adminSearchStatusResponse, error) {
	var (
		ingestCount             int64
		ingestUpdatedAt         *time.Time
		noteSearchRows          int64
		noteSearchUpdatedAt     *time.Time
		profilesCount           int64
		profilesUpdatedAt       *time.Time
		notesDiscoveryCount     int64
		notesDiscoveryUpdatedAt *time.Time
		searchDocumentRows      int64
		searchDocumentsUpdated  *time.Time
		missingProfileRows      int64
	)
	if err := s.pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM ingest_checkpoints),
			(SELECT MAX(updated_at) FROM ingest_checkpoints),
			(SELECT COUNT(*) FROM events WHERE kind = 1),
			(SELECT MAX(inserted_at) FROM events WHERE kind = 1),
			(SELECT COUNT(*) FROM profiles_latest),
			(SELECT MAX(updated_at) FROM profiles_latest),
			(SELECT COUNT(*) FROM note_discovery_stats),
			(SELECT MAX(projected_at) FROM note_discovery_stats),
			(SELECT COUNT(*) FROM search_documents),
			(SELECT MAX(updated_at) FROM search_documents),
			(
				SELECT COUNT(*)
				FROM (
					SELECT DISTINCT events.pubkey
					FROM events
					WHERE events.kind = 0
					EXCEPT
					SELECT profiles_latest.pubkey
					FROM profiles_latest
				) missing_profiles
			)
	`).Scan(
		&ingestCount,
		&ingestUpdatedAt,
		&noteSearchRows,
		&noteSearchUpdatedAt,
		&profilesCount,
		&profilesUpdatedAt,
		&notesDiscoveryCount,
		&notesDiscoveryUpdatedAt,
		&searchDocumentRows,
		&searchDocumentsUpdated,
		&missingProfileRows,
	); err != nil {
		return adminSearchStatusResponse{}, fmt.Errorf("read search status snapshot: %w", err)
	}

	now := time.Now().UTC()
	signals := []adminFreshnessSignal{
		buildFreshnessSignal("ingest", ingestUpdatedAt, adminIngestFreshnessThreshold, now, &ingestCount),
		buildFreshnessSignal("events_note_index", noteSearchUpdatedAt, adminSearchFreshnessThreshold, now, &noteSearchRows),
		buildFreshnessSignal("profiles_latest", profilesUpdatedAt, adminSearchFreshnessThreshold, now, &profilesCount),
		buildFreshnessSignal("note_discovery_stats", notesDiscoveryUpdatedAt, adminSearchFreshnessThreshold, now, &notesDiscoveryCount),
		buildFreshnessSignal("search_documents", searchDocumentsUpdated, adminSearchFreshnessThreshold, now, &searchDocumentRows),
		buildCoverageSignal("profiles_latest_projection_gap", missingProfileRows),
	}
	if s.searchTrustPolicyEnabled() {
		snapshotUpdatedAt, snapshotCount, err := s.loadTrustSnapshotStatus(ctx)
		if err != nil {
			return adminSearchStatusResponse{}, err
		}
		signals = append(signals, buildFreshnessSignal(
			"trust_graph_snapshot",
			snapshotUpdatedAt,
			s.trustFreshnessThreshold(),
			now,
			&snapshotCount,
		))
	}

	stale := staleSignalNames(signals)
	return adminSearchStatusResponse{
		NowUTC:           now,
		TrustAwarePolicy: s.searchTrustPolicyEnabled(),
		Signals:          signals,
		StaleSubsystems:  stale,
		Ready:            len(stale) == 0,
	}, nil
}

func buildCoverageSignal(name string, uncoveredRows int64) adminFreshnessSignal {
	signal := adminFreshnessSignal{
		Name:             strings.TrimSpace(name),
		ThresholdSeconds: 0,
		RowCount:         int64Ptr(uncoveredRows),
		Stale:            uncoveredRows > 0,
	}
	if uncoveredRows > 0 {
		signal.Status = "stale"
		signal.Details = "kind=0 metadata rows exist without matching profiles_latest projection rows"
		return signal
	}
	signal.Status = "fresh"
	return signal
}

func buildFreshnessSignal(
	name string,
	lastUpdatedAt *time.Time,
	threshold time.Duration,
	now time.Time,
	rowCount *int64,
) adminFreshnessSignal {
	signal := adminFreshnessSignal{
		Name:             strings.TrimSpace(name),
		ThresholdSeconds: int64(threshold.Seconds()),
		Status:           "missing",
		Stale:            true,
	}
	if rowCount != nil {
		signal.RowCount = int64Ptr(*rowCount)
	}
	if threshold <= 0 {
		signal.ThresholdSeconds = int64(adminProjectionFreshnessThreshold.Seconds())
	}
	if lastUpdatedAt == nil || lastUpdatedAt.IsZero() {
		return signal
	}
	updatedAt := lastUpdatedAt.UTC()
	signal.LastUpdatedAt = &updatedAt
	lag := now.Sub(updatedAt)
	if lag < 0 {
		lag = 0
	}
	lagSeconds := int64(lag.Seconds())
	signal.LagSeconds = &lagSeconds
	signal.Stale = lag > threshold
	if signal.Stale {
		signal.Status = "stale"
		return signal
	}
	signal.Status = "fresh"
	return signal
}

func int64Ptr(v int64) *int64 {
	c := v
	return &c
}

func latestNonNilTimestamp(values ...*time.Time) *time.Time {
	var out *time.Time
	for _, value := range values {
		if value == nil || value.IsZero() {
			continue
		}
		candidate := value.UTC()
		if out == nil || candidate.After(*out) {
			out = &candidate
		}
	}
	return out
}

func staleSignalNames(signals []adminFreshnessSignal) []string {
	out := make([]string, 0)
	for _, signal := range signals {
		if signal.Stale {
			out = append(out, signal.Name)
		}
	}
	slices.Sort(out)
	return out
}

func (s *adminService) discoveryTrustPolicyEnabled() bool {
	return strings.TrimSpace(s.discoveryTrustMode) != "" && strings.TrimSpace(s.discoveryTrustMode) != adminStatusTrustModeOpen
}

func (s *adminService) searchTrustPolicyEnabled() bool {
	return strings.TrimSpace(s.searchTrustMode) != "" && strings.TrimSpace(s.searchTrustMode) != adminStatusTrustModeOpen
}

func (s *adminService) anyTrustPolicyEnabled() bool {
	return s.discoveryTrustPolicyEnabled() || s.searchTrustPolicyEnabled()
}

func (s *adminService) trustFreshnessThreshold() time.Duration {
	interval := s.trustRefreshInterval
	if interval <= 0 {
		interval = adminMinimumTrustFreshnessDeadline
	}
	window := interval * 2
	if window < adminMinimumTrustFreshnessDeadline {
		return adminMinimumTrustFreshnessDeadline
	}
	return window
}

func (s *adminService) loadTrustSnapshotStatus(ctx context.Context) (*time.Time, int64, error) {
	var (
		count       int64
		refreshedAt *time.Time
	)
	if err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*), MAX(refreshed_at)
		FROM trust_graph_snapshot
	`).Scan(&count, &refreshedAt); err != nil {
		return nil, 0, fmt.Errorf("read trust snapshot status: %w", err)
	}
	if refreshedAt == nil {
		return nil, count, nil
	}
	ts := refreshedAt.UTC()
	return &ts, count, nil
}

func (s *adminService) loadTrustedDiscoveryProjectionState(ctx context.Context) (
	*time.Time,
	int64,
	*time.Time,
	int64,
	error,
) {
	var (
		noteStateUpdatedAt    *time.Time
		noteStateCount        int64
		profileStateUpdatedAt *time.Time
		profileStateCount     int64
	)
	if err := s.pool.QueryRow(ctx, `
		SELECT
			(SELECT MAX(refreshed_at) FROM trusted_discovery_projection_state WHERE projection_name = 'trusted_note_discovery_candidates'),
			(SELECT COUNT(*) FROM trusted_note_discovery_candidates),
			(SELECT MAX(refreshed_at) FROM trusted_discovery_projection_state WHERE projection_name = 'trusted_profile_discovery_candidates'),
			(SELECT COUNT(*) FROM trusted_profile_discovery_candidates)
	`).Scan(
		&noteStateUpdatedAt,
		&noteStateCount,
		&profileStateUpdatedAt,
		&profileStateCount,
	); err != nil {
		return nil, 0, nil, 0, fmt.Errorf("read trusted discovery projection state: %w", err)
	}
	if noteStateUpdatedAt != nil {
		ts := noteStateUpdatedAt.UTC()
		noteStateUpdatedAt = &ts
	}
	if profileStateUpdatedAt != nil {
		ts := profileStateUpdatedAt.UTC()
		profileStateUpdatedAt = &ts
	}
	return noteStateUpdatedAt, noteStateCount, profileStateUpdatedAt, profileStateCount, nil
}
