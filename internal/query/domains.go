package query

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/domainnorm"
)

// homeTrendingSnapshotLimit is the number of rows materialized in
// top_hashtags_* and top_domains_*. Callers page inside that list.
const homeTrendingSnapshotLimit = 50

// isFixedDiscoveryWindow reports the two windows the overview and the
// topics/links tabs share. Those are the windows the relay snapshots cover.
func isFixedDiscoveryWindow(window time.Duration) bool {
	return window == 24*time.Hour || window == 7*24*time.Hour
}

func (s Service) GetEventLinkedDomains(ctx context.Context, eventID string, limit int) ([]EventDomainLink, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, fmt.Errorf("event id is required")
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	if r := s.capabilities.curated.eventLinkedDomains; r != nil {
		rows, err := r.GetEventLinkedDomains(ctx, eventID, limit)
		if err != nil {
			return nil, err
		}
		return mapSlice(rows, eventDomainLinkFromStore), nil
	}
	return nil, unsupportedCapabilityError("event linked domains")
}

func (s Service) GetTopDomainsByAuthor(
	ctx context.Context,
	pubkey string,
	window time.Duration,
	limit int,
	offset int,
) ([]DomainStat, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	if window <= 0 {
		return nil, fmt.Errorf("window must be positive")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	if r := s.capabilities.curated.topDomainsByAuthor; r != nil {
		rows, err := r.GetTopDomainsByAuthor(ctx, pubkey, window, limit, offset)
		if err != nil {
			return nil, err
		}
		return mapSlice(rows, domainStatFromStore), nil
	}
	return nil, unsupportedCapabilityError("top domains by author")
}

func (s Service) GetTopDomains(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
) ([]DomainStat, error) {
	if window <= 0 {
		return nil, fmt.Errorf("window must be positive")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	if r := s.capabilities.curated.topDomains; r != nil {
		rows, err := r.GetTopDomains(ctx, window, limit, offset)
		if err != nil {
			return nil, err
		}
		return mapSlice(rows, domainStatFromStore), nil
	}
	return nil, unsupportedCapabilityError("top domains")
}

func (s Service) GetTrendingDomains(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
) ([]DomainSummary, error) {
	if window <= 0 {
		return nil, fmt.Errorf("window must be positive")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	// 24h and 7d are the windows the overview and the links tab both
	// request. Serve the same trust-filtered snapshot the homepage uses so
	// the two surfaces rank the same domains. The live aggregate stays
	// available for readers that have no snapshot and for other windows.
	if isFixedDiscoveryWindow(window) {
		if r := s.capabilities.curated.homeTrendingDomains; r != nil {
			rows, err := r.GetHomeTrendingDomains(ctx, window, homeTrendingSnapshotLimit)
			if err != nil {
				return nil, err
			}
			if offset >= len(rows) {
				return []DomainSummary{}, nil
			}
			rows = rows[offset:]
			if len(rows) > limit {
				rows = rows[:limit]
			}
			return mapSlice(rows, domainSummaryFromStore), nil
		}
	}
	if r := s.capabilities.curated.trendingDomains; r != nil {
		rows, err := r.GetTrendingDomains(ctx, window, limit, offset)
		if err != nil {
			return nil, err
		}
		return mapSlice(rows, domainSummaryFromStore), nil
	}
	return nil, unsupportedCapabilityError("trending domains")
}

// GetHomeTrendingDomains serves a fixed (24h, 7d) trending-domains window
// from the precomputed snapshot (see internal/derivation/projection_relay_window_snapshots.go).
// GetTrendingDomains uses the same snapshot for those windows.
func (s Service) GetHomeTrendingDomains(
	ctx context.Context,
	window time.Duration,
	limit int,
) ([]DomainSummary, error) {
	if window <= 0 {
		return nil, fmt.Errorf("window must be positive")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 50 {
		limit = 50
	}
	if r := s.capabilities.curated.homeTrendingDomains; r != nil {
		rows, err := r.GetHomeTrendingDomains(ctx, window, limit)
		if err != nil {
			return nil, err
		}
		return mapSlice(rows, domainSummaryFromStore), nil
	}
	return nil, unsupportedCapabilityError("home trending domains")
}

func (s Service) GetDomainSummary(ctx context.Context, domain string) (DomainSummary, error) {
	normalized, err := normalizeDomainToken(domain)
	if err != nil {
		return DomainSummary{}, err
	}
	if r := s.capabilities.curated.domainSummary; r != nil {
		row, err := r.GetDomainSummary(ctx, normalized, 5, 5)
		if err != nil {
			return DomainSummary{}, err
		}
		return domainSummaryFromStore(row), nil
	}
	return DomainSummary{}, unsupportedCapabilityError("domain summary")
}

func (s Service) GetDomainNotes(
	ctx context.Context,
	domain string,
	sort string,
	window string,
	limit int,
	offset int,
) ([]TrendingNote, error) {
	normalized, err := normalizeDomainToken(domain)
	if err != nil {
		return nil, err
	}
	if r := s.capabilities.curated.domainNotes; r != nil {
		rows, err := r.GetDomainNotes(ctx, normalized, sort, window, limit, offset)
		if err != nil {
			return nil, err
		}
		return mapSlice(rows, trendingNoteFromStore), nil
	}
	return nil, unsupportedCapabilityError("domain notes")
}

func normalizeDomainToken(value string) (string, error) {
	normalized := domainnorm.CanonicalizeDiscoveryDomain(value)
	if normalized == "" {
		return "", fmt.Errorf("domain is invalid: %w", ErrInvalidDomain)
	}
	return normalized, nil
}
