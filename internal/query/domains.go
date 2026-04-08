package query

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/domainnorm"
)

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
		return r.GetEventLinkedDomains(ctx, eventID, limit)
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
		return r.GetTopDomainsByAuthor(ctx, pubkey, window, limit, offset)
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
		return r.GetTopDomains(ctx, window, limit, offset)
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
	if r := s.capabilities.curated.trendingDomains; r != nil {
		return r.GetTrendingDomains(ctx, window, limit, offset)
	}
	return nil, unsupportedCapabilityError("trending domains")
}

func (s Service) GetDomainSummary(ctx context.Context, domain string) (DomainSummary, error) {
	normalized, err := normalizeDomainToken(domain)
	if err != nil {
		return DomainSummary{}, err
	}
	if r := s.capabilities.curated.domainSummary; r != nil {
		return r.GetDomainSummary(ctx, normalized, 5, 5)
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
		return r.GetDomainNotes(ctx, normalized, sort, window, limit, offset)
	}
	return nil, unsupportedCapabilityError("domain notes")
}

func normalizeDomainToken(value string) (string, error) {
	normalized := domainnorm.NormalizeLookupValue(value)
	if normalized == "" {
		return "", fmt.Errorf("domain is invalid: %w", ErrInvalidDomain)
	}
	return normalized, nil
}
