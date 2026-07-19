package query

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

// TrustMode is the validated, typed trust-gating mode used across discovery,
// search, and fallback surfaces. Parsing happens once at the config boundary
// (ParseTrustMode); internal comparisons use the derived string constants.
type TrustMode string

const (
	TrustModeOpen          TrustMode = "open"
	TrustModePreferTrusted TrustMode = "prefer_trusted"
	TrustModeTrustedOnly   TrustMode = "trusted_only"
)

// Valid reports whether m is a recognized trust mode.
func (m TrustMode) Valid() bool {
	switch m {
	case TrustModeOpen, TrustModePreferTrusted, TrustModeTrustedOnly:
		return true
	default:
		return false
	}
}

func (m TrustMode) String() string { return string(m) }

// ParseTrustMode normalizes and validates a raw trust-mode string, applying
// def when the input is empty. It is the single validated parse point.
func ParseTrustMode(raw string, def TrustMode) (TrustMode, error) {
	normalized := TrustMode(strings.ToLower(strings.TrimSpace(raw)))
	if normalized == "" {
		normalized = def
	}
	if !normalized.Valid() {
		return "", fmt.Errorf("invalid trust mode %q", string(normalized))
	}
	return normalized, nil
}

// Internal string constants derived from the typed modes so the many existing
// string comparisons remain valid without a package-wide type migration.
const (
	trustModeOpen          = string(TrustModeOpen)
	trustModePreferTrusted = string(TrustModePreferTrusted)
	trustModeTrustedOnly   = string(TrustModeTrustedOnly)
)

var hashtagPattern = regexp.MustCompile(`(?i)(?:^|[^a-z0-9_])#([a-z0-9_]{1,64})`)

type trustedNoteCandidate struct {
	note    TrendingNote
	trusted bool
}

type trustedProfileCandidate struct {
	profile TrendingProfile
	trusted bool
}

// plainTrendingFetch loads ranked candidate rows for one trending surface.
type plainTrendingFetch func(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingNote, error)

// trustQualifiedTrendingFetch loads trust-qualified candidate rows from a
// precomputed projection. It returns ready=false when the projection is stale
// so the caller can fall back to on-the-fly qualification.
type trustQualifiedTrendingFetch func(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
	mode string,
	policy TrustQualificationPolicy,
	maxStaleness time.Duration,
) ([]trustedNoteCandidate, bool, error)

func (s Service) getTrendingNotesTrustAware(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingNote, error) {
	notesCap := s.capabilities.curated.trendingNotes
	if notesCap == nil {
		return nil, unsupportedCapabilityError("trending notes")
	}
	var tqFetch trustQualifiedTrendingFetch
	if cap := s.capabilities.curated.trustQualifiedNotes; cap != nil {
		tqFetch = cap.GetTrustQualifiedTrendingNotes
	}
	return s.getTrendingTrustAware(ctx, window, limit, offset, notesCap.GetTrendingNotes, tqFetch)
}

func (s Service) getTrendingLongFormTrustAware(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingNote, error) {
	longFormCap := s.capabilities.curated.trendingLongForm
	if longFormCap == nil {
		return nil, unsupportedCapabilityError("trending long-form")
	}
	// Long-form volume is low, so we always qualify on the fly rather than
	// maintaining a dedicated trust projection.
	return s.getTrendingTrustAware(ctx, window, limit, offset, longFormCap.GetTrendingLongForm, nil)
}

func (s Service) getTrendingTrustAware(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
	plain plainTrendingFetch,
	tq trustQualifiedTrendingFetch,
) ([]TrendingNote, error) {
	limit, offset = normalizeDiscoveryPage(limit, offset, 20, 100)
	target := limit + offset
	candidates, err := s.collectTrustedTrending(ctx, window, target, target*4, plain, tq)
	if err != nil {
		return nil, err
	}
	return paginateTrendingNotes(trustedNoteRowsByMode(candidates, s.discoveryTrustMode), limit, offset), nil
}

func (s Service) getTrendingProfilesTrustAware(
	ctx context.Context,
	fetch func(context.Context, time.Duration, int, int) ([]TrendingProfile, error),
	rising bool,
	window time.Duration,
	limit int,
	offset int,
) ([]TrendingProfile, error) {
	if fetch == nil {
		return nil, unsupportedCapabilityError("profile discovery")
	}
	limit, offset = normalizeDiscoveryPage(limit, offset, 20, 100)
	target := limit + offset
	candidates, err := s.collectTrustedTrendingProfiles(ctx, fetch, rising, window, target, target*4)
	if err != nil {
		return nil, err
	}
	return paginateTrendingProfiles(trustedProfileRowsByMode(candidates, s.discoveryTrustMode), limit, offset), nil
}

func (s Service) getTrendingHashtagsTrustAware(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingHashtag, error) {
	limit, offset = normalizeDiscoveryPage(limit, offset, 50, 100)
	target := limit + offset
	// Hashtags are derived from trust-qualified note candidates.
	notes, err := s.collectTrustedTrendingNotes(ctx, window, target*20, target*40)
	if err != nil {
		return nil, err
	}
	type hashtagAgg struct {
		eventCount int64
		authors    map[string]struct{}
	}
	agg := make(map[string]*hashtagAgg, len(notes))
	for _, candidate := range notes {
		if s.discoveryTrustMode == trustModeTrustedOnly && !candidate.trusted {
			continue
		}
		for _, hashtag := range hashtagsFromContent(candidate.note.Content) {
			row := agg[hashtag]
			if row == nil {
				row = &hashtagAgg{authors: map[string]struct{}{}}
				agg[hashtag] = row
			}
			row.eventCount++
			row.authors[candidate.note.AuthorPubkey] = struct{}{}
		}
	}
	out := make([]TrendingHashtag, 0, len(agg))
	for hashtag, row := range agg {
		out = append(out, TrendingHashtag{
			Hashtag:       hashtag,
			EventCount:    row.eventCount,
			UniqueAuthors: int64(len(row.authors)),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].EventCount != out[j].EventCount {
			return out[i].EventCount > out[j].EventCount
		}
		if out[i].UniqueAuthors != out[j].UniqueAuthors {
			return out[i].UniqueAuthors > out[j].UniqueAuthors
		}
		return out[i].Hashtag < out[j].Hashtag
	})
	return paginateTrendingHashtags(out, limit, offset), nil
}

func (s Service) collectTrustedTrendingNotes(
	ctx context.Context,
	window time.Duration,
	targetRows int,
	scanBudget int,
) ([]trustedNoteCandidate, error) {
	capability := s.capabilities.curated.trendingNotes
	if capability == nil {
		return nil, unsupportedCapabilityError("trending notes")
	}
	var tqFetch trustQualifiedTrendingFetch
	if cap := s.capabilities.curated.trustQualifiedNotes; cap != nil {
		tqFetch = cap.GetTrustQualifiedTrendingNotes
	}
	return s.collectTrustedTrending(ctx, window, targetRows, scanBudget, capability.GetTrendingNotes, tqFetch)
}

func (s Service) collectTrustedTrending(
	ctx context.Context,
	window time.Duration,
	targetRows int,
	scanBudget int,
	plain plainTrendingFetch,
	tq trustQualifiedTrendingFetch,
) ([]trustedNoteCandidate, error) {
	if plain == nil {
		return nil, unsupportedCapabilityError("trending notes")
	}
	if s.capabilities.trust.qualification == nil {
		return nil, unsupportedCapabilityError("trust qualification")
	}
	if targetRows <= 0 {
		targetRows = 20
	}
	if scanBudget <= 0 {
		scanBudget = targetRows * 4
	}
	if scanBudget > s.discoveryTrustScanSize {
		scanBudget = s.discoveryTrustScanSize
	}
	if scanBudget < targetRows {
		scanBudget = targetRows
	}
	if tq != nil {
		rows, ready, err := tq(
			ctx,
			window,
			scanBudget,
			0,
			s.discoveryTrustMode,
			s.discoveryTrustPolicy,
			s.discoveryProjectionMaxStaleness,
		)
		if err != nil {
			return nil, err
		}
		if ready {
			return rows, nil
		}
	}
	out := make([]trustedNoteCandidate, 0, targetRows)
	for fetched := 0; fetched < scanBudget; {
		batchSize := 100
		if remaining := scanBudget - fetched; remaining < batchSize {
			batchSize = remaining
		}
		batch, err := plain(ctx, window, batchSize, fetched)
		if err != nil {
			return nil, err
		}
		if len(batch) == 0 {
			break
		}
		qualified, err := s.qualifyNoteBatch(ctx, batch)
		if err != nil {
			return nil, err
		}
		if s.discoveryTrustMode == trustModeTrustedOnly {
			for _, row := range qualified {
				if row.trusted {
					out = append(out, row)
				}
			}
		} else {
			out = append(out, qualified...)
		}
		if len(out) >= targetRows {
			break
		}
		if len(batch) < batchSize {
			break
		}
		fetched += batchSize
	}
	return out, nil
}

func (s Service) collectTrustedTrendingProfiles(
	ctx context.Context,
	fetch func(context.Context, time.Duration, int, int) ([]TrendingProfile, error),
	rising bool,
	window time.Duration,
	targetRows int,
	scanBudget int,
) ([]trustedProfileCandidate, error) {
	if s.capabilities.trust.qualification == nil {
		return nil, unsupportedCapabilityError("trust qualification")
	}
	if targetRows <= 0 {
		targetRows = 20
	}
	if scanBudget <= 0 {
		scanBudget = targetRows * 4
	}
	if scanBudget > s.discoveryTrustScanSize {
		scanBudget = s.discoveryTrustScanSize
	}
	if scanBudget < targetRows {
		scanBudget = targetRows
	}
	if capability := s.capabilities.curated.trustQualifiedProfiles; capability != nil {
		rows, ready, err := capability.GetTrustQualifiedTrendingProfiles(
			ctx,
			window,
			scanBudget,
			0,
			rising,
			s.discoveryTrustMode,
			s.discoveryTrustPolicy,
			s.discoveryProjectionMaxStaleness,
		)
		if err != nil {
			return nil, err
		}
		if ready {
			return rows, nil
		}
	}
	out := make([]trustedProfileCandidate, 0, targetRows)
	for fetched := 0; fetched < scanBudget; {
		batchSize := 100
		if remaining := scanBudget - fetched; remaining < batchSize {
			batchSize = remaining
		}
		batch, err := fetch(ctx, window, batchSize, fetched)
		if err != nil {
			return nil, err
		}
		if len(batch) == 0 {
			break
		}
		qualified, err := s.qualifyProfileBatch(ctx, batch)
		if err != nil {
			return nil, err
		}
		if s.discoveryTrustMode == trustModeTrustedOnly {
			for _, row := range qualified {
				if row.trusted {
					out = append(out, row)
				}
			}
		} else {
			out = append(out, qualified...)
		}
		if len(out) >= targetRows {
			break
		}
		if len(batch) < batchSize {
			break
		}
		fetched += batchSize
	}
	return out, nil
}

func (s Service) qualifyNoteBatch(ctx context.Context, rows []TrendingNote) ([]trustedNoteCandidate, error) {
	pubkeys := make([]string, 0, len(rows))
	for _, row := range rows {
		pubkeys = append(pubkeys, row.AuthorPubkey)
	}
	trustRows, err := s.GetTrustQualification(ctx, pubkeys, s.discoveryTrustPolicy)
	if err != nil {
		return nil, fmt.Errorf("qualify trending note candidates: %w", err)
	}
	out := make([]trustedNoteCandidate, 0, len(rows))
	for _, row := range rows {
		out = append(out, trustedNoteCandidate{
			note:    row,
			trusted: trustRows[row.AuthorPubkey].Trusted,
		})
	}
	return out, nil
}

func (s Service) qualifyProfileBatch(ctx context.Context, rows []TrendingProfile) ([]trustedProfileCandidate, error) {
	pubkeys := make([]string, 0, len(rows))
	for _, row := range rows {
		pubkeys = append(pubkeys, row.Pubkey)
	}
	trustRows, err := s.GetTrustQualification(ctx, pubkeys, s.discoveryTrustPolicy)
	if err != nil {
		return nil, fmt.Errorf("qualify profile discovery candidates: %w", err)
	}
	out := make([]trustedProfileCandidate, 0, len(rows))
	for _, row := range rows {
		out = append(out, trustedProfileCandidate{
			profile: row,
			trusted: trustRows[row.Pubkey].Trusted,
		})
	}
	return out, nil
}

func trustedNoteRowsByMode(rows []trustedNoteCandidate, mode string) []TrendingNote {
	if mode != trustModePreferTrusted {
		out := make([]TrendingNote, 0, len(rows))
		for _, row := range rows {
			out = append(out, row.note)
		}
		return out
	}
	out := make([]TrendingNote, 0, len(rows))
	for _, row := range rows {
		if row.trusted {
			out = append(out, row.note)
		}
	}
	for _, row := range rows {
		if !row.trusted {
			out = append(out, row.note)
		}
	}
	return out
}

func trustedProfileRowsByMode(rows []trustedProfileCandidate, mode string) []TrendingProfile {
	if mode != trustModePreferTrusted {
		out := make([]TrendingProfile, 0, len(rows))
		for _, row := range rows {
			out = append(out, row.profile)
		}
		return out
	}
	out := make([]TrendingProfile, 0, len(rows))
	for _, row := range rows {
		if row.trusted {
			out = append(out, row.profile)
		}
	}
	for _, row := range rows {
		if !row.trusted {
			out = append(out, row.profile)
		}
	}
	return out
}

func hashtagsFromContent(content string) []string {
	matches := hashtagPattern.FindAllStringSubmatch(strings.ToLower(content), -1)
	if len(matches) == 0 {
		return nil
	}
	out := make([]string, 0, len(matches))
	seen := make(map[string]struct{}, len(matches))
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		hashtag := strings.TrimSpace(match[1])
		if hashtag == "" {
			continue
		}
		if _, ok := seen[hashtag]; ok {
			continue
		}
		seen[hashtag] = struct{}{}
		out = append(out, hashtag)
	}
	return out
}

func normalizeDiscoveryPage(limit int, offset int, defaultLimit int, maxLimit int) (int, int) {
	if limit <= 0 {
		limit = defaultLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}

func paginateTrendingNotes(rows []TrendingNote, limit int, offset int) []TrendingNote {
	if offset >= len(rows) {
		return []TrendingNote{}
	}
	end := offset + limit
	if end > len(rows) {
		end = len(rows)
	}
	return rows[offset:end]
}

func paginateTrendingProfiles(rows []TrendingProfile, limit int, offset int) []TrendingProfile {
	if offset >= len(rows) {
		return []TrendingProfile{}
	}
	end := offset + limit
	if end > len(rows) {
		end = len(rows)
	}
	return rows[offset:end]
}

func paginateTrendingHashtags(rows []TrendingHashtag, limit int, offset int) []TrendingHashtag {
	if offset >= len(rows) {
		return []TrendingHashtag{}
	}
	end := offset + limit
	if end > len(rows) {
		end = len(rows)
	}
	return rows[offset:end]
}
