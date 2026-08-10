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
	score   *float64
	rank    *int64
}

type trustedProfileCandidate struct {
	profile TrendingProfile
	trusted bool
	score   *float64
	rank    *int64
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
	tqFetch := queryTrustQualifiedNotesFetch(s.capabilities.curated.trustQualifiedNotes)
	return s.getTrendingTrustAware(ctx, window, limit, offset, queryTrendingNotesFetch(notesCap.GetTrendingNotes), tqFetch)
}

func (s Service) getTrendingLongFormTrustAware(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingNote, error) {
	longFormCap := s.capabilities.curated.trendingLongForm
	if longFormCap == nil {
		return nil, unsupportedCapabilityError("trending long-form")
	}
	// Long-form volume is low, so we always qualify on the fly rather than
	// maintaining a dedicated trust projection.
	return s.getTrendingTrustAware(ctx, window, limit, offset, queryTrendingNotesFetch(longFormCap.GetTrendingLongForm), nil)
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
	return paginateTrendingNotes(trustedNoteRowsByMode(candidates, s.discoveryTrustMode, s.discoveryScoreBoostWeight), limit, offset), nil
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
	return paginateTrendingProfiles(trustedProfileRowsByMode(candidates, s.discoveryTrustMode, s.discoveryScoreBoostWeight), limit, offset), nil
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
		eventCount  int64
		authors     map[string]struct{}
		trustWeight float64
	}
	agg := make(map[string]*hashtagAgg, len(notes))
	for _, candidate := range notes {
		if s.discoveryTrustMode == trustModeTrustedOnly && !candidate.trusted {
			continue
		}
		authorTrust := candidateTrustSignal(candidate.score, candidate.rank)
		for _, hashtag := range hashtagsFromContent(candidate.note.Content) {
			row := agg[hashtag]
			if row == nil {
				row = &hashtagAgg{authors: map[string]struct{}{}}
				agg[hashtag] = row
			}
			row.eventCount++
			row.authors[candidate.note.AuthorPubkey] = struct{}{}
			row.trustWeight += authorTrust
		}
	}
	type hashtagSortRow struct {
		tag         TrendingHashtag
		trustWeight float64
	}
	sortable := make([]hashtagSortRow, 0, len(agg))
	for hashtag, row := range agg {
		sortable = append(sortable, hashtagSortRow{
			tag: TrendingHashtag{
				Hashtag:       hashtag,
				EventCount:    row.eventCount,
				UniqueAuthors: int64(len(row.authors)),
			},
			trustWeight: row.trustWeight,
		})
	}
	boostWeight := s.discoveryScoreBoostWeight
	sort.SliceStable(sortable, func(i, j int) bool {
		if sortable[i].tag.EventCount != sortable[j].tag.EventCount {
			return sortable[i].tag.EventCount > sortable[j].tag.EventCount
		}
		if boostWeight > 0 && sortable[i].trustWeight != sortable[j].trustWeight {
			return sortable[i].trustWeight > sortable[j].trustWeight
		}
		if sortable[i].tag.UniqueAuthors != sortable[j].tag.UniqueAuthors {
			return sortable[i].tag.UniqueAuthors > sortable[j].tag.UniqueAuthors
		}
		return sortable[i].tag.Hashtag < sortable[j].tag.Hashtag
	})
	out := make([]TrendingHashtag, 0, len(sortable))
	for _, row := range sortable {
		out = append(out, row.tag)
	}
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
	tqFetch := queryTrustQualifiedNotesFetch(s.capabilities.curated.trustQualifiedNotes)
	return s.collectTrustedTrending(ctx, window, targetRows, scanBudget, queryTrendingNotesFetch(capability.GetTrendingNotes), tqFetch)
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
		switch {
		case err != nil && !IsUnsupportedCapability(err):
			return nil, err
		case err == nil && ready:
			if s.discoveryScoreBoostWeight > 0 {
				rows, err = s.enrichNoteCandidatesTrustSignals(ctx, rows)
				if err != nil {
					return nil, err
				}
			}
			return rows, nil
		}
		// A missing or unsupported trust-qualified projection degrades to
		// on-the-fly qualification below.
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
		storeRows, ready, err := capability.GetTrustQualifiedTrendingProfiles(
			ctx,
			window,
			scanBudget,
			0,
			rising,
			s.discoveryTrustMode,
			trustQualificationPolicyToStore(s.discoveryTrustPolicy),
			s.discoveryProjectionMaxStaleness,
		)
		switch {
		case err != nil && !IsUnsupportedCapability(err):
			return nil, err
		case err == nil && ready:
			rows := make([]trustedProfileCandidate, 0, len(storeRows))
			for _, row := range storeRows {
				rows = append(rows, trustedProfileCandidate{
					profile: trendingProfileFromStore(row.Profile),
					trusted: row.Trusted,
				})
			}
			if s.discoveryScoreBoostWeight > 0 {
				rows, err = s.enrichProfileCandidatesTrustSignals(ctx, rows)
				if err != nil {
					return nil, err
				}
			}
			return rows, nil
		}
		// A missing or unsupported trust-qualified projection degrades to
		// on-the-fly qualification below.
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
		qualification := trustRows[row.AuthorPubkey]
		out = append(out, trustedNoteCandidate{
			note:    row,
			trusted: qualification.Trusted,
			score:   qualification.Score,
			rank:    qualification.Rank,
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
		qualification := trustRows[row.Pubkey]
		out = append(out, trustedProfileCandidate{
			profile: row,
			trusted: qualification.Trusted,
			score:   qualification.Score,
			rank:    qualification.Rank,
		})
	}
	return out, nil
}

func (s Service) enrichNoteCandidatesTrustSignals(
	ctx context.Context,
	rows []trustedNoteCandidate,
) ([]trustedNoteCandidate, error) {
	if len(rows) == 0 {
		return rows, nil
	}
	pubkeys := make([]string, 0, len(rows))
	for _, row := range rows {
		pubkeys = append(pubkeys, row.note.AuthorPubkey)
	}
	trustRows, err := s.GetTrustQualification(ctx, pubkeys, s.discoveryTrustPolicy)
	if err != nil {
		return nil, fmt.Errorf("enrich trending note trust signals: %w", err)
	}
	for i := range rows {
		qualification := trustRows[rows[i].note.AuthorPubkey]
		rows[i].score = qualification.Score
		rows[i].rank = qualification.Rank
	}
	return rows, nil
}

func (s Service) enrichProfileCandidatesTrustSignals(
	ctx context.Context,
	rows []trustedProfileCandidate,
) ([]trustedProfileCandidate, error) {
	if len(rows) == 0 {
		return rows, nil
	}
	pubkeys := make([]string, 0, len(rows))
	for _, row := range rows {
		pubkeys = append(pubkeys, row.profile.Pubkey)
	}
	trustRows, err := s.GetTrustQualification(ctx, pubkeys, s.discoveryTrustPolicy)
	if err != nil {
		return nil, fmt.Errorf("enrich profile discovery trust signals: %w", err)
	}
	for i := range rows {
		qualification := trustRows[rows[i].profile.Pubkey]
		rows[i].score = qualification.Score
		rows[i].rank = qualification.Rank
	}
	return rows, nil
}

func trustedNoteRowsByMode(rows []trustedNoteCandidate, mode string, boostWeight float64) []TrendingNote {
	if mode != trustModePreferTrusted {
		out := make([]TrendingNote, 0, len(rows))
		for _, row := range rows {
			out = append(out, row.note)
		}
		return out
	}
	trusted := make([]trustedNoteCandidate, 0, len(rows))
	untrusted := make([]trustedNoteCandidate, 0, len(rows))
	for _, row := range rows {
		if row.trusted {
			trusted = append(trusted, row)
		} else {
			untrusted = append(untrusted, row)
		}
	}
	orderNoteBucket(trusted, boostWeight)
	orderNoteBucket(untrusted, boostWeight)
	out := make([]TrendingNote, 0, len(rows))
	for _, row := range trusted {
		out = append(out, row.note)
	}
	for _, row := range untrusted {
		out = append(out, row.note)
	}
	return out
}

func trustedProfileRowsByMode(rows []trustedProfileCandidate, mode string, boostWeight float64) []TrendingProfile {
	if mode != trustModePreferTrusted {
		out := make([]TrendingProfile, 0, len(rows))
		for _, row := range rows {
			out = append(out, row.profile)
		}
		return out
	}
	trusted := make([]trustedProfileCandidate, 0, len(rows))
	untrusted := make([]trustedProfileCandidate, 0, len(rows))
	for _, row := range rows {
		if row.trusted {
			trusted = append(trusted, row)
		} else {
			untrusted = append(untrusted, row)
		}
	}
	orderProfileBucket(trusted, boostWeight)
	orderProfileBucket(untrusted, boostWeight)
	out := make([]TrendingProfile, 0, len(rows))
	for _, row := range trusted {
		out = append(out, row.profile)
	}
	for _, row := range untrusted {
		out = append(out, row.profile)
	}
	return out
}

func orderNoteBucket(rows []trustedNoteCandidate, boostWeight float64) {
	if boostWeight <= 0 || len(rows) < 2 {
		return
	}
	type keyed struct {
		row   trustedNoteCandidate
		index int
		key   float64
	}
	items := make([]keyed, len(rows))
	for i, row := range rows {
		items[i] = keyed{
			row:   row,
			index: i,
			key:   float64(i) + boostWeight*normalizedTrustRank(row.score, row.rank, rows),
		}
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].key != items[j].key {
			return items[i].key < items[j].key
		}
		return items[i].index < items[j].index
	})
	for i := range items {
		rows[i] = items[i].row
	}
}

func orderProfileBucket(rows []trustedProfileCandidate, boostWeight float64) {
	if boostWeight <= 0 || len(rows) < 2 {
		return
	}
	type keyed struct {
		row   trustedProfileCandidate
		index int
		key   float64
	}
	items := make([]keyed, len(rows))
	for i, row := range rows {
		items[i] = keyed{
			row:   row,
			index: i,
			key:   float64(i) + boostWeight*normalizedTrustRankForProfiles(row.score, row.rank, rows),
		}
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].key != items[j].key {
			return items[i].key < items[j].key
		}
		return items[i].index < items[j].index
	})
	for i := range items {
		rows[i] = items[i].row
	}
}

// normalizedTrustRank returns a [0,1] value where lower is better trust.
// Prefer global rank when present; otherwise invert score against the bucket max.
func normalizedTrustRank(score *float64, rank *int64, peers []trustedNoteCandidate) float64 {
	if rank != nil && *rank > 0 {
		maxRank := *rank
		for _, peer := range peers {
			if peer.rank != nil && *peer.rank > maxRank {
				maxRank = *peer.rank
			}
		}
		if maxRank <= 1 {
			return 0
		}
		return float64(*rank-1) / float64(maxRank-1)
	}
	if score != nil {
		maxScore := *score
		for _, peer := range peers {
			if peer.score != nil && *peer.score > maxScore {
				maxScore = *peer.score
			}
		}
		if maxScore <= 0 {
			return 1
		}
		return 1.0 - (*score / maxScore)
	}
	return 1
}

func normalizedTrustRankForProfiles(score *float64, rank *int64, peers []trustedProfileCandidate) float64 {
	if rank != nil && *rank > 0 {
		maxRank := *rank
		for _, peer := range peers {
			if peer.rank != nil && *peer.rank > maxRank {
				maxRank = *peer.rank
			}
		}
		if maxRank <= 1 {
			return 0
		}
		return float64(*rank-1) / float64(maxRank-1)
	}
	if score != nil {
		maxScore := *score
		for _, peer := range peers {
			if peer.score != nil && *peer.score > maxScore {
				maxScore = *peer.score
			}
		}
		if maxScore <= 0 {
			return 1
		}
		return 1.0 - (*score / maxScore)
	}
	return 1
}

// candidateTrustSignal is a higher-is-better weight for hashtag aggregation.
func candidateTrustSignal(score *float64, rank *int64) float64 {
	if score != nil && *score > 0 {
		return *score
	}
	if rank != nil && *rank > 0 {
		return 1.0 / float64(*rank)
	}
	return 0
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
