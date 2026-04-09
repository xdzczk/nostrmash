package query

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

var authorAnalyticsWindowByLabel = map[string]int{
	"7d":  7,
	"30d": 30,
	"90d": 90,
}

var authorAnalyticsAgeByLabel = map[string]int{
	"3d":   3,
	"7d":   7,
	"14d":  14,
	"30d":  30,
	"60d":  60,
	"90d":  90,
	"180d": 180,
	"365d": 365,
}

var groupedAnalyticsAllowedMetadataTags = map[string]struct{}{
	"d":      {},
	"g":      {},
	"group":  {},
	"series": {},
}

var groupedAnalyticsKeyPattern = regexp.MustCompile(`^[a-z0-9._:/-]{1,128}$`)

func (s Service) GetAuthorAnalyticsSummary(ctx context.Context, pubkey string) (AuthorAnalyticsSummary, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return AuthorAnalyticsSummary{}, fmt.Errorf("pubkey is required")
	}
	summary, err := s.reader.GetAuthorAnalyticsSummary(ctx, pubkey)
	if err != nil {
		return AuthorAnalyticsSummary{}, err
	}
	topLanguages, langErr := s.reader.GetAuthorTopLanguages(ctx, pubkey, 30, 8)
	if langErr == nil {
		summary.TopLanguages = topLanguages
	}
	return summary, nil
}

func (s Service) GetAuthorTopicStats(
	ctx context.Context,
	pubkey string,
	window string,
	limit int,
) ([]AuthorTopicStat, int, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, 0, fmt.Errorf("pubkey is required")
	}
	windowDays, err := normalizeAuthorAnalyticsWindow(window)
	if err != nil {
		return nil, 0, err
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	rows, err := s.reader.GetAuthorTopicStats(ctx, pubkey, windowDays, limit)
	if err != nil {
		return nil, 0, err
	}
	return rows, windowDays, nil
}

func (s Service) GetAuthorMediaMix(
	ctx context.Context,
	pubkey string,
	window string,
) (AuthorAnalyticsMediaMix, int, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return AuthorAnalyticsMediaMix{}, 0, fmt.Errorf("pubkey is required")
	}
	windowDays, err := normalizeAuthorAnalyticsWindow(window)
	if err != nil {
		return AuthorAnalyticsMediaMix{}, 0, err
	}
	row, err := s.reader.GetAuthorMediaMixStats(ctx, pubkey, windowDays)
	if err != nil {
		return AuthorAnalyticsMediaMix{}, 0, err
	}
	return row, windowDays, nil
}

func (s Service) GetAuthorActivityWindows(
	ctx context.Context,
	pubkey string,
	window string,
) (AuthorActivityWindows, int, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return AuthorActivityWindows{}, 0, fmt.Errorf("pubkey is required")
	}
	windowDays, err := normalizeAuthorAnalyticsWindow(window)
	if err != nil {
		return AuthorActivityWindows{}, 0, err
	}
	row, err := s.reader.GetAuthorActivityWindows(ctx, pubkey, windowDays)
	if err != nil {
		return AuthorActivityWindows{}, 0, err
	}
	return row, windowDays, nil
}

func (s Service) GetAuthorPostingPatterns(
	ctx context.Context,
	pubkey string,
	window string,
) (AuthorPostingPatterns, int, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return AuthorPostingPatterns{}, 0, fmt.Errorf("pubkey is required")
	}
	windowDays, err := normalizeAuthorAnalyticsWindow(window)
	if err != nil {
		return AuthorPostingPatterns{}, 0, err
	}
	row, err := s.reader.GetAuthorPostingPatterns(ctx, pubkey, windowDays)
	if err != nil {
		return AuthorPostingPatterns{}, 0, err
	}
	return row, windowDays, nil
}

func (s Service) GetAuthorTopNotes(
	ctx context.Context,
	pubkey string,
	window string,
	limit int,
) ([]AuthorTopNote, int, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, 0, fmt.Errorf("pubkey is required")
	}
	windowDays, err := normalizeAuthorAnalyticsWindow(window)
	if err != nil {
		return nil, 0, err
	}
	if limit <= 0 {
		limit = 10
	}
	if limit > 50 {
		limit = 50
	}
	rows, err := s.reader.GetAuthorTopNotes(ctx, pubkey, windowDays, limit)
	if err != nil {
		return nil, 0, err
	}
	return rows, windowDays, nil
}

func (s Service) GetAuthorRecycleCandidates(
	ctx context.Context,
	pubkey string,
	window string,
	minAge string,
	limit int,
	minPerformancePercentile float64,
	includeReplies bool,
) ([]AuthorRecycleCandidate, AuthorRecycleCandidateFilter, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, AuthorRecycleCandidateFilter{}, fmt.Errorf("pubkey is required")
	}
	if strings.TrimSpace(window) == "" {
		window = "90d"
	}
	windowDays, err := normalizeAuthorAnalyticsWindow(window)
	if err != nil {
		return nil, AuthorRecycleCandidateFilter{}, err
	}
	minAgeDays, minAgeLabel, err := normalizeAuthorAnalyticsAge(minAge, windowDays)
	if err != nil {
		return nil, AuthorRecycleCandidateFilter{}, err
	}
	if minAgeDays >= windowDays {
		return nil, AuthorRecycleCandidateFilter{}, fmt.Errorf("min_age must be less than window")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 50 {
		limit = 50
	}
	if minPerformancePercentile < 0 || minPerformancePercentile > 100 {
		return nil, AuthorRecycleCandidateFilter{}, fmt.Errorf("min_performance_percentile must be between 0 and 100")
	}
	recentRepostWindowDays := 30
	rows, err := s.reader.GetAuthorRecycleCandidates(
		ctx,
		pubkey,
		windowDays,
		minAgeDays,
		minPerformancePercentile,
		includeReplies,
		true,
		recentRepostWindowDays,
		limit,
	)
	if err != nil {
		return nil, AuthorRecycleCandidateFilter{}, err
	}
	return rows, AuthorRecycleCandidateFilter{
		Window:                   fmt.Sprintf("%dd", windowDays),
		MinAge:                   minAgeLabel,
		MinPerformancePercentile: minPerformancePercentile,
		IncludeReplies:           includeReplies,
		ExcludeRecentlyReposted:  true,
		RecentRepostWindow:       fmt.Sprintf("%dd", recentRepostWindowDays),
	}, nil
}

func (s Service) GetAuthorPerformanceSummary(
	ctx context.Context,
	pubkey string,
	window string,
) (AuthorPerformanceSummary, int, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return AuthorPerformanceSummary{}, 0, fmt.Errorf("pubkey is required")
	}
	windowDays, err := normalizeAuthorAnalyticsWindow(window)
	if err != nil {
		return AuthorPerformanceSummary{}, 0, err
	}
	row, err := s.reader.GetAuthorPerformanceSummary(ctx, pubkey, windowDays)
	if err != nil {
		return AuthorPerformanceSummary{}, 0, err
	}
	return row, windowDays, nil
}

func (s Service) GetGroupedNoteAnalytics(
	ctx context.Context,
	pubkey string,
	window string,
	groupBy string,
	groupKey string,
	metadataTag string,
	topNotesLimit int,
	topicsLimit int,
) (GroupedNoteAnalyticsSummary, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return GroupedNoteAnalyticsSummary{}, fmt.Errorf("pubkey is required")
	}
	windowDays, err := normalizeAuthorAnalyticsWindow(window)
	if err != nil {
		return GroupedNoteAnalyticsSummary{}, err
	}
	groupKind, err := normalizeGroupedAnalyticsKind(groupBy)
	if err != nil {
		return GroupedNoteAnalyticsSummary{}, err
	}
	normalizedGroupKey, err := normalizeGroupedAnalyticsKey(groupKey, groupKind)
	if err != nil {
		return GroupedNoteAnalyticsSummary{}, err
	}
	normalizedMetadataTag, err := normalizeGroupedAnalyticsMetadataTag(groupKind, metadataTag)
	if err != nil {
		return GroupedNoteAnalyticsSummary{}, err
	}
	if topNotesLimit <= 0 {
		topNotesLimit = 5
	}
	if topNotesLimit > 20 {
		topNotesLimit = 20
	}
	if topicsLimit <= 0 {
		topicsLimit = 5
	}
	if topicsLimit > 20 {
		topicsLimit = 20
	}
	if r := s.capabilities.curated.groupedNoteAnalytics; r != nil {
		return r.GetGroupedNoteAnalytics(ctx, GroupedNoteAnalyticsRequest{
			Pubkey:        pubkey,
			WindowDays:    windowDays,
			GroupKind:     groupKind,
			GroupKey:      normalizedGroupKey,
			MetadataTag:   normalizedMetadataTag,
			TopNotesLimit: topNotesLimit,
			TopicsLimit:   topicsLimit,
		})
	}
	return GroupedNoteAnalyticsSummary{}, unsupportedCapabilityError("grouped note analytics")
}

func normalizeAuthorAnalyticsWindow(window string) (int, error) {
	normalized := strings.ToLower(strings.TrimSpace(window))
	if normalized == "" {
		normalized = "30d"
	}
	windowDays, ok := authorAnalyticsWindowByLabel[normalized]
	if !ok {
		return 0, fmt.Errorf("window must be one of: 7d, 30d, 90d")
	}
	return windowDays, nil
}

func normalizeAuthorAnalyticsAge(minAge string, windowDays int) (int, string, error) {
	normalized := strings.ToLower(strings.TrimSpace(minAge))
	if normalized == "" {
		switch {
		case windowDays <= 7:
			normalized = "3d"
		case windowDays <= 30:
			normalized = "14d"
		default:
			normalized = "30d"
		}
	}
	days, ok := authorAnalyticsAgeByLabel[normalized]
	if !ok {
		return 0, "", fmt.Errorf("min_age must be one of: 3d, 7d, 14d, 30d, 60d, 90d, 180d, 365d")
	}
	return days, normalized, nil
}

func normalizeGroupedAnalyticsKind(groupBy string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(groupBy))
	if normalized == "" {
		normalized = "hashtag"
	}
	switch normalized {
	case "hashtag", "metadata":
		return normalized, nil
	default:
		return "", fmt.Errorf("group_by must be one of: hashtag, metadata")
	}
}

func normalizeGroupedAnalyticsKey(groupKey string, groupKind string) (string, error) {
	key := strings.ToLower(strings.TrimSpace(groupKey))
	if groupKind == "hashtag" {
		key = strings.TrimPrefix(key, "#")
	}
	if key == "" {
		return "", fmt.Errorf("group_key is required")
	}
	if !groupedAnalyticsKeyPattern.MatchString(key) {
		return "", fmt.Errorf("group_key must be 1-128 chars of [a-z0-9._:/-]")
	}
	return key, nil
}

func normalizeGroupedAnalyticsMetadataTag(groupKind string, metadataTag string) (string, error) {
	if groupKind != "metadata" {
		return "", nil
	}
	tag := strings.ToLower(strings.TrimSpace(metadataTag))
	if tag == "" {
		return "", fmt.Errorf("metadata_tag is required when group_by=metadata")
	}
	if _, ok := groupedAnalyticsAllowedMetadataTags[tag]; !ok {
		return "", fmt.Errorf("metadata_tag must be one of: d, g, group, series")
	}
	return tag, nil
}
