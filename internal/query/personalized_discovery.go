package query

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/nostr"
	"github.com/xdzczk/nostrmash/internal/readmodel"
)

func (s Service) GetPersonalizedTrendingNotes(ctx context.Context, window time.Duration, scope DiscoveryScope, limit, offset int) ([]TrendingNote, PersonalizationMeta, error) {
	return s.personalizedNotes(ctx, window, scope, limit, offset, []int{1}, func(ctx context.Context, limit, offset int) ([]TrendingNote, error) {
		return s.GetTrendingNotes(ctx, window, scope, limit, offset)
	})
}

func (s Service) GetPersonalizedTrendingLongForm(ctx context.Context, window time.Duration, scope DiscoveryScope, limit, offset int) ([]TrendingNote, PersonalizationMeta, error) {
	return s.personalizedNotes(ctx, window, scope, limit, offset, []int{30023}, func(ctx context.Context, limit, offset int) ([]TrendingNote, error) {
		return s.GetTrendingLongForm(ctx, window, scope, limit, offset)
	})
}

func (s Service) GetPersonalizedTrendingMedia(ctx context.Context, format string, window time.Duration, scope DiscoveryScope, limit, offset int) ([]TrendingNote, PersonalizationMeta, error) {
	kinds, ok := nostr.TrendingMediaKinds(format)
	if !ok {
		return nil, PersonalizationMeta{}, fmt.Errorf("unsupported media ranking %q", format)
	}
	return s.personalizedNotes(ctx, window, scope, limit, offset, kinds, func(ctx context.Context, limit, offset int) ([]TrendingNote, error) {
		return s.GetTrendingMedia(ctx, format, window, scope, limit, offset)
	})
}

func (s Service) GetPersonalizedBookmarkedNotes(ctx context.Context, scope DiscoveryScope, limit, offset int) ([]TrendingNote, PersonalizationMeta, error) {
	limit, offset = s.personalizedPage(limit, offset)
	cohort, err := s.resolveViewerCohort(ctx, scope.Viewer)
	if err != nil {
		return nil, PersonalizationMeta{}, err
	}
	if len(cohort.Follows) == 0 {
		rows, err := s.GetBookmarkedNotes(ctx, scope, limit, offset)
		return rows, s.personalizationMeta(cohort, PersonalizationSourceGlobalFallback), err
	}
	reader, err := s.cohortReader()
	if err != nil {
		return nil, PersonalizationMeta{}, err
	}
	candidates := s.personalizedCandidateLimit()
	cohortRows, globalRows, err := parallel2(
		func() ([]readmodel.CohortNote, error) {
			return reader.GetCohortBookmarkedNotes(ctx, cohort.Follows, scope.Lang(), candidates)
		},
		func() ([]TrendingNote, error) {
			return s.GetBookmarkedNotes(ctx, scope, candidates, 0)
		},
	)
	if err != nil {
		return nil, PersonalizationMeta{}, err
	}
	blended := blendRanked(cohortCountNotes(cohortRows), globalNoteRows(globalRows), s.personalizedCohortWeight, limit, offset)
	return finishNotes(blended), s.personalizationMeta(cohort, PersonalizationSourcePersonalized), nil
}

func (s Service) GetPersonalizedZappedNotes(ctx context.Context, window time.Duration, scope DiscoveryScope, limit, offset int) ([]TrendingNote, PersonalizationMeta, error) {
	limit, offset = s.personalizedPage(limit, offset)
	cohort, err := s.resolveViewerCohort(ctx, scope.Viewer)
	if err != nil {
		return nil, PersonalizationMeta{}, err
	}
	if len(cohort.Follows) == 0 {
		rows, err := s.GetMostZappedNotes(ctx, window, scope, limit, offset)
		return rows, s.personalizationMeta(cohort, PersonalizationSourceGlobalFallback), err
	}
	reader, err := s.cohortReader()
	if err != nil {
		return nil, PersonalizationMeta{}, err
	}
	candidates := s.personalizedCandidateLimit()
	cohortRows, globalRows, err := parallel2(
		func() ([]readmodel.CohortNote, error) {
			return reader.GetCohortZappedNotes(ctx, cohort.Follows, window, scope.Lang(), candidates)
		},
		func() ([]TrendingNote, error) {
			return s.GetMostZappedNotes(ctx, window, scope, candidates, 0)
		},
	)
	if err != nil {
		return nil, PersonalizationMeta{}, err
	}
	blended := blendRanked(cohortCountNotes(cohortRows), globalNoteRows(globalRows), s.personalizedCohortWeight, limit, offset)
	return finishNotes(blended), s.personalizationMeta(cohort, PersonalizationSourcePersonalized), nil
}

func (s Service) GetPersonalizedHotConversations(ctx context.Context, window time.Duration, scope DiscoveryScope, limit, offset int) ([]HotConversation, PersonalizationMeta, error) {
	limit, offset = s.personalizedPage(limit, offset)
	cohort, err := s.resolveViewerCohort(ctx, scope.Viewer)
	if err != nil {
		return nil, PersonalizationMeta{}, err
	}
	if len(cohort.Follows) == 0 {
		rows, err := s.GetHotConversations(ctx, window, scope, limit, offset)
		return rows, s.personalizationMeta(cohort, PersonalizationSourceGlobalFallback), err
	}
	reader, err := s.cohortReader()
	if err != nil {
		return nil, PersonalizationMeta{}, err
	}
	candidates := s.personalizedCandidateLimit()
	cohortRows, globalRows, err := parallel2(
		func() ([]readmodel.CohortConversation, error) {
			return reader.GetCohortHotConversations(ctx, cohort.Follows, window, scope.Lang(), candidates)
		},
		func() ([]HotConversation, error) {
			return s.GetHotConversations(ctx, window, scope, candidates, 0)
		},
	)
	if err != nil {
		return nil, PersonalizationMeta{}, err
	}
	blended := blendRanked(cohortConversationRows(cohortRows), globalConversationRows(globalRows), s.personalizedCohortWeight, limit, offset)
	return finishConversations(blended), s.personalizationMeta(cohort, PersonalizationSourcePersonalized), nil
}

func (s Service) GetPersonalizedFollowedProfiles(ctx context.Context, scope DiscoveryScope, limit, offset int) ([]TrendingProfile, PersonalizationMeta, error) {
	limit, offset = s.personalizedPage(limit, offset)
	cohort, err := s.resolveViewerCohort(ctx, scope.Viewer)
	if err != nil {
		return nil, PersonalizationMeta{}, err
	}
	if len(cohort.Follows) == 0 {
		rows, err := s.GetMostFollowedProfiles(ctx, limit, offset)
		return rows, s.personalizationMeta(cohort, PersonalizationSourceGlobalFallback), err
	}
	reader, err := s.cohortReader()
	if err != nil {
		return nil, PersonalizationMeta{}, err
	}
	candidates := s.personalizedCandidateLimit()
	cohortRows, globalRows, err := parallel2(
		func() ([]readmodel.CohortProfile, error) {
			return reader.GetCohortFollowedProfiles(ctx, cohort.Viewer, cohort.Follows, candidates)
		},
		func() ([]TrendingProfile, error) {
			return s.GetMostFollowedProfiles(ctx, candidates, 0)
		},
	)
	if err != nil {
		return nil, PersonalizationMeta{}, err
	}
	blended := blendRanked(cohortProfileRows(cohortRows), globalProfileRows(globalRows), s.personalizedCohortWeight, limit, offset)
	rows := finishProfiles(blended)
	followed := make(map[string]struct{}, len(cohort.Follows))
	for _, pubkey := range cohort.Follows {
		followed[pubkey] = struct{}{}
	}
	for i := range rows {
		if _, ok := followed[rows[i].Pubkey]; ok {
			rows[i].ViewerFollows = true
		}
	}
	return rows, s.personalizationMeta(cohort, PersonalizationSourcePersonalized), nil
}

func (s Service) GetPersonalizedSupportedProfiles(ctx context.Context, window time.Duration, scope DiscoveryScope, limit, offset int) ([]TrendingProfile, PersonalizationMeta, error) {
	limit, offset = s.personalizedPage(limit, offset)
	cohort, err := s.resolveViewerCohort(ctx, scope.Viewer)
	if err != nil {
		return nil, PersonalizationMeta{}, err
	}
	if len(cohort.Follows) == 0 {
		rows, err := s.GetMostSupportedProfiles(ctx, window, limit, offset)
		return rows, s.personalizationMeta(cohort, PersonalizationSourceGlobalFallback), err
	}
	reader, err := s.cohortReader()
	if err != nil {
		return nil, PersonalizationMeta{}, err
	}
	candidates := s.personalizedCandidateLimit()
	cohortRows, globalRows, err := parallel2(
		func() ([]readmodel.CohortProfile, error) {
			return reader.GetCohortSupportedProfiles(ctx, cohort.Follows, window, derivation.SupportSenderCapSats, candidates)
		},
		func() ([]TrendingProfile, error) {
			return s.GetMostSupportedProfiles(ctx, window, candidates, 0)
		},
	)
	if err != nil {
		return nil, PersonalizationMeta{}, err
	}
	blended := blendRanked(cohortProfileRows(cohortRows), globalProfileRows(globalRows), s.personalizedCohortWeight, limit, offset)
	return finishProfiles(blended), s.personalizationMeta(cohort, PersonalizationSourcePersonalized), nil
}

func (s Service) GetPersonalizedTrendingProfiles(ctx context.Context, window time.Duration, scope DiscoveryScope, limit, offset int) ([]TrendingProfile, PersonalizationMeta, error) {
	return s.personalizedProfiles(ctx, window, scope, limit, offset, false)
}

func (s Service) GetPersonalizedRisingProfiles(ctx context.Context, window time.Duration, scope DiscoveryScope, limit, offset int) ([]TrendingProfile, PersonalizationMeta, error) {
	return s.personalizedProfiles(ctx, window, scope, limit, offset, true)
}

func (s Service) GetPersonalizedTrendingHashtags(ctx context.Context, window time.Duration, scope DiscoveryScope, limit, offset int) ([]TrendingHashtag, PersonalizationMeta, error) {
	limit, offset = s.personalizedPage(limit, offset)
	cohort, err := s.resolveViewerCohort(ctx, scope.Viewer)
	if err != nil {
		return nil, PersonalizationMeta{}, err
	}
	if len(cohort.Follows) == 0 {
		rows, err := s.GetTrendingHashtags(ctx, window, scope, limit, offset)
		return rows, s.personalizationMeta(cohort, PersonalizationSourceGlobalFallback), err
	}
	reader, err := s.cohortReader()
	if err != nil {
		return nil, PersonalizationMeta{}, err
	}
	candidates := s.personalizedCandidateLimit()
	cohortRows, globalRows, err := parallel2(
		func() ([]readmodel.TrendingHashtag, error) {
			return reader.GetCohortTrendingHashtags(ctx, cohort.Follows, window, scope.Lang(), candidates)
		},
		func() ([]TrendingHashtag, error) {
			return s.GetTrendingHashtags(ctx, window, scope, candidates, 0)
		},
	)
	if err != nil {
		return nil, PersonalizationMeta{}, err
	}
	blended := blendRanked(cohortHashtagRows(cohortRows), globalHashtagRows(globalRows), s.personalizedCohortWeight, limit, offset)
	return finishHashtags(blended), s.personalizationMeta(cohort, PersonalizationSourcePersonalized), nil
}

func (s Service) GetPersonalizedTrendingDomains(ctx context.Context, window time.Duration, scope DiscoveryScope, limit, offset int) ([]DomainSummary, PersonalizationMeta, error) {
	limit, offset = s.personalizedPage(limit, offset)
	cohort, err := s.resolveViewerCohort(ctx, scope.Viewer)
	if err != nil {
		return nil, PersonalizationMeta{}, err
	}
	if len(cohort.Follows) == 0 {
		rows, err := s.GetTrendingDomains(ctx, window, scope, limit, offset)
		return rows, s.personalizationMeta(cohort, PersonalizationSourceGlobalFallback), err
	}
	reader, err := s.cohortReader()
	if err != nil {
		return nil, PersonalizationMeta{}, err
	}
	candidates := s.personalizedCandidateLimit()
	cohortRows, globalRows, err := parallel2(
		func() ([]readmodel.CohortDomain, error) {
			return reader.GetCohortTrendingDomains(ctx, cohort.Follows, window, scope.Lang(), candidates)
		},
		func() ([]DomainSummary, error) {
			return s.GetTrendingDomains(ctx, window, scope, candidates, 0)
		},
	)
	if err != nil {
		return nil, PersonalizationMeta{}, err
	}
	blended := blendRanked(cohortDomainRows(window, cohortRows), globalDomainRows(window, globalRows), s.personalizedCohortWeight, limit, offset)
	return finishDomains(blended), s.personalizationMeta(cohort, PersonalizationSourcePersonalized), nil
}

func (s Service) personalizedNotes(
	ctx context.Context,
	window time.Duration,
	scope DiscoveryScope,
	limit, offset int,
	kinds []int,
	global func(context.Context, int, int) ([]TrendingNote, error),
) ([]TrendingNote, PersonalizationMeta, error) {
	limit, offset = s.personalizedPage(limit, offset)
	cohort, err := s.resolveViewerCohort(ctx, scope.Viewer)
	if err != nil {
		return nil, PersonalizationMeta{}, err
	}
	if len(cohort.Follows) == 0 {
		rows, err := global(ctx, limit, offset)
		return rows, s.personalizationMeta(cohort, PersonalizationSourceGlobalFallback), err
	}
	reader, err := s.cohortReader()
	if err != nil {
		return nil, PersonalizationMeta{}, err
	}
	candidates := s.personalizedCandidateLimit()
	cohortRows, globalRows, err := parallel2(
		func() ([]readmodel.CohortNote, error) {
			return reader.GetCohortTrendingNotes(ctx, cohort.Follows, kinds, window, scope.Lang(), candidates)
		},
		func() ([]TrendingNote, error) {
			return global(ctx, candidates, 0)
		},
	)
	if err != nil {
		return nil, PersonalizationMeta{}, err
	}
	blended := blendRanked(cohortScoredNotes(window, cohortRows), globalNoteRows(globalRows), s.personalizedCohortWeight, limit, offset)
	return finishNotes(blended), s.personalizationMeta(cohort, PersonalizationSourcePersonalized), nil
}

func (s Service) personalizedProfiles(ctx context.Context, window time.Duration, scope DiscoveryScope, limit, offset int, rising bool) ([]TrendingProfile, PersonalizationMeta, error) {
	limit, offset = s.personalizedPage(limit, offset)
	cohort, err := s.resolveViewerCohort(ctx, scope.Viewer)
	if err != nil {
		return nil, PersonalizationMeta{}, err
	}
	global := s.GetTrendingProfiles
	if rising {
		global = s.GetRisingProfiles
	}
	if len(cohort.Follows) == 0 {
		rows, err := global(ctx, window, scope, limit, offset)
		return rows, s.personalizationMeta(cohort, PersonalizationSourceGlobalFallback), err
	}
	reader, err := s.cohortReader()
	if err != nil {
		return nil, PersonalizationMeta{}, err
	}
	candidates := s.personalizedCandidateLimit()
	cohortRows, globalRows, err := parallel2(
		func() ([]readmodel.CohortProfile, error) {
			if rising {
				return reader.GetCohortRisingProfiles(ctx, cohort.Viewer, cohort.Follows, window, scope.Lang(), candidates)
			}
			return reader.GetCohortTrendingProfiles(ctx, cohort.Follows, window, scope.Lang(), candidates)
		},
		func() ([]TrendingProfile, error) {
			return global(ctx, window, scope, candidates, 0)
		},
	)
	if err != nil {
		return nil, PersonalizationMeta{}, err
	}
	blended := blendRanked(cohortProfileRows(cohortRows), globalProfileRows(globalRows), s.personalizedCohortWeight, limit, offset)
	return finishProfiles(blended), s.personalizationMeta(cohort, PersonalizationSourcePersonalized), nil
}

func (s Service) cohortReader() (personalizedCohortCapability, error) {
	if s.capabilities.curated.personalizedCohort == nil {
		return nil, unsupportedCapabilityError("personalized discovery")
	}
	return s.capabilities.curated.personalizedCohort, nil
}

func parallel2[A, B any](loadA func() (A, error), loadB func() (B, error)) (A, B, error) {
	var (
		a    A
		b    B
		errA error
		errB error
		wait sync.WaitGroup
	)
	wait.Add(2)
	go func() {
		defer wait.Done()
		a, errA = loadA()
	}()
	go func() {
		defer wait.Done()
		b, errB = loadB()
	}()
	wait.Wait()
	return a, b, errors.Join(errA, errB)
}

func cohortScoredNotes(window time.Duration, rows []readmodel.CohortNote) []blendRow[TrendingNote] {
	now := time.Now().Unix()
	out := make([]blendRow[TrendingNote], 0, len(rows))
	for _, row := range rows {
		item := trendingNoteFromStore(row.Note)
		score := row.CohortScore
		evidence := row.CohortEvidence
		if score == 0 {
			weights := row.ReplyWeight + row.RepostWeight + row.ReactionWeight + row.ZapWeight
			if weights > 0 || row.ZapMSats > 0 {
				score = derivation.CohortTrendingScore(window, now, row.Note.CreatedAt, row.ReplyWeight, row.RepostWeight, row.ReactionWeight, row.ZapWeight, row.ZapMSats)
				evidence = weights
			}
		}
		if evidence == 0 {
			evidence = score
		}
		out = append(out, blendRow[TrendingNote]{
			ID:             item.EventID,
			Item:           item,
			CohortScore:    score,
			CohortEvidence: evidence,
		})
	}
	return out
}

func cohortCountNotes(rows []readmodel.CohortNote) []blendRow[TrendingNote] {
	out := make([]blendRow[TrendingNote], 0, len(rows))
	for _, row := range rows {
		item := trendingNoteFromStore(row.Note)
		evidence := row.CohortEvidence
		if evidence == 0 {
			evidence = row.CohortScore
		}
		out = append(out, blendRow[TrendingNote]{
			ID:             item.EventID,
			Item:           item,
			CohortScore:    row.CohortScore,
			CohortEvidence: evidence,
		})
	}
	return out
}

func globalNoteRows(rows []TrendingNote) []blendRow[TrendingNote] {
	out := make([]blendRow[TrendingNote], 0, len(rows))
	for _, row := range rows {
		out = append(out, blendRow[TrendingNote]{ID: row.EventID, Item: row, GlobalScore: row.Score})
	}
	return out
}

func finishNotes(rows []Blended[TrendingNote]) []TrendingNote {
	out := make([]TrendingNote, 0, len(rows))
	for _, row := range rows {
		item := row.Item
		item.Score = row.Score
		item.CohortEvidence = row.CohortEvidence
		out = append(out, item)
	}
	return out
}

func cohortConversationRows(rows []readmodel.CohortConversation) []blendRow[HotConversation] {
	out := make([]blendRow[HotConversation], 0, len(rows))
	for _, row := range rows {
		item := hotConversationFromStore(row.Conversation)
		evidence := row.CohortParticipants
		if row.CohortReplies > evidence {
			evidence = row.CohortReplies
		}
		out = append(out, blendRow[HotConversation]{
			ID:             item.RootEventID,
			Item:           item,
			CohortScore:    row.CohortReplies,
			GlobalScore:    item.VelocityScore,
			CohortEvidence: evidence,
		})
	}
	return out
}

func globalConversationRows(rows []HotConversation) []blendRow[HotConversation] {
	out := make([]blendRow[HotConversation], 0, len(rows))
	for _, row := range rows {
		out = append(out, blendRow[HotConversation]{ID: row.RootEventID, Item: row, GlobalScore: row.VelocityScore})
	}
	return out
}

func finishConversations(rows []Blended[HotConversation]) []HotConversation {
	out := make([]HotConversation, 0, len(rows))
	for _, row := range rows {
		item := row.Item
		item.BlendScore = row.Score
		item.CohortEvidence = row.CohortEvidence
		out = append(out, item)
	}
	return out
}

func cohortProfileRows(rows []readmodel.CohortProfile) []blendRow[TrendingProfile] {
	out := make([]blendRow[TrendingProfile], 0, len(rows))
	for _, row := range rows {
		item := trendingProfileFromStore(row.Profile)
		item.ViewerFollows = row.ViewerFollows
		evidence := row.CohortEvidence
		if evidence == 0 {
			evidence = row.CohortScore
		}
		out = append(out, blendRow[TrendingProfile]{
			ID:             item.Pubkey,
			Item:           item,
			CohortScore:    row.CohortScore,
			CohortEvidence: evidence,
		})
	}
	return out
}

func globalProfileRows(rows []TrendingProfile) []blendRow[TrendingProfile] {
	out := make([]blendRow[TrendingProfile], 0, len(rows))
	for _, row := range rows {
		score := row.Score
		if score == 0 && row.CappedSats > 0 {
			score = float64(row.CappedSats)
		}
		if score == 0 && row.TrustedFollowerCount > 0 {
			score = float64(row.TrustedFollowerCount)
		}
		out = append(out, blendRow[TrendingProfile]{ID: row.Pubkey, Item: row, GlobalScore: score})
	}
	return out
}

func finishProfiles(rows []Blended[TrendingProfile]) []TrendingProfile {
	out := make([]TrendingProfile, 0, len(rows))
	for _, row := range rows {
		item := row.Item
		item.Score = row.Score
		item.CohortEvidence = row.CohortEvidence
		out = append(out, item)
	}
	return out
}

func cohortHashtagRows(rows []readmodel.TrendingHashtag) []blendRow[TrendingHashtag] {
	out := make([]blendRow[TrendingHashtag], 0, len(rows))
	for _, row := range rows {
		item := trendingHashtagFromStore(row)
		out = append(out, blendRow[TrendingHashtag]{
			ID:             item.Hashtag,
			Item:           item,
			CohortScore:    float64(row.UniqueAuthors),
			CohortEvidence: float64(row.UniqueAuthors),
		})
	}
	return out
}

func globalHashtagRows(rows []TrendingHashtag) []blendRow[TrendingHashtag] {
	out := make([]blendRow[TrendingHashtag], 0, len(rows))
	for _, row := range rows {
		out = append(out, blendRow[TrendingHashtag]{
			ID:          row.Hashtag,
			Item:        row,
			GlobalScore: float64(row.UniqueAuthors),
		})
	}
	return out
}

func finishHashtags(rows []Blended[TrendingHashtag]) []TrendingHashtag {
	out := make([]TrendingHashtag, 0, len(rows))
	for _, row := range rows {
		item := row.Item
		item.BlendScore = row.Score
		item.CohortEvidence = row.CohortEvidence
		out = append(out, item)
	}
	return out
}

func cohortDomainRows(window time.Duration, rows []readmodel.CohortDomain) []blendRow[DomainSummary] {
	out := make([]blendRow[DomainSummary], 0, len(rows))
	for _, row := range rows {
		out = append(out, blendRow[DomainSummary]{
			ID:             row.Domain,
			Item:           cohortDomainItem(window, row),
			CohortScore:    float64(row.UniqueAuthors),
			CohortEvidence: float64(row.UniqueAuthors),
		})
	}
	return out
}

func globalDomainRows(window time.Duration, rows []DomainSummary) []blendRow[DomainSummary] {
	out := make([]blendRow[DomainSummary], 0, len(rows))
	for _, row := range rows {
		out = append(out, blendRow[DomainSummary]{
			ID:          row.Domain,
			Item:        row,
			GlobalScore: float64(domainWindowActivity(window, row).UniqueAuthors),
		})
	}
	return out
}

func finishDomains(rows []Blended[DomainSummary]) []DomainSummary {
	out := make([]DomainSummary, 0, len(rows))
	for _, row := range rows {
		item := row.Item
		item.BlendScore = row.Score
		item.CohortEvidence = row.CohortEvidence
		out = append(out, item)
	}
	return out
}

func cohortDomainItem(window time.Duration, row readmodel.CohortDomain) DomainSummary {
	activity := DomainActivity{LinkCount: row.LinkCount, NoteCount: row.NoteCount, UniqueAuthors: row.UniqueAuthors}
	summary := DomainSummary{Domain: row.Domain, LatestEventAt: row.LatestEventAt}
	switch window {
	case 7 * 24 * time.Hour:
		summary.Activity.Last7d = activity
	case 30 * 24 * time.Hour:
		summary.Activity.Last30d = activity
	default:
		summary.Activity.Last24h = activity
	}
	return summary
}

func domainWindowActivity(window time.Duration, summary DomainSummary) DomainActivity {
	switch window {
	case 7 * 24 * time.Hour:
		return summary.Activity.Last7d
	case 30 * 24 * time.Hour:
		return summary.Activity.Last30d
	default:
		return summary.Activity.Last24h
	}
}
