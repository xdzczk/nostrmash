package query

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/store/traceutil"
)

type profileService struct {
	reader   ProfileReader
	fallback FallbackReader
	policy   fallbackPolicyRuntime
}

// NewProfileService constructs a profile-only orchestration service from a narrow dependency.
func NewProfileService(reader ProfileReader) ProfileService {
	return profileService{reader: reader}
}

func (s Service) GetUserInfos(ctx context.Context, pubkeys []string) (UserInfosResult, error) {
	return profileService{reader: s.reader, fallback: s.fallback, policy: s.fallbackPolicy()}.GetProfiles(ctx, pubkeys)
}

func (s Service) GetProfiles(ctx context.Context, pubkeys []string) (UserInfosResult, error) {
	return s.GetUserInfos(ctx, pubkeys)
}

func (s profileService) GetProfile(ctx context.Context, pubkey string) (Profile, error) {
	normalized := strings.TrimSpace(pubkey)
	if normalized == "" {
		return Profile{}, fmt.Errorf("pubkey is required")
	}
	row, err := s.reader.GetProfileByPubkey(ctx, normalized)
	if err == nil {
		metrics.ObserveLookupLocal("profile_by_pubkey", true)
		return row, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return Profile{}, err
	}
	metrics.ObserveLookupLocal("profile_by_pubkey", false)
	if s.fallback == nil {
		return Profile{}, err
	}
	allowedPubkeys, allTrusted := s.policy.admitProfiles(ctx, []string{normalized}, fallbackLookupDirect)
	if len(allowedPubkeys) == 0 {
		return Profile{}, err
	}
	maxAttempts, maxTimeBudget := s.policy.executionBounds(!allTrusted)
	started := time.Now()
	fallbackCtx, fallbackSpan := traceutil.StartSpan(ctx, "query.get_profile.fallback", traceutil.KV("fallback.surface", "profile_by_pubkey"))
	budgetCtx, cancel := withFallbackTimeBudget(fallbackCtx, maxTimeBudget)
	defer cancel()
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		observeFallbackAttemptByEntity(fallbackEntityProfile)
		foundByPubkey, fallbackErr := s.fallback.FetchProfilesByPubkeys(budgetCtx, allowedPubkeys)
		if fallbackErr != nil {
			lastErr = fallbackErr
			if budgetCtx.Err() != nil {
				break
			}
			continue
		}
		profile, ok := foundByPubkey[normalized]
		if ok {
			fallbackSpan.End(nil)
			observeFallbackResultByEntity(fallbackEntityProfile, fallbackResultHit, time.Since(started))
			return profile, nil
		}
		if budgetCtx.Err() != nil {
			break
		}
	}
	fallbackSpan.End(lastErr)
	if lastErr != nil {
		observeFallbackResultByEntity(fallbackEntityProfile, fallbackResultError, time.Since(started))
		logFallbackInfraFailure(ctx, "profile_by_pubkey", fallbackEntityProfile, normalized, lastErr, true)
		return Profile{}, err
	}
	observeFallbackResultByEntity(fallbackEntityProfile, fallbackResultMiss, time.Since(started))
	return Profile{}, err
}

func (s profileService) GetProfiles(ctx context.Context, pubkeys []string) (UserInfosResult, error) {
	normalized := normalizeUniqueStrings(pubkeys)
	if len(normalized) == 0 {
		return UserInfosResult{}, fmt.Errorf("pubkeys must include at least one non-empty value")
	}
	profilesByPubkey, err := s.reader.GetProfilesByPubkeys(ctx, normalized)
	if err != nil {
		return UserInfosResult{}, err
	}
	missing := make([]string, 0)
	for _, pubkey := range normalized {
		if _, ok := profilesByPubkey[pubkey]; ok {
			continue
		}
		missing = append(missing, pubkey)
	}
	if len(missing) == 0 {
		metrics.ObserveLookupLocal("profile_batch", true)
	} else {
		metrics.ObserveLookupLocal("profile_batch", false)
	}
	if len(missing) > 0 && s.fallback != nil {
		allowedPubkeys, allTrusted := s.policy.admitProfiles(ctx, missing, fallbackLookupDirect)
		if len(allowedPubkeys) == 0 {
			goto buildResult
		}
		maxAttempts, maxTimeBudget := s.policy.executionBounds(!allTrusted)
		started := time.Now()
		fallbackCtx, fallbackSpan := traceutil.StartSpan(ctx, "query.get_profile_batch.fallback", traceutil.KV("fallback.surface", "profile_batch"))
		budgetCtx, cancel := withFallbackTimeBudget(fallbackCtx, maxTimeBudget)
		defer cancel()

		remaining := append([]string(nil), allowedPubkeys...)
		fallbackProfiles := make(map[string]Profile, len(allowedPubkeys))
		var lastErr error
		for attempt := 0; attempt < maxAttempts && len(remaining) > 0; attempt++ {
			observeFallbackAttemptByEntity(fallbackEntityProfile)
			attemptProfiles, fallbackErr := s.fallback.FetchProfilesByPubkeys(budgetCtx, remaining)
			if fallbackErr != nil {
				lastErr = fallbackErr
				if budgetCtx.Err() != nil {
					break
				}
				continue
			}
			nextRemaining := make([]string, 0, len(remaining))
			for _, pubkey := range remaining {
				profile, ok := attemptProfiles[pubkey]
				if !ok {
					nextRemaining = append(nextRemaining, pubkey)
					continue
				}
				fallbackProfiles[pubkey] = profile
			}
			remaining = nextRemaining
			if budgetCtx.Err() != nil {
				break
			}
		}

		fallbackSpan.End(lastErr)
		if lastErr != nil {
			observeFallbackResultByEntity(fallbackEntityProfile, fallbackResultError, time.Since(started))
			logFallbackBatchInfraFailure(ctx, "profile_batch", fallbackEntityProfile, missing, lastErr, true)
		} else if len(fallbackProfiles) == 0 {
			observeFallbackResultByEntity(fallbackEntityProfile, fallbackResultMiss, time.Since(started))
		} else {
			recovered := 0
			for _, pubkey := range missing {
				if _, ok := fallbackProfiles[pubkey]; ok {
					recovered++
				}
			}
			if recovered == 0 {
				observeFallbackResultByEntity(fallbackEntityProfile, fallbackResultMiss, time.Since(started))
			} else {
				observeFallbackResultByEntity(fallbackEntityProfile, fallbackResultHit, time.Since(started))
			}
			for pubkey, profile := range fallbackProfiles {
				profilesByPubkey[pubkey] = profile
			}
		}
	}
buildResult:
	out := UserInfosResult{
		Profiles:       make([]Profile, 0, len(profilesByPubkey)),
		MissingPubkeys: make([]string, 0),
	}
	for _, pubkey := range normalized {
		profile, ok := profilesByPubkey[pubkey]
		if !ok {
			out.MissingPubkeys = append(out.MissingPubkeys, pubkey)
			continue
		}
		out.Profiles = append(out.Profiles, profile)
	}
	return out, nil
}

func (s profileService) GetProfilePublicSummary(ctx context.Context, pubkey string) (ProfilePublicSummary, error) {
	profile, err := s.GetProfile(ctx, pubkey)
	if err != nil {
		return ProfilePublicSummary{}, err
	}
	stats, err := s.reader.GetProfilePublicStatsByPubkey(ctx, profile.Pubkey)
	if err != nil {
		return ProfilePublicSummary{}, err
	}
	return ProfilePublicSummary{
		Profile: profile,
		Stats:   stats,
	}, nil
}

func (s Service) GetProfile(ctx context.Context, pubkey string) (out Profile, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.get_profile")
	defer func() { span.End(err) }()
	return profileService{reader: s.reader, fallback: s.fallback, policy: s.fallbackPolicy()}.GetProfile(ctx, pubkey)
}

func (s Service) GetProfilePublicSummary(ctx context.Context, pubkey string) (out ProfilePublicSummary, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.get_profile_public_summary")
	defer func() { span.End(err) }()
	return profileService{reader: s.reader, fallback: s.fallback, policy: s.fallbackPolicy()}.GetProfilePublicSummary(ctx, pubkey)
}

func (s Service) GetContactList(ctx context.Context, pubkey string) (ContactList, error) {
	return s.reader.GetContactListByPubkey(ctx, pubkey)
}

func (s Service) GetRelayList(ctx context.Context, pubkey string) (RelayList, error) {
	return s.reader.GetRelayListByPubkey(ctx, pubkey)
}

func (s Service) IsUserFollowing(ctx context.Context, followerPubkey string, followedPubkey string) (bool, error) {
	if r := s.capabilities.social.userFollowing; r != nil {
		return r.IsUserFollowing(ctx, followerPubkey, followedPubkey)
	}
	return false, unsupportedCapabilityError("is user following")
}

func (s Service) GetMutualFollows(ctx context.Context, leftPubkey string, rightPubkey string, limit int) ([]string, error) {
	if r := s.capabilities.social.mutualFollows; r != nil {
		return r.GetMutualFollows(ctx, leftPubkey, rightPubkey, limit)
	}
	return nil, unsupportedCapabilityError("mutual follows")
}
