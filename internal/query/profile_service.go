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
}

// NewProfileService constructs a profile-only orchestration service from a narrow dependency.
func NewProfileService(reader ProfileReader) ProfileService {
	return profileService{reader: reader}
}

func (s Service) GetUserInfos(ctx context.Context, pubkeys []string) (UserInfosResult, error) {
	return profileService{reader: s.reader, fallback: s.fallback}.GetProfiles(ctx, pubkeys)
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
	started := time.Now()
	observeFallbackAttemptByEntity(fallbackEntityProfile)
	fallbackCtx, fallbackSpan := traceutil.StartSpan(ctx, "query.get_profile.fallback", traceutil.KV("fallback.surface", "profile_by_pubkey"))
	foundByPubkey, fallbackErr := s.fallback.FetchProfilesByPubkeys(fallbackCtx, []string{normalized})
	fallbackSpan.End(fallbackErr)
	if fallbackErr != nil {
		observeFallbackResultByEntity(fallbackEntityProfile, fallbackResultError, time.Since(started))
		logFallbackInfraFailure(ctx, "profile_by_pubkey", fallbackEntityProfile, normalized, fallbackErr, true)
		return Profile{}, err
	}
	profile, ok := foundByPubkey[normalized]
	if !ok {
		observeFallbackResultByEntity(fallbackEntityProfile, fallbackResultMiss, time.Since(started))
		return Profile{}, err
	}
	observeFallbackResultByEntity(fallbackEntityProfile, fallbackResultHit, time.Since(started))
	return profile, nil
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
		started := time.Now()
		observeFallbackAttemptByEntity(fallbackEntityProfile)
		fallbackCtx, fallbackSpan := traceutil.StartSpan(ctx, "query.get_profile_batch.fallback", traceutil.KV("fallback.surface", "profile_batch"))
		fallbackProfiles, fallbackErr := s.fallback.FetchProfilesByPubkeys(fallbackCtx, missing)
		fallbackSpan.End(fallbackErr)
		if fallbackErr != nil {
			observeFallbackResultByEntity(fallbackEntityProfile, fallbackResultError, time.Since(started))
			logFallbackBatchInfraFailure(ctx, "profile_batch", fallbackEntityProfile, missing, fallbackErr, true)
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

func (s Service) GetProfile(ctx context.Context, pubkey string) (out Profile, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.get_profile")
	defer func() { span.End(err) }()
	return profileService{reader: s.reader, fallback: s.fallback}.GetProfile(ctx, pubkey)
}

func (s Service) GetContactList(ctx context.Context, pubkey string) (ContactList, error) {
	return s.reader.GetContactListByPubkey(ctx, pubkey)
}

func (s Service) GetRelayList(ctx context.Context, pubkey string) (RelayList, error) {
	return s.reader.GetRelayListByPubkey(ctx, pubkey)
}

func (s Service) IsUserFollowing(ctx context.Context, followerPubkey string, followedPubkey string) (bool, error) {
	type followingReader interface {
		IsUserFollowing(ctx context.Context, followerPubkey string, followedPubkey string) (bool, error)
	}
	if r, ok := s.rawReader.(followingReader); ok {
		return r.IsUserFollowing(ctx, followerPubkey, followedPubkey)
	}
	return false, nil
}

func (s Service) GetMutualFollows(ctx context.Context, leftPubkey string, rightPubkey string, limit int) ([]string, error) {
	type mutualReader interface {
		GetMutualFollows(ctx context.Context, leftPubkey string, rightPubkey string, limit int) ([]string, error)
	}
	if r, ok := s.rawReader.(mutualReader); ok {
		return r.GetMutualFollows(ctx, leftPubkey, rightPubkey, limit)
	}
	return []string{}, nil
}
