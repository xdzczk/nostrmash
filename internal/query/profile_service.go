package query

import (
	"context"
	"fmt"

	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/store/traceutil"
)

type profileService struct {
	reader ProfileReader
}

// NewProfileService constructs a profile-only orchestration service from a narrow dependency.
func NewProfileService(reader ProfileReader) ProfileService {
	return profileService{reader: reader}
}

func (s Service) GetUserInfos(ctx context.Context, pubkeys []string) (UserInfosResult, error) {
	normalized := normalizeUniqueStrings(pubkeys)
	if len(normalized) == 0 {
		return UserInfosResult{}, fmt.Errorf("pubkeys must include at least one non-empty value")
	}
	profilesByPubkey, err := s.reader.GetProfilesByPubkeys(ctx, normalized)
	if err != nil {
		return UserInfosResult{}, err
	}
	out := UserInfosResult{
		Profiles:       make([]store.ProfileProjection, 0, len(profilesByPubkey)),
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

func (s Service) GetProfiles(ctx context.Context, pubkeys []string) (UserInfosResult, error) {
	return s.GetUserInfos(ctx, pubkeys)
}

func (s profileService) GetProfile(ctx context.Context, pubkey string) (store.ProfileProjection, error) {
	return s.reader.GetProfileByPubkey(ctx, pubkey)
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
	out := UserInfosResult{
		Profiles:       make([]store.ProfileProjection, 0, len(profilesByPubkey)),
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

func (s Service) GetProfile(ctx context.Context, pubkey string) (out store.ProfileProjection, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.get_profile")
	defer func() { span.End(err) }()
	return s.reader.GetProfileByPubkey(ctx, pubkey)
}

func (s Service) GetContactList(ctx context.Context, pubkey string) (store.ContactListProjection, error) {
	return s.reader.GetContactListByPubkey(ctx, pubkey)
}

func (s Service) GetRelayList(ctx context.Context, pubkey string) (store.RelayListProjection, error) {
	return s.reader.GetRelayListByPubkey(ctx, pubkey)
}

func (s Service) IsUserFollowing(ctx context.Context, followerPubkey string, followedPubkey string) (bool, error) {
	type followingReader interface {
		IsUserFollowing(ctx context.Context, followerPubkey string, followedPubkey string) (bool, error)
	}
	if r, ok := s.reader.(followingReader); ok {
		return r.IsUserFollowing(ctx, followerPubkey, followedPubkey)
	}
	return false, nil
}

func (s Service) GetMutualFollows(ctx context.Context, leftPubkey string, rightPubkey string, limit int) ([]string, error) {
	type mutualReader interface {
		GetMutualFollows(ctx context.Context, leftPubkey string, rightPubkey string, limit int) ([]string, error)
	}
	if r, ok := s.reader.(mutualReader); ok {
		return r.GetMutualFollows(ctx, leftPubkey, rightPubkey, limit)
	}
	return []string{}, nil
}
