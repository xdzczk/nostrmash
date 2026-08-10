package query

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

type trustQualificationReader struct {
	fakeReader
	getTrustStateFn          func(context.Context, string) (readmodel.TrustState, error)
	getTrustStatesFn         func(context.Context, []string) (map[string]readmodel.TrustState, error)
	getTrustQualificationsFn func(context.Context, []string, readmodel.TrustQualificationPolicy) (map[string]readmodel.TrustQualification, error)
	isTrustedAuthorFn        func(context.Context, string, readmodel.TrustQualificationPolicy) (bool, error)
	countRankedPubkeysFn     func(context.Context) (int64, error)
}

func (r trustQualificationReader) GetTrustState(ctx context.Context, pubkey string) (readmodel.TrustState, error) {
	if r.getTrustStateFn == nil {
		return readmodel.TrustState{}, unsupportedCapabilityError("trust state")
	}
	return r.getTrustStateFn(ctx, pubkey)
}

func (r trustQualificationReader) GetTrustStates(ctx context.Context, pubkeys []string) (map[string]readmodel.TrustState, error) {
	if r.getTrustStatesFn == nil {
		return nil, unsupportedCapabilityError("trust state")
	}
	return r.getTrustStatesFn(ctx, pubkeys)
}

func (r trustQualificationReader) GetTrustQualifications(
	ctx context.Context,
	pubkeys []string,
	policy readmodel.TrustQualificationPolicy,
) (map[string]readmodel.TrustQualification, error) {
	if r.getTrustQualificationsFn == nil {
		return map[string]readmodel.TrustQualification{}, nil
	}
	return r.getTrustQualificationsFn(ctx, pubkeys, policy)
}

func (r trustQualificationReader) IsTrustedAuthor(
	ctx context.Context,
	pubkey string,
	policy readmodel.TrustQualificationPolicy,
) (bool, error) {
	if r.isTrustedAuthorFn == nil {
		return false, nil
	}
	return r.isTrustedAuthorFn(ctx, pubkey, policy)
}

func (r trustQualificationReader) CountRankedPubkeys(ctx context.Context) (int64, error) {
	if r.countRankedPubkeysFn == nil {
		return 0, nil
	}
	return r.countRankedPubkeysFn(ctx)
}

func TestGetTrustSummary_MapsTierHopAndPercentile(t *testing.T) {
	t.Parallel()
	hops := 2
	rank := int64(10)
	svc := mustNewService(t, trustQualificationReader{
		fakeReader: fakeReader{},
		getTrustStateFn: func(_ context.Context, pubkey string) (readmodel.TrustState, error) {
			if pubkey != "alice" {
				return readmodel.TrustState{}, readmodel.ErrNotFound
			}
			return readmodel.TrustState{
				Pubkey:      pubkey,
				Qualified:   true,
				HopDistance: &hops,
				Rank:        &rank,
			}, nil
		},
		countRankedPubkeysFn: func(context.Context) (int64, error) {
			return 100, nil
		},
	})

	summary, err := svc.GetTrustSummary(context.Background(), "alice")
	if err != nil {
		t.Fatalf("GetTrustSummary: %v", err)
	}
	if summary.Tier != "in_network" || summary.HopDistance == nil || *summary.HopDistance != 2 {
		t.Fatalf("unexpected summary: %#v", summary)
	}
	if summary.Percentile == nil || *summary.Percentile != 10.0 {
		t.Fatalf("expected percentile 10.0, got %#v", summary.Percentile)
	}

	missing, err := svc.GetTrustSummary(context.Background(), "missing")
	if err != nil {
		t.Fatalf("GetTrustSummary missing: %v", err)
	}
	if missing.Tier != "unranked" || missing.Percentile != nil {
		t.Fatalf("expected unranked summary for missing pubkey, got %#v", missing)
	}
}

func TestTrustQualificationService_BatchAndUnknownPubkeys(t *testing.T) {
	t.Parallel()
	svc := mustNewService(t, trustQualificationReader{
		fakeReader: fakeReader{},
		getTrustQualificationsFn: func(_ context.Context, pubkeys []string, policy readmodel.TrustQualificationPolicy) (map[string]readmodel.TrustQualification, error) {
			if policy.MaxHops != 2 {
				t.Fatalf("expected policy max hops 2, got %d", policy.MaxHops)
			}
			out := make(map[string]readmodel.TrustQualification, len(pubkeys))
			for _, pubkey := range pubkeys {
				switch pubkey {
				case "trusted":
					hops := 1
					out[pubkey] = readmodel.TrustQualification{Pubkey: pubkey, Trusted: true, DistanceHops: &hops}
				case "unknown":
					// Deliberately omitted to exercise query-service defaulting for missing keys.
				default:
					out[pubkey] = readmodel.TrustQualification{Pubkey: pubkey}
				}
			}
			return out, nil
		},
	})

	rows, err := svc.GetTrustQualification(context.Background(), []string{"trusted", "unknown", "trusted"}, TrustQualificationPolicy{
		MaxHops: 2,
	})
	if err != nil {
		t.Fatalf("GetTrustQualification returned error: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected deduplicated response size 2, got %d", len(rows))
	}
	if !rows["trusted"].Trusted {
		t.Fatalf("expected trusted pubkey to be trusted")
	}
	if rows["unknown"].Trusted {
		t.Fatalf("expected unknown pubkey to be untrusted")
	}
	if rows["unknown"].Pubkey != "unknown" {
		t.Fatalf("expected unknown pubkey placeholder row, got %+v", rows["unknown"])
	}
}

func TestTrustStateService_BatchLookupAndPolicyQualification(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	hops1 := 1
	hops2 := 2
	scoreHigh := 0.8
	scoreLow := 0.2
	rank1 := int64(1)
	rank2 := int64(2)
	gen := int64(44)
	svc := mustNewService(t, trustQualificationReader{
		fakeReader: fakeReader{},
		getTrustStatesFn: func(_ context.Context, pubkeys []string) (map[string]readmodel.TrustState, error) {
			out := make(map[string]readmodel.TrustState, len(pubkeys))
			for _, pubkey := range pubkeys {
				switch pubkey {
				case "trusted":
					out[pubkey] = readmodel.TrustState{
						Pubkey:       pubkey,
						Score:        &scoreHigh,
						Qualified:    true,
						Tier:         "core",
						HopDistance:  &hops1,
						HopBucket:    "1",
						Rank:         &rank1,
						ComputedAt:   &now,
						GenerationID: &gen,
					}
				case "low_score":
					out[pubkey] = readmodel.TrustState{
						Pubkey:       pubkey,
						Score:        &scoreLow,
						Qualified:    true,
						Tier:         "near",
						HopDistance:  &hops2,
						HopBucket:    "2",
						Rank:         &rank2,
						ComputedAt:   &now,
						GenerationID: &gen,
					}
				}
			}
			return out, nil
		},
	})

	states, err := svc.GetTrustStates(context.Background(), []string{"trusted", "unknown", "trusted"})
	if err != nil {
		t.Fatalf("GetTrustStates returned error: %v", err)
	}
	if len(states) != 2 {
		t.Fatalf("expected deduplicated trust state map, got %d entries", len(states))
	}
	if !states["trusted"].Qualified || states["trusted"].Tier != "core" || states["trusted"].HopBucket != "1" {
		t.Fatalf("unexpected trusted state: %#v", states["trusted"])
	}
	if states["unknown"].Qualified || states["unknown"].Tier != "unknown" || states["unknown"].HopBucket != "unknown" {
		t.Fatalf("unexpected unknown state defaulting: %#v", states["unknown"])
	}

	qualified, err := svc.GetTrustQualification(context.Background(), []string{"trusted", "low_score", "unknown"}, TrustQualificationPolicy{
		MaxHops:      2,
		MinimumScore: 0.5,
	})
	if err != nil {
		t.Fatalf("GetTrustQualification returned error: %v", err)
	}
	if !qualified["trusted"].Trusted {
		t.Fatalf("expected trusted pubkey to pass policy")
	}
	if qualified["low_score"].Trusted {
		t.Fatalf("expected low_score pubkey to fail minimum score policy")
	}
	if qualified["unknown"].Trusted {
		t.Fatalf("expected unknown pubkey to be untrusted")
	}
	if qualified["trusted"].SourceRunID == nil || *qualified["trusted"].SourceRunID != gen {
		t.Fatalf("expected generation id to flow into qualification, got %#v", qualified["trusted"])
	}
}

func TestTrustQualificationService_IsTrustedAuthor(t *testing.T) {
	t.Parallel()
	svc := mustNewService(t, trustQualificationReader{
		fakeReader: fakeReader{},
		isTrustedAuthorFn: func(_ context.Context, pubkey string, policy readmodel.TrustQualificationPolicy) (bool, error) {
			if policy.MinimumScore != 0.5 {
				t.Fatalf("expected minimum score 0.5, got %f", policy.MinimumScore)
			}
			return pubkey == "alice", nil
		},
	})

	ok, err := svc.IsTrustedAuthor(context.Background(), "alice", TrustQualificationPolicy{
		MinimumScore: 0.5,
	})
	if err != nil {
		t.Fatalf("IsTrustedAuthor returned error: %v", err)
	}
	if !ok {
		t.Fatalf("expected alice to be trusted")
	}

	if _, err := svc.IsTrustedAuthor(context.Background(), "  ", TrustQualificationPolicy{}); err == nil {
		t.Fatalf("expected validation error for empty pubkey")
	}
}
