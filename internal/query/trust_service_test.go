package query

import (
	"context"
	"testing"
)

type trustQualificationReader struct {
	fakeReader
	getTrustQualificationsFn func(context.Context, []string, TrustQualificationPolicy) (map[string]TrustQualification, error)
	isTrustedAuthorFn        func(context.Context, string, TrustQualificationPolicy) (bool, error)
}

func (r trustQualificationReader) GetTrustQualifications(
	ctx context.Context,
	pubkeys []string,
	policy TrustQualificationPolicy,
) (map[string]TrustQualification, error) {
	if r.getTrustQualificationsFn == nil {
		return map[string]TrustQualification{}, nil
	}
	return r.getTrustQualificationsFn(ctx, pubkeys, policy)
}

func (r trustQualificationReader) IsTrustedAuthor(
	ctx context.Context,
	pubkey string,
	policy TrustQualificationPolicy,
) (bool, error) {
	if r.isTrustedAuthorFn == nil {
		return false, nil
	}
	return r.isTrustedAuthorFn(ctx, pubkey, policy)
}

func TestTrustQualificationService_BatchAndUnknownPubkeys(t *testing.T) {
	t.Parallel()
	svc := mustNewService(t, trustQualificationReader{
		fakeReader: fakeReader{},
		getTrustQualificationsFn: func(_ context.Context, pubkeys []string, policy TrustQualificationPolicy) (map[string]TrustQualification, error) {
			if policy.MaxHops != 2 {
				t.Fatalf("expected policy max hops 2, got %d", policy.MaxHops)
			}
			out := make(map[string]TrustQualification, len(pubkeys))
			for _, pubkey := range pubkeys {
				switch pubkey {
				case "trusted":
					hops := 1
					out[pubkey] = TrustQualification{Pubkey: pubkey, Trusted: true, DistanceHops: &hops}
				case "unknown":
					// Deliberately omitted to exercise query-service defaulting for missing keys.
				default:
					out[pubkey] = TrustQualification{Pubkey: pubkey}
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

func TestTrustQualificationService_IsTrustedAuthor(t *testing.T) {
	t.Parallel()
	svc := mustNewService(t, trustQualificationReader{
		fakeReader: fakeReader{},
		isTrustedAuthorFn: func(_ context.Context, pubkey string, policy TrustQualificationPolicy) (bool, error) {
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
