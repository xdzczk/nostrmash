package query

import (
	"context"
	"testing"
)

type fakePersonalizedRanker struct {
	rows []PersonalizedTrustScore
	err  error
}

func (f fakePersonalizedRanker) GetRanking(ctx context.Context, viewerPubkey string, limit int) ([]PersonalizedTrustScore, error) {
	if f.err != nil {
		return nil, f.err
	}
	if limit > 0 && len(f.rows) > limit {
		return f.rows[:limit], nil
	}
	return f.rows, nil
}

func TestGetPersonalizedTrustRanking(t *testing.T) {
	svc := mustNewServiceWithOptions(t, fakeReader{}, ServiceOptions{
		PersonalizedTrustRanker: fakePersonalizedRanker{
			rows: []PersonalizedTrustScore{{Pubkey: "a", Rank: 1, Source: "personalized"}},
		},
	})
	rows, err := svc.GetPersonalizedTrustRanking(context.Background(), "viewer", 10)
	if err != nil {
		t.Fatalf("GetPersonalizedTrustRanking: %v", err)
	}
	if len(rows) != 1 || rows[0].Pubkey != "a" {
		t.Fatalf("unexpected rows: %#v", rows)
	}

	svcNoCap := mustNewServiceWithOptions(t, fakeReader{}, ServiceOptions{})
	if _, err := svcNoCap.GetPersonalizedTrustRanking(context.Background(), "viewer", 10); err == nil || !IsUnsupportedCapability(err) {
		t.Fatalf("expected unsupported capability, got %v", err)
	}
}
