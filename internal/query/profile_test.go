package query

import (
	"context"
	"testing"

	"github.com/xdzczk/nostrmash/internal/store"
)

func TestGetProfilesNormalizesInputOrder(t *testing.T) {
	t.Parallel()
	svc := NewProfileService(fakeProfileReader{
		getProfilesByPubkeysFn: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			if len(pubkeys) != 2 || pubkeys[0] != "pk1" || pubkeys[1] != "pk2" {
				t.Fatalf("unexpected normalized pubkeys: %#v", pubkeys)
			}
			return map[string]store.ProfileProjection{
				"pk1": {Pubkey: "pk1"},
			}, nil
		},
	})

	out, err := svc.GetProfiles(context.Background(), []string{" pk1 ", "", "pk2", "pk1"})
	if err != nil {
		t.Fatalf("GetProfiles returned error: %v", err)
	}
	if len(out.Profiles) != 1 || out.Profiles[0].Pubkey != "pk1" {
		t.Fatalf("unexpected profiles result: %#v", out.Profiles)
	}
	if len(out.MissingPubkeys) != 1 || out.MissingPubkeys[0] != "pk2" {
		t.Fatalf("unexpected missing pubkeys: %#v", out.MissingPubkeys)
	}
}
