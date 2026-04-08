package query

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

func BenchmarkServiceGetUserInfos(b *testing.B) {
	pubkeys := make([]string, 0, 240)
	for i := 0; i < 200; i++ {
		pubkeys = append(pubkeys, fmt.Sprintf("pk_%03d", i))
	}
	pubkeys = append(pubkeys, "pk_010", "pk_010", "", "   ", "pk_199")

	profiles := make(map[string]Profile, 200)
	for i := 0; i < 160; i++ {
		key := fmt.Sprintf("pk_%03d", i)
		profiles[key] = Profile{
			Pubkey:            key,
			MetadataEventID:   fmt.Sprintf("meta_%03d", i),
			MetadataCreatedAt: int64(1700000000 + i),
			ProfileJSON:       json.RawMessage(`{"name":"bench"}`),
		}
	}

	svc := NewProfileService(fakeProfileReader{
		getProfilesByPubkeysFn: func(_ context.Context, keys []string) (map[string]Profile, error) {
			out := make(map[string]Profile, len(keys))
			for _, key := range keys {
				if profile, ok := profiles[key]; ok {
					out[key] = profile
				}
			}
			return out, nil
		},
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := svc.GetProfiles(context.Background(), pubkeys)
		if err != nil {
			b.Fatalf("GetUserInfos: %v", err)
		}
		if len(out.Profiles) == 0 {
			b.Fatal("expected non-empty profile result")
		}
	}
}
