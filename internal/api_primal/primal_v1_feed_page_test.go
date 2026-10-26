package api_primal

import "testing"

func TestTrendingHoursReadsSpecWindow(t *testing.T) {
	cases := []struct {
		spec map[string]any
		want int
	}{
		{map[string]any{"hours": float64(4)}, 4},
		{map[string]any{"since": float64(12)}, 12},
		{map[string]any{"timeframe": "trending_7d"}, 168},
		{map[string]any{"id": "global-trending"}, 24},
		{map[string]any{"since": float64(1_700_000_000)}, 24},
	}
	for _, tc := range cases {
		if got := trendingHours(tc.spec); got != tc.want {
			t.Fatalf("spec %#v hours=%d want %d", tc.spec, got, tc.want)
		}
	}
}

func TestCanonicalNotificationArgsMatchIOS(t *testing.T) {
	body := map[string]any{"event_id": "note", "user": "actor", "amount_sats": float64(21)}
	canonicalNotificationArgs(3, body)
	if body["your_post"] != "note" || body["who_zapped_it"] != "actor" || body["satszapped"] != float64(21) {
		t.Fatalf("zap %#v", body)
	}
	liked := map[string]any{"event_id": "note", "user": "actor"}
	canonicalNotificationArgs(4, liked)
	if liked["your_post"] != "note" || liked["who_liked_it"] != "actor" {
		t.Fatalf("like %#v", liked)
	}
	mention := map[string]any{"event_id": "note"}
	canonicalNotificationArgs(7, mention)
	if mention["you_were_mentioned_in"] != "note" {
		t.Fatalf("mention %#v", mention)
	}
}
