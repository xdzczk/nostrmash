package api_primal

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"
)

// FuzzHandleRequestFilters drives the full WS REQ dispatch surface with
// arbitrary filter payloads. Unlike FuzzDecodeFrame (which stops at framing),
// this reaches requestKindFromFilter + resolveFilter and therefore the whole
// kwargs-parsing path behind every cache verb, the ids/search/range routes, and
// the per-request panic recovery. The reader is a no-op fake, so any panic
// surfaced here is a parsing/dispatch bug rather than a data-layer failure.
func FuzzHandleRequestFilters(f *testing.F) {
	seeds := []string{
		`[{"cache":["thread_view",{"event_id":"evt_1","limit":20}]}]`,
		`[{"cache":["user_infos",{"pubkeys":["abc","def"]}]}]`,
		`[{"cache":["feed",{"pubkey":"abc","since":0,"until":100,"limit":20}]}]`,
		`[{"cache":["events",{"event_ids":["evt_1","evt_2"]}]}]`,
		`[{"cache":["get_directional_messages",{"receiver":"abc","sender":"def","offset":0}]}]`,
		`[{"cache":["net_stats",{}]}]`,
		`[{"cache":["scored",{"selector":"trending","limit":10}]}]`,
		`[{"cache":[]}]`,
		`[{"cache":["unknown_verb",{"weird":true}]}]`,
		`[{"cache":"not-an-array"}]`,
		`[{"ids":["evt_1","evt_2"]}]`,
		`[{"search":"nostr","limit":5}]`,
		`[{"since":0,"until":100}]`,
		`[{"kinds":[1],"authors":["abc"]}]`,
		`[{}]`,
		`[]`,
		`["not-an-object"]`,
		`[123,null,{"cache":["thread_view",{"event_id":123}]}]`,
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}

	gateway := mustNewWSGateway(f, fakeEventReader{}, WSGatewayOptions{
		RequestTimeout: 2 * time.Second,
		Logger:         slog.New(slog.DiscardHandler),
	})

	f.Fuzz(func(t *testing.T, payload []byte) {
		var filters []any
		if err := json.Unmarshal(payload, &filters); err != nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		// Must not panic and must always return a non-nil frame slice regardless
		// of how malformed the filters are.
		frames := gateway.handleRequestFilters(ctx, "fuzz-sub", "127.0.0.1", filters)
		if frames == nil {
			t.Fatal("handleRequestFilters returned nil frames slice")
		}
	})
}
