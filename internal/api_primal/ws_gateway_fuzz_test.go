package api_primal

import (
	"encoding/json"
	"testing"
)

func FuzzDecodeFrame(f *testing.F) {
	f.Add([]byte(`["REQ","sub-1",{"ids":["evt_1"]}]`))
	f.Add([]byte(`{"not":"an array frame"}`))
	f.Add([]byte(``))
	f.Add([]byte(`["REQ",1,{"cache":["thread_view",{"event_id":"evt_1"}]}]`))

	f.Fuzz(func(t *testing.T, payload []byte) {
		_, _ = decodeFrame(payload)
	})
}

func FuzzParseParameterizedReplaceableRefs(f *testing.F) {
	f.Add([]byte(`[{"pubkey":"abc","kind":30023,"identifier":"x"}]`))
	f.Add([]byte(`[{"pubkey":"abc","kind":30023,"d_tag":"x"}]`))
	f.Add([]byte(`[{"pubkey":"","kind":0}]`))
	f.Add([]byte(`{"not":"an array"}`))

	f.Fuzz(func(t *testing.T, raw []byte) {
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return
		}
		refs, err := parseParameterizedReplaceableRefs(value)
		if err != nil {
			return
		}
		for _, ref := range refs {
			if ref.pubkey == "" || ref.kind <= 0 {
				t.Fatalf("invalid parsed ref: %#v", ref)
			}
		}
	})
}

func FuzzParseAndValidateDMResetAuth(f *testing.F) {
	f.Add([]byte(`{"event_from_user":{"id":"bad"},"peer_pubkey":"abc"}`))
	f.Add([]byte(`{"event_from_user":"oops","sender":"abc"}`))
	f.Add([]byte(`{"sender":"abc"}`))
	f.Add([]byte(`{}`))

	f.Fuzz(func(t *testing.T, raw []byte) {
		var kwargs map[string]any
		if err := json.Unmarshal(raw, &kwargs); err != nil {
			return
		}

		receiver, sender, err := parseAndValidateDMResetAuth(kwargs)
		if err == nil {
			if err := validatePubkeyHex(receiver); err != nil {
				t.Fatalf("receiver must be a valid pubkey when parse succeeds: %v", err)
			}
			if err := validatePubkeyHex(sender); err != nil {
				t.Fatalf("sender must be a valid pubkey when parse succeeds: %v", err)
			}
		}

		receiverOnly, err := parseAndValidateDMResetAllAuth(kwargs)
		if err == nil {
			if err := validatePubkeyHex(receiverOnly); err != nil {
				t.Fatalf("receiver must be a valid pubkey when parse succeeds: %v", err)
			}
		}
	})
}
