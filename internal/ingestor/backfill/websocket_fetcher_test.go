package backfill

import "testing"

func TestParseNostrEnvelope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		frame       string
		wantType    string
		wantSubID   string
		wantOK      bool
		wantPayload string
	}{
		{
			name:        "event envelope",
			frame:       `["EVENT","sub-1",{"id":"x","created_at":12}]`,
			wantType:    "EVENT",
			wantSubID:   "sub-1",
			wantOK:      true,
			wantPayload: `{"id":"x","created_at":12}`,
		},
		{
			name:      "eose envelope",
			frame:     `["EOSE","sub-1"]`,
			wantType:  "EOSE",
			wantSubID: "sub-1",
			wantOK:    true,
		},
		{
			name:   "notice ignored",
			frame:  `["NOTICE","oops"]`,
			wantOK: false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotType, gotSubID, gotPayload, ok := parseNostrEnvelope([]byte(tc.frame))
			if ok != tc.wantOK {
				t.Fatalf("ok mismatch: got %v want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if gotType != tc.wantType {
				t.Fatalf("type mismatch: got %q want %q", gotType, tc.wantType)
			}
			if gotSubID != tc.wantSubID {
				t.Fatalf("sub id mismatch: got %q want %q", gotSubID, tc.wantSubID)
			}
			if string(gotPayload) != tc.wantPayload {
				t.Fatalf("payload mismatch: got %q want %q", string(gotPayload), tc.wantPayload)
			}
		})
	}
}
