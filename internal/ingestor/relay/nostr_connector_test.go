package relay

import "testing"

func TestExtractEventPayload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		frame      string
		wantOK     bool
		wantResult string
	}{
		{
			name:       "valid event envelope",
			frame:      `["EVENT","sub-1",{"id":"abc","kind":1}]`,
			wantOK:     true,
			wantResult: `{"id":"abc","kind":1}`,
		},
		{
			name:       "event envelope with missing payload still forwarded",
			frame:      `["EVENT","sub-1"]`,
			wantOK:     true,
			wantResult: `["EVENT","sub-1"]`,
		},
		{
			name:       "notice envelope ignored",
			frame:      `["NOTICE","hello"]`,
			wantOK:     false,
			wantResult: "",
		},
		{
			name:       "invalid json ignored",
			frame:      `[not-json`,
			wantOK:     false,
			wantResult: "",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := extractEventPayload([]byte(tc.frame))
			if ok != tc.wantOK {
				t.Fatalf("ok mismatch: got %v want %v", ok, tc.wantOK)
			}
			if string(got) != tc.wantResult {
				t.Fatalf("payload mismatch: got %q want %q", string(got), tc.wantResult)
			}
		})
	}
}
