package backfill

import "testing"

func TestBuildFilter_IncludesAuthorsWhenProvided(t *testing.T) {
	since := int64(100)
	until := int64(200)
	filter := buildFilter(PageRequest{
		Kinds:   []int{0, 3, 10002},
		Authors: []string{"pubkey-a"},
		Since:   &since,
		Until:   &until,
		Limit:   50,
	})

	authors, ok := filter["authors"].([]string)
	if !ok || len(authors) != 1 || authors[0] != "pubkey-a" {
		t.Fatalf("unexpected authors in filter: %#v", filter["authors"])
	}
	if _, ok := filter["since"].(int64); !ok {
		t.Fatalf("expected since in filter, got %#v", filter["since"])
	}
	if _, ok := filter["until"].(int64); !ok {
		t.Fatalf("expected until in filter, got %#v", filter["until"])
	}
}

func TestBuildFilter_OmitsAuthorsWhenEmpty(t *testing.T) {
	filter := buildFilter(PageRequest{
		Kinds: []int{1},
		Limit: 10,
	})
	if _, ok := filter["authors"]; ok {
		t.Fatalf("did not expect authors key: %#v", filter)
	}
}

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
