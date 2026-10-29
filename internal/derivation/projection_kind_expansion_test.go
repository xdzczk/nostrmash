package derivation

import "testing"

func TestReportTypeFromTag(t *testing.T) {
	t.Parallel()
	cases := []struct {
		tag  []string
		want string
	}{
		{[]string{"e", "evt", "spam"}, "spam"},
		{[]string{"e", "evt", "wss://relay.example", "nudity"}, "nudity"},
		{[]string{"e", "evt", "wss://relay.example"}, "other"},
		{[]string{"p", "pk"}, "other"},
		{[]string{"e", "evt", "not-a-type"}, "other"},
	}
	for _, tc := range cases {
		if got := reportTypeFromTag(tc.tag); got != tc.want {
			t.Fatalf("reportTypeFromTag(%v) = %q, want %q", tc.tag, got, tc.want)
		}
	}
}
