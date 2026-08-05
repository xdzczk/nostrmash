package eventtags

import "testing"

func TestShouldPersist_AllowlistAndKindScope(t *testing.T) {
	cases := []struct {
		kind    int
		tagName string
		want    bool
	}{
		{1, "p", true},
		{1, "e", true},
		{1, "r", true},
		{1, "client", false},
		{1, "nonce", false},
		{1, "", false},
		{KindContactList, "p", false},
		{KindContactList, "d", true},
		{KindRelayList, "r", false},
		{KindRelayList, "p", true},
		{7, "p", true},
		{5, "e", true},
		{5, "k", false},
	}
	for _, tc := range cases {
		if got := ShouldPersist(tc.kind, tc.tagName); got != tc.want {
			t.Fatalf("ShouldPersist(%d, %q) = %v, want %v", tc.kind, tc.tagName, got, tc.want)
		}
	}
}

func TestAllowedTagNames_SortedAndNonEmpty(t *testing.T) {
	if len(AllowedTagNames) == 0 {
		t.Fatal("AllowedTagNames must not be empty")
	}
	for i := 1; i < len(AllowedTagNames); i++ {
		if AllowedTagNames[i-1] >= AllowedTagNames[i] {
			t.Fatalf("AllowedTagNames not strictly sorted at %d: %q >= %q", i, AllowedTagNames[i-1], AllowedTagNames[i])
		}
	}
	copy := AllowedTagNamesCopy()
	copy[0] = "mutated"
	if AllowedTagNames[0] == "mutated" {
		t.Fatal("AllowedTagNamesCopy must not alias the package slice")
	}
}
