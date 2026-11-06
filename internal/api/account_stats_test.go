package api

import "testing"

func TestAccountAgeBucketsFillsEveryIndex(t *testing.T) {
	got := accountAgeBuckets(map[int]int64{0: 3, 5: 9, 7: 100})
	if len(got) != 6 {
		t.Fatalf("len = %d, want 6", len(got))
	}
	want := []int64{3, 0, 0, 0, 0, 9}
	for i, authors := range want {
		if got[i]["bucket"] != i || got[i]["authors"] != authors {
			t.Fatalf("bucket %d = %#v, want bucket %d authors %d", i, got[i], i, authors)
		}
	}
}
