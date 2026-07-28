package derivation

import "testing"

func TestMaxAuthorAnalyticsWindowDays(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   []int
		want int
	}{
		{name: "default list", in: []int{7, 30}, want: 30},
		{name: "includes 90", in: []int{7, 30, 90}, want: 90},
		{name: "empty falls back", in: nil, want: 30},
		{name: "single", in: []int{7}, want: 7},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := maxAuthorAnalyticsWindowDays(tc.in); got != tc.want {
				t.Fatalf("maxAuthorAnalyticsWindowDays(%v)=%d, want %d", tc.in, got, tc.want)
			}
		})
	}
}
