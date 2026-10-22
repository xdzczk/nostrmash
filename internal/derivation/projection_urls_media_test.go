package derivation

import "testing"

func TestMediaURLPattern(t *testing.T) {
	t.Parallel()
	cases := []struct {
		url   string
		media bool
	}{
		{"https://cdn.example.com/a.png", true},
		{"https://cdn.example.com/a.JPEG?width=10", true},
		{"https://cdn.example.com/a.mp4#t=1", true},
		{"https://example.com/post", false},
		{"https://example.com/png-guide", false},
		{"https://youtu.be/dQw4w9WgXcQ", false},
		{"https://example.com/photo.png/extra", false},
	}
	for _, tc := range cases {
		if got := mediaURLPattern.MatchString(tc.url); got != tc.media {
			t.Fatalf("mediaURLPattern.MatchString(%q) = %v, want %v", tc.url, got, tc.media)
		}
	}
}
