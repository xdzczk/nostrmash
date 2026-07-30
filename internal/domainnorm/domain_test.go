package domainnorm

import "testing"

func TestCanonicalizeDiscoveryDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "normalizes URL input", input: " HTTPS://Example.COM./path ", want: "example.com"},
		{name: "strips leading www", input: "www.example.com", want: "example.com"},
		{name: "maps product alias", input: "youtu.be", want: "youtube.com"},
		{name: "maps www product alias", input: "https://www.youtu.be/watch", want: "youtube.com"},
		{name: "preserves ordinary subdomain", input: "news.example.com", want: "news.example.com"},
		{name: "does not merge registrable domain", input: "one.example.co.uk", want: "one.example.co.uk"},
		{name: "rejects invalid domain", input: "-bad.example", want: ""},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := CanonicalizeDiscoveryDomain(tt.input); got != tt.want {
				t.Fatalf("CanonicalizeDiscoveryDomain(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
