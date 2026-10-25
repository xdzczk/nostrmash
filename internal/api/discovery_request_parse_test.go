package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestParseOptionalDiscoveryLanguage(t *testing.T) {
	t.Parallel()
	cases := []struct {
		raw  string
		want string
		ok   bool
	}{
		{raw: "", want: "", ok: true},
		{raw: "DE", want: "de", ok: true},
		{raw: "ja", want: "ja", ok: true},
		{raw: "und", want: "und", ok: true},
		{raw: "UND-UNCERTAIN", want: "und-uncertain", ok: true},
		{raw: "english", want: "english", ok: true},
		{raw: "e", ok: false},
		{raw: "de-DE", ok: false},
		{raw: "123", ok: false},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/notes/trending?lang="+tc.raw, nil)
		got, err := parseOptionalDiscoveryLanguage(req)
		if tc.ok {
			if err != nil || got != tc.want {
				t.Fatalf("lang %q = %q %v, want %q", tc.raw, got, err, tc.want)
			}
			continue
		}
		if err == nil {
			t.Fatalf("lang %q should be rejected", tc.raw)
		}
	}
}
