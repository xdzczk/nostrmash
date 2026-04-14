package relayurl

import (
	"testing"
)

func TestNormalize(t *testing.T) {
	tests := []struct {
		name       string
		raw        string
		requireTLS bool
		want       string
		wantErr    bool
	}{
		{name: "basic wss", raw: "wss://relay.example.com", want: "wss://relay.example.com"},
		{name: "trailing slash stripped", raw: "wss://relay.example.com/", want: "wss://relay.example.com"},
		{name: "path preserved", raw: "wss://relay.example.com/nostr", want: "wss://relay.example.com/nostr"},
		{name: "host lowered", raw: "wss://Relay.Example.COM", want: "wss://relay.example.com"},
		{name: "scheme lowered", raw: "WSS://relay.example.com", want: "wss://relay.example.com"},
		{name: "ws allowed when tls not required", raw: "ws://relay.example.com", want: "ws://relay.example.com"},
		{name: "ws rejected when tls required", raw: "ws://relay.example.com", requireTLS: true, wantErr: true},
		{name: "whitespace trimmed", raw: "  wss://relay.example.com  ", want: "wss://relay.example.com"},
		{name: "invalid scheme", raw: "http://relay.example.com", wantErr: true},
		{name: "no host", raw: "wss://", wantErr: true},
		{name: "fragment rejected", raw: "wss://relay.example.com#frag", wantErr: true},
		{name: "query rejected", raw: "wss://relay.example.com?k=v", wantErr: true},
		{name: "userinfo rejected", raw: "wss://user@relay.example.com", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Normalize(tt.raw, NormalizeOptions{RequireTLS: tt.requireTLS})
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNormalize_DedupeEquivalence(t *testing.T) {
	variants := []string{
		"wss://relay.example.com",
		"wss://relay.example.com/",
		"wss://Relay.Example.COM",
		"  wss://relay.example.com  ",
		"WSS://relay.example.com",
	}
	opts := NormalizeOptions{}
	first, err := Normalize(variants[0], opts)
	if err != nil {
		t.Fatalf("normalize %q: %v", variants[0], err)
	}
	for _, v := range variants[1:] {
		got, err := Normalize(v, opts)
		if err != nil {
			t.Fatalf("normalize %q: %v", v, err)
		}
		if got != first {
			t.Fatalf("variant %q normalized to %q, want %q", v, got, first)
		}
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name         string
		url          string
		allowPrivate bool
		wantErr      bool
	}{
		{name: "valid public relay", url: "wss://relay.example.com"},
		{name: "localhost rejected", url: "wss://localhost", wantErr: true},
		{name: "localhost allowed when private ok", url: "wss://localhost", allowPrivate: true},
		{name: "loopback rejected", url: "wss://127.0.0.1", wantErr: true},
		{name: "loopback allowed when private ok", url: "wss://127.0.0.1", allowPrivate: true},
		{name: "private ip rejected", url: "wss://192.168.1.1", wantErr: true},
		{name: "private ip allowed when private ok", url: "wss://192.168.1.1", allowPrivate: true},
		{name: "10.x rejected", url: "wss://10.0.0.1", wantErr: true},
		{name: "empty url rejected", url: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(tt.url, ValidateOptions{AllowPrivateNetwork: tt.allowPrivate})
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestCanonicalKey(t *testing.T) {
	a := CanonicalKey("wss://relay.example.com")
	b := CanonicalKey("wss://relay.example.com")
	if a != b {
		t.Fatalf("same input produced different keys: %q vs %q", a, b)
	}
	if len(a) != 32 {
		t.Fatalf("expected 32-char hex key, got %d chars", len(a))
	}
	c := CanonicalKey("wss://other.example.com")
	if a == c {
		t.Fatal("different inputs produced same key")
	}
}
