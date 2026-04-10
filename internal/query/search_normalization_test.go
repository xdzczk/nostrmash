package query

import (
	"encoding/hex"
	"testing"

	"github.com/btcsuite/btcd/btcutil/bech32"
)

func TestNormalizeProfileSearchQuery_StripsPrefixes(t *testing.T) {
	out := normalizeProfileSearchQuery("  nostr:@fiatjaf ")
	if out.NormalizedQuery != "fiatjaf" {
		t.Fatalf("unexpected normalized query: %q", out.NormalizedQuery)
	}
}

func TestNormalizeProfileSearchQuery_DecodesNpubIdentifier(t *testing.T) {
	pubkey := "f6e7657f7c0c6b03d4de2f2648c64d13f53cf9ce9e840ff6f3f4f85f8b5c5f55"
	npub := mustEncodeNpub(t, pubkey)
	out := normalizeProfileSearchQuery(npub)
	if out.CanonicalIdentifier != pubkey {
		t.Fatalf("unexpected canonical identifier: got %q want %q", out.CanonicalIdentifier, pubkey)
	}
}

func mustEncodeNpub(t *testing.T, pubkey string) string {
	t.Helper()
	raw, err := hex.DecodeString(pubkey)
	if err != nil {
		t.Fatalf("decode pubkey: %v", err)
	}
	words, err := bech32.ConvertBits(raw, 8, 5, true)
	if err != nil {
		t.Fatalf("convert bits: %v", err)
	}
	npub, err := bech32.Encode("npub", words)
	if err != nil {
		t.Fatalf("encode npub: %v", err)
	}
	return npub
}
