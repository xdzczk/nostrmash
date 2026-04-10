package query

import (
	"encoding/hex"
	"strings"

	"github.com/btcsuite/btcd/btcutil/bech32"
)

type normalizedProfileSearchQuery struct {
	RawQuery            string
	NormalizedQuery     string
	CanonicalIdentifier string
}

func normalizeProfileSearchQuery(raw string) normalizedProfileSearchQuery {
	trimmed := strings.TrimSpace(raw)
	normalized := strings.TrimSpace(strings.TrimPrefix(trimmed, "nostr:"))
	normalized = strings.TrimSpace(strings.TrimPrefix(normalized, "@"))
	out := normalizedProfileSearchQuery{
		RawQuery:        trimmed,
		NormalizedQuery: normalized,
	}
	if candidate := canonicalizePubkeyIdentifier(normalized); candidate != "" {
		out.CanonicalIdentifier = candidate
	}
	return out
}

func canonicalizePubkeyIdentifier(raw string) string {
	value := strings.TrimSpace(strings.ToLower(raw))
	if value == "" {
		return ""
	}
	if len(value) == 64 {
		if _, err := hex.DecodeString(value); err == nil {
			return value
		}
	}
	if !strings.HasPrefix(value, "npub1") {
		return ""
	}
	hrp, words, err := bech32.DecodeNoLimit(value)
	if err != nil {
		return ""
	}
	if hrp != "npub" {
		return ""
	}
	decoded, err := bech32.ConvertBits(words, 5, 8, false)
	if err != nil {
		return ""
	}
	if len(decoded) != 32 {
		return ""
	}
	return hex.EncodeToString(decoded)
}
