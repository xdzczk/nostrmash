package query

import (
	"encoding/binary"
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

// CanonicalizePubkey normalizes hex or bech32 npub identifiers to lowercase hex.
// Returns an empty string when the input is not a valid pubkey identifier.
func CanonicalizePubkey(raw string) string {
	return canonicalizePubkeyIdentifier(raw)
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

// normalizedEventIdentifier holds a decoded note1 or nevent1 identifier.
type normalizedEventIdentifier struct {
	EventID      string
	RelayHints   []string
	AuthorPubkey string
	Kind         *int
}

// canonicalizeEventIdentifier decodes a note1 or nevent1 bech32 string
// into a hex event ID (and optional TLV fields for nevent). Returns
// zero-value if the input is not a valid event identifier.
func canonicalizeEventIdentifier(raw string) normalizedEventIdentifier {
	value := strings.TrimSpace(strings.ToLower(raw))
	if value == "" {
		return normalizedEventIdentifier{}
	}
	if len(value) == 64 {
		if _, err := hex.DecodeString(value); err == nil {
			return normalizedEventIdentifier{EventID: value}
		}
	}
	if strings.HasPrefix(value, "note1") {
		return decodeNote1(value)
	}
	if strings.HasPrefix(value, "nevent1") {
		return decodeNevent1(value)
	}
	return normalizedEventIdentifier{}
}

func decodeNote1(value string) normalizedEventIdentifier {
	hrp, words, err := bech32.DecodeNoLimit(value)
	if err != nil || hrp != "note" {
		return normalizedEventIdentifier{}
	}
	decoded, err := bech32.ConvertBits(words, 5, 8, false)
	if err != nil || len(decoded) != 32 {
		return normalizedEventIdentifier{}
	}
	return normalizedEventIdentifier{EventID: hex.EncodeToString(decoded)}
}

func decodeNevent1(value string) normalizedEventIdentifier {
	hrp, words, err := bech32.DecodeNoLimit(value)
	if err != nil || hrp != "nevent" {
		return normalizedEventIdentifier{}
	}
	decoded, err := bech32.ConvertBits(words, 5, 8, false)
	if err != nil || len(decoded) == 0 {
		return normalizedEventIdentifier{}
	}
	var out normalizedEventIdentifier
	for i := 0; i < len(decoded); {
		if i+2 > len(decoded) {
			break
		}
		tlvType := decoded[i]
		tlvLen := int(decoded[i+1])
		i += 2
		if i+tlvLen > len(decoded) {
			break
		}
		tlvValue := decoded[i : i+tlvLen]
		i += tlvLen
		switch tlvType {
		case 0:
			if len(tlvValue) == 32 {
				out.EventID = hex.EncodeToString(tlvValue)
			}
		case 1:
			if relay := strings.TrimSpace(string(tlvValue)); relay != "" {
				out.RelayHints = append(out.RelayHints, relay)
			}
		case 2:
			if len(tlvValue) == 32 {
				out.AuthorPubkey = hex.EncodeToString(tlvValue)
			}
		case 3:
			if len(tlvValue) == 4 {
				k := int(binary.BigEndian.Uint32(tlvValue))
				out.Kind = &k
			}
		}
	}
	return out
}
