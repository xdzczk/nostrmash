package nostr

import (
	"encoding/hex"
	"strconv"
	"strings"
	"unicode/utf8"
)

// EventAttributes is the closed set of tags NostrMash keeps off event_tags
// and projects into event_attributes. Empty strings mean the tag was absent.
type EventAttributes struct {
	Client               string
	ClientPubkey         string
	ExpiresAt            *int64
	ContentWarning       bool
	ContentWarningReason string
	ProxyProtocol        string
	ProxyID              string
	PowBits              int
	PowTarget            int
	TargetKind           *int
	Subject              string
	HasAlt               bool
	PublishedAt          *int64
	ZapSplitCount        int
}

// ParseEventAttributes reads client, expiration, content-warning, proxy,
// nonce, and k tags. eventID is the event id whose leading zero bits are the
// computed proof-of-work (NIP-13).
func ParseEventAttributes(eventID string, tags [][]string) EventAttributes {
	out := EventAttributes{PowBits: LeadingZeroBits(eventID)}
	for _, tag := range tags {
		if len(tag) < 2 {
			if len(tag) == 1 && tag[0] == "content-warning" {
				out.ContentWarning = true
			}
			continue
		}
		switch tag[0] {
		case "client":
			if out.Client == "" {
				out.Client = trimBound(tag[1], 64)
				if len(tag) >= 3 {
					if addr, ok := ParseAddressCoordinate(tag[2]); ok {
						out.ClientPubkey = addr.Pubkey
					}
				}
			}
		case "expiration":
			if out.ExpiresAt == nil {
				if n, err := strconv.ParseInt(strings.TrimSpace(tag[1]), 10, 64); err == nil && n > 0 {
					out.ExpiresAt = &n
				}
			}
		case "content-warning":
			out.ContentWarning = true
			if out.ContentWarningReason == "" {
				out.ContentWarningReason = trimBound(tag[1], 120)
			}
		case "proxy":
			if out.ProxyID == "" {
				out.ProxyID = trimBound(tag[1], 160)
				if len(tag) >= 3 {
					out.ProxyProtocol = trimBound(strings.ToLower(tag[2]), 32)
				}
			}
		case "nonce":
			if out.PowTarget == 0 && len(tag) >= 3 {
				if n, err := strconv.Atoi(strings.TrimSpace(tag[2])); err == nil && n > 0 && n < 256 {
					out.PowTarget = n
				}
			}
		case "k":
			if out.TargetKind == nil {
				if n, err := strconv.Atoi(strings.TrimSpace(tag[1])); err == nil && n >= 0 && n <= 65535 {
					out.TargetKind = &n
				}
			}
		case "subject":
			if out.Subject == "" {
				out.Subject = trimBound(tag[1], 160)
			}
		case "alt":
			out.HasAlt = true
		case "published_at":
			if out.PublishedAt == nil {
				if n, err := strconv.ParseInt(strings.TrimSpace(tag[1]), 10, 64); err == nil && n > 0 {
					out.PublishedAt = &n
				}
			}
		case "zap":
			if len(tag) >= 2 && strings.TrimSpace(tag[1]) != "" {
				out.ZapSplitCount++
			}
		}
	}
	return out
}

// Bridged reports whether the event carries a NIP-48 proxy tag, meaning it
// was copied in from another network. A proxy tag with no protocol still
// counts: the tag itself is the claim of a non-Nostr origin.
func (a EventAttributes) Bridged() bool {
	return a.ProxyID != "" || a.ProxyProtocol != ""
}

// Expired reports whether a NIP-40 expiration is already in the past.
func (a EventAttributes) Expired(nowUnix int64) bool {
	return a.ExpiresAt != nil && *a.ExpiresAt <= nowUnix
}

// LeadingZeroBits counts leading zero bits in a hex event id. A bad id is 0.
func LeadingZeroBits(eventID string) int {
	raw, err := hex.DecodeString(strings.TrimSpace(eventID))
	if err != nil || len(raw) == 0 {
		return 0
	}
	bits := 0
	for _, b := range raw {
		if b == 0 {
			bits += 8
			continue
		}
		for mask := byte(0x80); mask > 0; mask >>= 1 {
			if b&mask != 0 {
				return bits
			}
			bits++
		}
		break
	}
	return bits
}

// NormalizeReactionContent collapses NIP-25 reaction content into a bounded key.
// "+" and an empty body are the same upvote. "-" is a downvote. Everything
// else is kept as a short emoji/text key.
func NormalizeReactionContent(content string) string {
	trimmed := strings.TrimSpace(content)
	switch trimmed {
	case "", "+", "👍":
		return "+"
	case "-", "👎":
		return "-"
	default:
		if utf8.RuneCountInString(trimmed) > 32 {
			runes := []rune(trimmed)
			trimmed = string(runes[:32])
		}
		return trimmed
	}
}

// ZapAmountBucket is the histogram key for a sat amount.
func ZapAmountBucket(sats int64) string {
	switch {
	case sats < 21:
		return "lt21"
	case sats <= 100:
		return "21_100"
	case sats <= 1000:
		return "101_1k"
	case sats <= 10000:
		return "1k_10k"
	case sats <= 100000:
		return "10k_100k"
	default:
		return "gt100k"
	}
}

func trimBound(value string, maxRunes int) string {
	value = strings.TrimSpace(value)
	if value == "" || maxRunes <= 0 {
		return ""
	}
	if utf8.RuneCountInString(value) <= maxRunes {
		return value
	}
	return string([]rune(value)[:maxRunes])
}
