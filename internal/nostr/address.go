package nostr

import (
	"fmt"
	"strconv"
	"strings"
)

// AddressCoordinate is a NIP-33 address: kind:pubkey:d-tag.
type AddressCoordinate struct {
	Kind   int
	Pubkey string
	DTag   string
}

func (a AddressCoordinate) String() string {
	return fmt.Sprintf("%d:%s:%s", a.Kind, a.Pubkey, a.DTag)
}

// ParseAddressCoordinate parses a NIP-33 coordinate. The d-tag is the remainder
// after kind and pubkey, so it may itself contain colons.
func ParseAddressCoordinate(value string) (AddressCoordinate, bool) {
	parts := strings.SplitN(strings.TrimSpace(value), ":", 3)
	if len(parts) < 3 {
		return AddressCoordinate{}, false
	}
	kind, err := strconv.Atoi(parts[0])
	if err != nil || kind < 0 {
		return AddressCoordinate{}, false
	}
	pubkey := strings.ToLower(strings.TrimSpace(parts[1]))
	if len(pubkey) != 64 {
		return AddressCoordinate{}, false
	}
	return AddressCoordinate{Kind: kind, Pubkey: pubkey, DTag: parts[2]}, true
}

// IndexedNoteKindsSQL is the kind list indexed as note-like content
// (short notes, pictures, video, long-form, and live activities).
const IndexedNoteKindsSQL = "1, 20, 21, 22, 30023, 30311, 34235, 34236"

// TrendingMediaKinds is the candidate set for one media ranking.
// Pictures, videos, and shorts are scored apart so one format cannot
// occupy the top of another. Addressable video kinds stay with the
// format they duplicate.
func TrendingMediaKinds(format string) ([]int, bool) {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "pictures":
		return []int{20}, true
	case "videos":
		return []int{21, 34235}, true
	case "shorts":
		return []int{22, 34236}, true
	default:
		return nil, false
	}
}

// IsIndexedNoteKind reports whether kind is copied into note search and discovery.
func IsIndexedNoteKind(kind int) bool {
	switch kind {
	case 1, 20, 21, 22, 30023, 30311, 34235, 34236:
		return true
	default:
		return false
	}
}
