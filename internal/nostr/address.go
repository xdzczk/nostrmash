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

// IsIndexedNoteKind reports whether kind is copied into note search and discovery.
func IsIndexedNoteKind(kind int) bool {
	switch kind {
	case 1, 20, 21, 22, 30023, 30311, 34235, 34236:
		return true
	default:
		return false
	}
}
