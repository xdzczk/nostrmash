package trust

import (
	"testing"

	"pgregory.net/rapid"
)

// TestNormalizeRelayURLsIdempotentProperty asserts that relay URL normalization
// is idempotent: normalizing an already-normalized list is a no-op, and the
// order map stays consistent with the returned slice. This is the invariant the
// scheduler relies on when it repeatedly reconciles desired relay sets.
func TestNormalizeRelayURLsIdempotentProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		input := rapid.SliceOfN(rapid.String(), 0, 32).Draw(t, "relays")

		norm1, order1 := NormalizeRelayURLs(input)
		norm2, _ := NormalizeRelayURLs(norm1)

		if len(norm2) != len(norm1) {
			t.Fatalf("normalization not idempotent: len %d -> %d\nfirst=%v\nsecond=%v", len(norm1), len(norm2), norm1, norm2)
		}
		for i := range norm1 {
			if norm1[i] != norm2[i] {
				t.Fatalf("normalization not idempotent at %d: %q != %q", i, norm1[i], norm2[i])
			}
		}

		// The order map records each normalized relay's original input index.
		// It must cover exactly the normalized slice, and those indices must be
		// strictly increasing in normalized order (first-occurrence ordering).
		if len(order1) != len(norm1) {
			t.Fatalf("order map size %d != normalized size %d", len(order1), len(norm1))
		}
		prevIdx := -1
		for _, relay := range norm1 {
			idx, ok := order1[relay]
			if !ok {
				t.Fatalf("normalized relay %q missing from order map", relay)
			}
			if idx <= prevIdx {
				t.Fatalf("order map indices not strictly increasing at %q: %d after %d", relay, idx, prevIdx)
			}
			prevIdx = idx
		}

		// Normalized output has no duplicates.
		seen := make(map[string]struct{}, len(norm1))
		for _, relay := range norm1 {
			if _, dup := seen[relay]; dup {
				t.Fatalf("normalized output contains duplicate %q", relay)
			}
			seen[relay] = struct{}{}
		}
	})
}
