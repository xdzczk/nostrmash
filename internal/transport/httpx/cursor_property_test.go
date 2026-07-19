package httpx

import (
	"errors"
	"strings"
	"testing"

	"pgregory.net/rapid"
)

// TestEventCursorPayloadRoundTripProperty asserts the cursor codec is a total,
// lossless round-trip over arbitrary payloads, including the documented
// empty/invalid edges. Encode never loses a valid (created_at,id) pair, and
// Decode recovers exactly the trimmed id and created_at.
func TestEventCursorPayloadRoundTripProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		createdAt := rapid.Int64().Draw(t, "created_at")
		rawID := rapid.String().Draw(t, "id")
		payload := EventCursorPayload{CreatedAt: createdAt, ID: rawID}
		trimmedID := strings.TrimSpace(rawID)

		encoded, err := EncodeEventCursorPayload(payload)
		if err != nil {
			t.Fatalf("encode returned error for %+v: %v", payload, err)
		}

		decoded, decErr := DecodeEventCursorPayload(encoded)

		switch {
		case trimmedID == "" && createdAt == 0:
			// Fully empty cursor: encodes to the empty token, decodes to nil.
			if encoded != "" {
				t.Fatalf("expected empty token for empty cursor, got %q", encoded)
			}
			if decErr != nil || decoded != nil {
				t.Fatalf("expected (nil,nil) decode for empty token, got (%+v,%v)", decoded, decErr)
			}
		case trimmedID == "":
			// A cursor with only created_at is not a valid cursor: the id is
			// required on decode.
			if !errors.Is(decErr, ErrCursorIDRequired) {
				t.Fatalf("expected ErrCursorIDRequired for id-less cursor, got %v", decErr)
			}
		default:
			if decErr != nil {
				t.Fatalf("decode returned error for %+v (token %q): %v", payload, encoded, decErr)
			}
			if decoded == nil {
				t.Fatalf("decode returned nil for non-empty cursor %+v", payload)
				return
			}
			if decoded.CreatedAt != createdAt {
				t.Fatalf("created_at round-trip mismatch: got %d want %d", decoded.CreatedAt, createdAt)
			}
			if decoded.ID != trimmedID {
				t.Fatalf("id round-trip mismatch: got %q want %q", decoded.ID, trimmedID)
			}
		}
	})
}
