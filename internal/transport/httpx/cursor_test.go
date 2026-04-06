package httpx

import "testing"

func TestEncodeDecodeEventCursorPayloadRoundTrip(t *testing.T) {
	encoded, err := EncodeEventCursorPayload(EventCursorPayload{
		CreatedAt: 123,
		ID:        "abc",
	})
	if err != nil {
		t.Fatalf("encode cursor: %v", err)
	}
	if encoded == "" {
		t.Fatal("expected encoded cursor")
	}
	decoded, err := DecodeEventCursorPayload(encoded)
	if err != nil {
		t.Fatalf("decode cursor: %v", err)
	}
	if decoded == nil || decoded.ID != "abc" || decoded.CreatedAt != 123 {
		t.Fatalf("unexpected decoded payload: %#v", decoded)
	}
}

func TestDecodeEventCursorPayloadRequiresID(t *testing.T) {
	encoded, err := EncodeEventCursorPayload(EventCursorPayload{CreatedAt: 12, ID: ""})
	if err != nil {
		t.Fatalf("encode cursor: %v", err)
	}
	if _, err := DecodeEventCursorPayload(encoded); err != ErrCursorIDRequired {
		t.Fatalf("expected ErrCursorIDRequired, got %v", err)
	}
}
