package derivation

import "testing"

func TestNormalizedContentFingerprint(t *testing.T) {
	short := "hello nostr"
	if _, ok := normalizedContentFingerprint(short); ok {
		t.Fatal("short content should not fingerprint")
	}
	body := "this is a long enough note about relays and people that stays stable"
	withNoise := body + " https://example.com/a nostr:note1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"
	left, ok := normalizedContentFingerprint(withNoise)
	if !ok {
		t.Fatal("long content should fingerprint")
	}
	right, ok := normalizedContentFingerprint(body + " https://other.example/b")
	if !ok || left != right {
		t.Fatalf("urls should not change the fingerprint: %s %s", left, right)
	}
}
