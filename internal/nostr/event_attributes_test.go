package nostr

import "testing"

func TestParseEventAttributes(t *testing.T) {
	t.Parallel()
	id := "0000000f" + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	attrs := ParseEventAttributes(id, [][]string{
		{"client", "Damus", "31990:37ce94259421d17a13e04382205c6061323ebc6bbfa46aab1f73e6f93c774a5e:app"},
		{"expiration", "1700000000"},
		{"content-warning", "spoilers"},
		{"proxy", "https://example.com/1", "activitypub"},
		{"nonce", "abc", "16"},
		{"k", "1"},
	})
	if attrs.Client != "Damus" || attrs.ClientPubkey == "" {
		t.Fatalf("client: %+v", attrs)
	}
	if attrs.ExpiresAt == nil || *attrs.ExpiresAt != 1700000000 {
		t.Fatalf("expiration: %+v", attrs.ExpiresAt)
	}
	if !attrs.ContentWarning || attrs.ContentWarningReason != "spoilers" {
		t.Fatalf("warning: %+v", attrs)
	}
	if attrs.ProxyProtocol != "activitypub" || attrs.ProxyID == "" {
		t.Fatalf("proxy: %+v", attrs)
	}
	if attrs.PowTarget != 16 || attrs.PowBits < 20 {
		t.Fatalf("pow bits=%d target=%d", attrs.PowBits, attrs.PowTarget)
	}
	if attrs.TargetKind == nil || *attrs.TargetKind != 1 {
		t.Fatalf("target kind: %+v", attrs.TargetKind)
	}
	if !attrs.Expired(1700000000) || attrs.Expired(1699999999) {
		t.Fatal("expiry boundary")
	}
}

func TestNormalizeReactionContent(t *testing.T) {
	t.Parallel()
	if got := NormalizeReactionContent("  "); got != "+" {
		t.Fatalf("empty: %q", got)
	}
	if got := NormalizeReactionContent("👍"); got != "+" {
		t.Fatalf("plus emoji: %q", got)
	}
	if got := NormalizeReactionContent("👎"); got != "-" {
		t.Fatalf("minus: %q", got)
	}
	if got := NormalizeReactionContent("😂"); got != "😂" {
		t.Fatalf("emoji: %q", got)
	}
}

func TestZapAmountBucket(t *testing.T) {
	t.Parallel()
	if ZapAmountBucket(20) != "lt21" || ZapAmountBucket(21) != "21_100" || ZapAmountBucket(100001) != "gt100k" {
		t.Fatal("bucket boundaries")
	}
}
