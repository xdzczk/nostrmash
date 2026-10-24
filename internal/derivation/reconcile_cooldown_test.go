package derivation

import (
	"testing"
	"time"
)

func TestReconcileCooldownSkipsUntilDeadline(t *testing.T) {
	c := newReconcileCooldown()
	now := time.Date(2026, 10, 24, 8, 0, 0, 0, time.UTC)
	if c.skip("bot", now) {
		t.Fatal("empty cooldown skipped a pubkey")
	}
	c.mark("bot", now.Add(time.Hour))
	if !c.skip("bot", now.Add(30*time.Minute)) {
		t.Fatal("expected skip inside the cooldown")
	}
	if c.skip("bot", now.Add(time.Hour)) {
		t.Fatal("expected the cooldown to expire at the deadline")
	}
	if c.skip("bot", now.Add(2*time.Hour)) {
		t.Fatal("expired cooldown entry was not forgotten")
	}
}

func TestReconcileCooldownNilIsOpen(t *testing.T) {
	var c *reconcileCooldown
	if c.skip("bot", time.Now()) {
		t.Fatal("nil cooldown skipped a pubkey")
	}
	c.mark("bot", time.Now().Add(time.Hour))
}
