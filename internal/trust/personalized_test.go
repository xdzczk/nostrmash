package trust

import "testing"

func TestTeleportFromFollows(t *testing.T) {
	got := teleportFromFollows([]string{" a ", "", "b"})
	if len(got) != 2 || got["a"] != 1 || got["b"] != 1 {
		t.Fatalf("unexpected teleport vector: %#v", got)
	}
}

func TestTruncatePersonalizedScores(t *testing.T) {
	in := []PersonalizedScore{{Pubkey: "a"}, {Pubkey: "b"}, {Pubkey: "c"}}
	got := truncatePersonalizedScores(in, 2)
	if len(got) != 2 || got[0].Pubkey != "a" || got[1].Pubkey != "b" {
		t.Fatalf("unexpected truncate result: %#v", got)
	}
	if len(truncatePersonalizedScores(in, 0)) != 3 {
		t.Fatalf("expected limit<=0 to keep all scores")
	}
}
