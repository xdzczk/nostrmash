package trust

import "testing"

func TestRedisKeyspace_RunScopedKeys(t *testing.T) {
	keys := newRedisKeyspace("nm")
	runID := int64(42)
	snapshot := "snap 1"

	nodes := keys.runNodesKey(runID, snapshot)
	adj := keys.runAdjKey(runID, snapshot, "pubkeyA")
	rev := keys.runRevAdjKey(runID, snapshot, "pubkeyA")
	meta := keys.runMetaKey(runID, snapshot)

	wantPrefix := "nm:trust:run:42:snap_1:"
	if nodes != wantPrefix+"nodes" {
		t.Fatalf("unexpected nodes key: %q", nodes)
	}
	if adj != wantPrefix+"adj:pubkeyA" {
		t.Fatalf("unexpected adj key: %q", adj)
	}
	if rev != wantPrefix+"rev_adj:pubkeyA" {
		t.Fatalf("unexpected rev adj key: %q", rev)
	}
	if meta != wantPrefix+"meta" {
		t.Fatalf("unexpected meta key: %q", meta)
	}
}

func TestSanitizeSnapshotRef(t *testing.T) {
	if got := sanitizeSnapshotRef(" "); got != "snapshot" {
		t.Fatalf("expected default snapshot ref, got %q", got)
	}
	if got := sanitizeSnapshotRef("a b"); got != "a_b" {
		t.Fatalf("expected spaces replaced, got %q", got)
	}
}
