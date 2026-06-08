package runtime

import (
	"context"
	"errors"
	"testing"
)

type fakeTrustedLoader struct {
	pubkeys []string
	err     error
	calls   int
}

func (f *fakeTrustedLoader) LoadTrustedSnapshotPubkeys(ctx context.Context, maxHops int) ([]string, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return append([]string(nil), f.pubkeys...), nil
}

func TestTrustedAuthorSet_RefreshLoadsAndNormalizes(t *testing.T) {
	t.Parallel()
	set := NewTrustedAuthorSet(2)
	if set.Loaded() {
		t.Fatal("new set should not report loaded")
	}
	loader := &fakeTrustedLoader{pubkeys: []string{"  ABCD ", "efgh", ""}}
	if err := set.Refresh(context.Background(), loader); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if !set.Loaded() {
		t.Fatal("set should report loaded after successful refresh")
	}
	if set.Size() != 2 {
		t.Fatalf("expected size 2, got %d", set.Size())
	}
	if !set.Contains("abcd") {
		t.Fatal("expected lowercased+trimmed membership for abcd")
	}
	if !set.Contains("EFGH") {
		t.Fatal("Contains should lowercase its input")
	}
	if set.LastRefreshAt().IsZero() {
		t.Fatal("LastRefreshAt should be set after successful refresh")
	}
}

func TestTrustedAuthorSet_RetainsLastGoodOnRefreshError(t *testing.T) {
	t.Parallel()
	set := NewTrustedAuthorSet(2)
	ok := &fakeTrustedLoader{pubkeys: []string{"alice", "bob"}}
	if err := set.Refresh(context.Background(), ok); err != nil {
		t.Fatalf("first Refresh: %v", err)
	}
	firstRefreshAt := set.LastRefreshAt()

	failing := &fakeTrustedLoader{err: errors.New("snapshot query failed")}
	if err := set.Refresh(context.Background(), failing); err == nil {
		t.Fatal("expected error from failing refresh")
	}
	// Last-good membership must survive a failed refresh.
	if !set.Contains("alice") || !set.Contains("bob") {
		t.Fatal("failed refresh must retain last-good set")
	}
	if set.Size() != 2 {
		t.Fatalf("expected size 2 after failed refresh, got %d", set.Size())
	}
	if !set.Loaded() {
		t.Fatal("set should remain loaded after a failed refresh")
	}
	if !set.LastRefreshAt().Equal(firstRefreshAt) {
		t.Fatal("LastRefreshAt must not advance on a failed refresh")
	}
}

func TestTrustedAuthorSet_NilSafe(t *testing.T) {
	t.Parallel()
	var set *TrustedAuthorSet
	if set.Contains("x") || set.Loaded() || set.Size() != 0 || !set.LastRefreshAt().IsZero() {
		t.Fatal("nil set accessors should be safe and empty")
	}
	if set.Blocked("x") {
		t.Fatal("nil set Blocked should be false")
	}
}

// fakeAccountStateLoader satisfies both trustedAuthorLoader and
// accountStateLoader so the refresh exercises account-state augmentation.
type fakeAccountStateLoader struct {
	trusted []string
	accept  []string
	blocked []string

	acceptErr  error
	blockedErr error
}

func (f *fakeAccountStateLoader) LoadTrustedSnapshotPubkeys(context.Context, int) ([]string, error) {
	return append([]string(nil), f.trusted...), nil
}

func (f *fakeAccountStateLoader) LoadIngestAcceptPubkeys(context.Context) ([]string, error) {
	if f.acceptErr != nil {
		return nil, f.acceptErr
	}
	return append([]string(nil), f.accept...), nil
}

func (f *fakeAccountStateLoader) LoadBlockedPubkeys(context.Context) ([]string, error) {
	if f.blockedErr != nil {
		return nil, f.blockedErr
	}
	return append([]string(nil), f.blocked...), nil
}

func TestTrustedAuthorSet_UnionsAcceptSetAndRecordsBlocked(t *testing.T) {
	t.Parallel()
	set := NewTrustedAuthorSet(2)
	loader := &fakeAccountStateLoader{
		trusted: []string{"alice"},
		accept:  []string{" TRACKED ", "strategic"},
		blocked: []string{"BadActor"},
	}
	if err := set.Refresh(context.Background(), loader); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	// Graph-trusted author plus account-state accept-set authors are all members.
	for _, pk := range []string{"alice", "tracked", "strategic"} {
		if !set.Contains(pk) {
			t.Fatalf("expected %q in accept set", pk)
		}
	}
	if set.Size() != 3 {
		t.Fatalf("expected 3 accept members, got %d", set.Size())
	}
	// Blocked is normalized and recorded, and is not in the accept set.
	if !set.Blocked("badactor") {
		t.Fatal("expected badactor to be blocked")
	}
	if set.Contains("badactor") {
		t.Fatal("blocked author must not be an accept-set member")
	}
}

func TestTrustedAuthorSet_AccountStateErrorsDoNotCollapseTrusted(t *testing.T) {
	t.Parallel()
	set := NewTrustedAuthorSet(2)
	loader := &fakeAccountStateLoader{
		trusted:    []string{"alice", "bob"},
		acceptErr:  errors.New("accept query failed"),
		blockedErr: errors.New("blocked query failed"),
	}
	if err := set.Refresh(context.Background(), loader); err != nil {
		t.Fatalf("Refresh should succeed despite account-state errors: %v", err)
	}
	if !set.Contains("alice") || !set.Contains("bob") {
		t.Fatal("graph-trusted members must survive account-state augmentation errors")
	}
	if set.Size() != 2 {
		t.Fatalf("expected 2 trusted members, got %d", set.Size())
	}
	if set.Blocked("anyone") {
		t.Fatal("no blocked entries expected when blocked query failed")
	}
}
