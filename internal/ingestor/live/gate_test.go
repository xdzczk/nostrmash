package live

import (
	"context"
	"errors"
	"testing"

	"github.com/xdzczk/nostrmash/internal/nostr"
)

type fakeTrustedAuthors struct {
	loaded  bool
	members map[string]struct{}
}

func (f *fakeTrustedAuthors) Contains(pubkey string) bool {
	_, ok := f.members[pubkey]
	return ok
}

func (f *fakeTrustedAuthors) Loaded() bool { return f.loaded }

type fakeTargetChecker struct {
	exists map[string]struct{}
	err    error
	calls  int
}

func (f *fakeTargetChecker) EventsExist(ctx context.Context, ids []string) (bool, error) {
	f.calls++
	if f.err != nil {
		return false, f.err
	}
	for _, id := range ids {
		if _, ok := f.exists[id]; ok {
			return true, nil
		}
	}
	return false, nil
}

func newGateProcessor(t *testing.T, mode string, trusted *fakeTrustedAuthors, checker *fakeTargetChecker) *Processor {
	t.Helper()
	p, err := NewProcessor(silentLogger(), &fakeStore{}, nostr.Options{})
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	p.SetTrustGate(mode, trusted, checker)
	return p
}

func TestEvaluateGate_OpenKindsAlwaysAccept(t *testing.T) {
	t.Parallel()
	p := newGateProcessor(t, TrustGateModeTrustedOnly, &fakeTrustedAuthors{loaded: false}, &fakeTargetChecker{})
	for _, kind := range []int{0, 3, 10002} {
		d := p.evaluateGate(context.Background(), kind, "anyone", nil)
		if !d.accept || d.decision != gateDecisionAccept {
			t.Fatalf("kind %d: expected accept, got %+v", kind, d)
		}
		if d.kindLabel != "open_kind" {
			t.Fatalf("kind %d: expected open_kind label, got %q", kind, d.kindLabel)
		}
	}
}

func TestEvaluateGate_DeletionTrustedAuthorAlwaysAccepted(t *testing.T) {
	t.Parallel()
	trusted := &fakeTrustedAuthors{loaded: true, members: map[string]struct{}{"alice": {}}}
	checker := &fakeTargetChecker{exists: map[string]struct{}{}}

	enforce := newGateProcessor(t, TrustGateModeTrustedOnly, trusted, checker)
	// No e-tags and no stored targets: trust alone must carry the decision.
	if d := enforce.evaluateGate(context.Background(), 5, "alice", nil); !d.accept || d.decision != gateDecisionAccept {
		t.Fatalf("trusted deleter: expected accept, got %+v", d)
	}
	if checker.calls != 0 {
		t.Fatalf("trusted deleter: expected no target lookups, got %d", checker.calls)
	}
	if d := enforce.evaluateGate(context.Background(), 5, "alice", nil); d.kindLabel != "5" {
		t.Fatalf("expected dedicated kind label 5, got %q", d.kindLabel)
	}
}

func TestEvaluateGate_DeletionUntrustedAuthorRejected(t *testing.T) {
	t.Parallel()
	trusted := &fakeTrustedAuthors{loaded: true}
	// Even with a locally-stored e-tag target, untrusted authors' deletions
	// are rejected — kind 5 is author-gated, not target-gated.
	checker := &fakeTargetChecker{exists: map[string]struct{}{"note1": {}}}
	tags := [][]string{{"e", "note1"}, {"a", "30023:someone:post"}}

	enforce := newGateProcessor(t, TrustGateModeTrustedOnly, trusted, checker)
	if d := enforce.evaluateGate(context.Background(), 5, "mallory", tags); d.accept || d.decision != gateDecisionRejectUntrustedAuthor {
		t.Fatalf("untrusted deleter enforce: expected reject_untrusted_author, got %+v", d)
	}
	if checker.calls != 0 {
		t.Fatalf("author-gated deletion must not probe targets, got %d lookups", checker.calls)
	}

	shadow := newGateProcessor(t, TrustGateModeOpen, trusted, checker)
	if d := shadow.evaluateGate(context.Background(), 5, "mallory", tags); !d.accept || d.decision != gateDecisionShadowReject {
		t.Fatalf("untrusted deleter shadow: expected accept+shadow_reject, got %+v", d)
	}
}

func TestEvaluateGate_DeletionFailClosedWhenNeverLoaded(t *testing.T) {
	t.Parallel()
	neverLoaded := &fakeTrustedAuthors{loaded: false}

	enforce := newGateProcessor(t, TrustGateModeTrustedOnly, neverLoaded, &fakeTargetChecker{})
	if d := enforce.evaluateGate(context.Background(), 5, "alice", nil); d.accept || d.decision != gateDecisionFailClosed {
		t.Fatalf("never-loaded enforce: expected fail_closed, got %+v", d)
	}

	shadow := newGateProcessor(t, TrustGateModeOpen, neverLoaded, &fakeTargetChecker{})
	if d := shadow.evaluateGate(context.Background(), 5, "alice", nil); !d.accept || d.decision != gateDecisionShadowReject {
		t.Fatalf("never-loaded shadow: expected accept+shadow_reject, got %+v", d)
	}
}

func TestEvaluateGate_Kind1Author(t *testing.T) {
	t.Parallel()
	trusted := &fakeTrustedAuthors{loaded: true, members: map[string]struct{}{"alice": {}}}

	enforce := newGateProcessor(t, TrustGateModeTrustedOnly, trusted, &fakeTargetChecker{})
	if d := enforce.evaluateGate(context.Background(), 1, "alice", nil); !d.accept || d.decision != gateDecisionAccept {
		t.Fatalf("trusted author: expected accept, got %+v", d)
	}
	if d := enforce.evaluateGate(context.Background(), 1, "mallory", nil); d.accept || d.decision != gateDecisionRejectUntrustedAuthor {
		t.Fatalf("untrusted author enforce: expected reject_untrusted_author, got %+v", d)
	}

	shadow := newGateProcessor(t, TrustGateModeOpen, trusted, &fakeTargetChecker{})
	if d := shadow.evaluateGate(context.Background(), 1, "mallory", nil); !d.accept || d.decision != gateDecisionShadowReject {
		t.Fatalf("untrusted author shadow: expected accept+shadow_reject, got %+v", d)
	}
}

func TestEvaluateGate_AuthorGatedProductKinds(t *testing.T) {
	t.Parallel()
	trusted := &fakeTrustedAuthors{loaded: true, members: map[string]struct{}{"alice": {}}}
	enforce := newGateProcessor(t, TrustGateModeTrustedOnly, trusted, &fakeTargetChecker{})

	for _, kind := range []int{4, 5, 9802, 10000, 10003, 30023} {
		if d := enforce.evaluateGate(context.Background(), kind, "alice", nil); !d.accept || d.decision != gateDecisionAccept {
			t.Fatalf("kind %d trusted author: expected accept, got %+v", kind, d)
		}
		if d := enforce.evaluateGate(context.Background(), kind, "mallory", nil); d.accept || d.decision != gateDecisionRejectUntrustedAuthor {
			t.Fatalf("kind %d untrusted author enforce: expected reject_untrusted_author, got %+v", kind, d)
		}
		if d := enforce.evaluateGate(context.Background(), kind, "mallory", nil); d.kindLabel != gateKindLabel(kind) || d.kindLabel == "other" {
			t.Fatalf("kind %d: expected dedicated metric label, got %q", kind, d.kindLabel)
		}
	}
}

func TestEvaluateGate_Kind1FailClosedWhenNeverLoaded(t *testing.T) {
	t.Parallel()
	neverLoaded := &fakeTrustedAuthors{loaded: false}

	enforce := newGateProcessor(t, TrustGateModeTrustedOnly, neverLoaded, &fakeTargetChecker{})
	if d := enforce.evaluateGate(context.Background(), 1, "alice", nil); d.accept || d.decision != gateDecisionFailClosed {
		t.Fatalf("never-loaded enforce: expected fail_closed, got %+v", d)
	}

	shadow := newGateProcessor(t, TrustGateModeOpen, neverLoaded, &fakeTargetChecker{})
	if d := shadow.evaluateGate(context.Background(), 1, "alice", nil); !d.accept || d.decision != gateDecisionShadowReject {
		t.Fatalf("never-loaded shadow: expected accept+shadow_reject, got %+v", d)
	}
}

func TestEvaluateGate_EngagementTargetExists(t *testing.T) {
	t.Parallel()
	checker := &fakeTargetChecker{exists: map[string]struct{}{"note1": {}}}
	trusted := &fakeTrustedAuthors{loaded: true}

	enforce := newGateProcessor(t, TrustGateModeTrustedOnly, trusted, checker)
	for _, kind := range []int{6, 7, 9735} {
		tags := [][]string{{"e", "note1"}}
		if d := enforce.evaluateGate(context.Background(), kind, "anyone", tags); !d.accept || d.decision != gateDecisionAccept {
			t.Fatalf("kind %d target exists: expected accept, got %+v", kind, d)
		}
	}
}

func TestEvaluateGate_EngagementMissingTarget(t *testing.T) {
	t.Parallel()
	checker := &fakeTargetChecker{exists: map[string]struct{}{}}
	trusted := &fakeTrustedAuthors{loaded: true}

	enforce := newGateProcessor(t, TrustGateModeTrustedOnly, trusted, checker)
	tags := [][]string{{"e", "missing"}}
	if d := enforce.evaluateGate(context.Background(), 7, "anyone", tags); d.accept || d.decision != gateDecisionRejectMissingTarget {
		t.Fatalf("missing target enforce: expected reject_missing_target, got %+v", d)
	}

	shadow := newGateProcessor(t, TrustGateModeOpen, trusted, checker)
	if d := shadow.evaluateGate(context.Background(), 7, "anyone", tags); !d.accept || d.decision != gateDecisionShadowReject {
		t.Fatalf("missing target shadow: expected accept+shadow_reject, got %+v", d)
	}
}

func TestEvaluateGate_EngagementNoTagsRejectedWhenEnforced(t *testing.T) {
	t.Parallel()
	checker := &fakeTargetChecker{exists: map[string]struct{}{}}
	enforce := newGateProcessor(t, TrustGateModeTrustedOnly, &fakeTrustedAuthors{loaded: true}, checker)
	if d := enforce.evaluateGate(context.Background(), 6, "anyone", nil); d.accept || d.decision != gateDecisionRejectMissingTarget {
		t.Fatalf("no e-tags enforce: expected reject_missing_target, got %+v", d)
	}
	if checker.calls != 0 {
		t.Fatalf("expected no target lookups when no e-tags present, got %d", checker.calls)
	}
}

func TestEvaluateGate_EngagementTargetCheckErrorFailsOpen(t *testing.T) {
	t.Parallel()
	checker := &fakeTargetChecker{err: errors.New("db down")}
	enforce := newGateProcessor(t, TrustGateModeTrustedOnly, &fakeTrustedAuthors{loaded: true}, checker)
	tags := [][]string{{"e", "note1"}}
	if d := enforce.evaluateGate(context.Background(), 7, "anyone", tags); !d.accept || d.decision != gateDecisionAccept {
		t.Fatalf("target check error: expected fail-open accept, got %+v", d)
	}
}

func TestEngagementTargetIDs(t *testing.T) {
	t.Parallel()
	// zaps: first non-empty e tag only.
	zapTags := [][]string{{"p", "receiver"}, {"e", "zap_target"}, {"e", "second"}}
	got := engagementTargetIDs(9735, zapTags)
	if len(got) != 1 || got[0] != "zap_target" {
		t.Fatalf("zap target ids: expected [zap_target], got %v", got)
	}

	// reactions/reposts: all distinct e tags.
	multiTags := [][]string{{"e", "a"}, {"e", "b"}, {"e", "a"}, {"p", "x"}}
	for _, kind := range []int{6, 7} {
		got := engagementTargetIDs(kind, multiTags)
		if len(got) != 2 || got[0] != "a" || got[1] != "b" {
			t.Fatalf("kind %d target ids: expected [a b], got %v", kind, got)
		}
	}
}
