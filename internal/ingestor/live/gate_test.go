package live

import (
	"context"
	"errors"
	"strings"
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
	exists       map[string]struct{}
	addresses    map[string]struct{}
	err          error
	calls        int
	addressCalls int
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

func (f *fakeTargetChecker) ReplaceableAddressesExist(ctx context.Context, addresses []nostr.AddressCoordinate) (bool, error) {
	f.addressCalls++
	if f.err != nil {
		return false, f.err
	}
	for _, addr := range addresses {
		if _, ok := f.addresses[addr.String()]; ok {
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
		d := p.evaluateGate(context.Background(), kind, "anyone", nil, "")
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
	if d := enforce.evaluateGate(context.Background(), 5, "alice", nil, ""); !d.accept || d.decision != gateDecisionAccept {
		t.Fatalf("trusted deleter: expected accept, got %+v", d)
	}
	if checker.calls != 0 {
		t.Fatalf("trusted deleter: expected no target lookups, got %d", checker.calls)
	}
	if d := enforce.evaluateGate(context.Background(), 5, "alice", nil, ""); d.kindLabel != "5" {
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
	if d := enforce.evaluateGate(context.Background(), 5, "mallory", tags, ""); d.accept || d.decision != gateDecisionRejectUntrustedAuthor {
		t.Fatalf("untrusted deleter enforce: expected reject_untrusted_author, got %+v", d)
	}
	if checker.calls != 0 {
		t.Fatalf("author-gated deletion must not probe targets, got %d lookups", checker.calls)
	}

	shadow := newGateProcessor(t, TrustGateModeOpen, trusted, checker)
	if d := shadow.evaluateGate(context.Background(), 5, "mallory", tags, ""); !d.accept || d.decision != gateDecisionShadowReject {
		t.Fatalf("untrusted deleter shadow: expected accept+shadow_reject, got %+v", d)
	}
}

func TestEvaluateGate_DeletionFailClosedWhenNeverLoaded(t *testing.T) {
	t.Parallel()
	neverLoaded := &fakeTrustedAuthors{loaded: false}

	enforce := newGateProcessor(t, TrustGateModeTrustedOnly, neverLoaded, &fakeTargetChecker{})
	if d := enforce.evaluateGate(context.Background(), 5, "alice", nil, ""); d.accept || d.decision != gateDecisionFailClosed {
		t.Fatalf("never-loaded enforce: expected fail_closed, got %+v", d)
	}

	shadow := newGateProcessor(t, TrustGateModeOpen, neverLoaded, &fakeTargetChecker{})
	if d := shadow.evaluateGate(context.Background(), 5, "alice", nil, ""); !d.accept || d.decision != gateDecisionShadowReject {
		t.Fatalf("never-loaded shadow: expected accept+shadow_reject, got %+v", d)
	}
}

func TestEvaluateGate_Kind1Author(t *testing.T) {
	t.Parallel()
	trusted := &fakeTrustedAuthors{loaded: true, members: map[string]struct{}{"alice": {}}}

	enforce := newGateProcessor(t, TrustGateModeTrustedOnly, trusted, &fakeTargetChecker{})
	if d := enforce.evaluateGate(context.Background(), 1, "alice", nil, ""); !d.accept || d.decision != gateDecisionAccept {
		t.Fatalf("trusted author: expected accept, got %+v", d)
	}
	if d := enforce.evaluateGate(context.Background(), 1, "mallory", nil, ""); d.accept || d.decision != gateDecisionRejectUntrustedAuthor {
		t.Fatalf("untrusted author enforce: expected reject_untrusted_author, got %+v", d)
	}

	shadow := newGateProcessor(t, TrustGateModeOpen, trusted, &fakeTargetChecker{})
	if d := shadow.evaluateGate(context.Background(), 1, "mallory", nil, ""); !d.accept || d.decision != gateDecisionShadowReject {
		t.Fatalf("untrusted author shadow: expected accept+shadow_reject, got %+v", d)
	}
}

func TestEvaluateGate_Kind1ReplyTargetExistsBypass(t *testing.T) {
	t.Parallel()
	trusted := &fakeTrustedAuthors{loaded: true}
	checker := &fakeTargetChecker{exists: map[string]struct{}{"note1": {}}}
	enforce := newGateProcessor(t, TrustGateModeTrustedOnly, trusted, checker)

	markedReply := [][]string{{"e", "note1", "", "reply"}}
	if d := enforce.evaluateGate(context.Background(), 1, "mallory", markedReply, ""); !d.accept || d.decision != gateDecisionAccept {
		t.Fatalf("untrusted marked reply to local note: expected accept, got %+v", d)
	}

	legacySingle := [][]string{{"e", "note1"}}
	if d := enforce.evaluateGate(context.Background(), 1, "mallory", legacySingle, ""); !d.accept || d.decision != gateDecisionAccept {
		t.Fatalf("untrusted single-e reply to local note: expected accept, got %+v", d)
	}

	// Mentions of a local note must not bypass author gating.
	mentionOnly := [][]string{{"e", "note1", "", "mention"}}
	if d := enforce.evaluateGate(context.Background(), 1, "mallory", mentionOnly, ""); d.accept || d.decision != gateDecisionRejectUntrustedAuthor {
		t.Fatalf("untrusted mention-only note: expected reject_untrusted_author, got %+v", d)
	}
}

func TestEvaluateGate_Kind1ReplyMissingTargetFallsBackToAuthorGate(t *testing.T) {
	t.Parallel()
	trusted := &fakeTrustedAuthors{loaded: true, members: map[string]struct{}{"alice": {}}}
	checker := &fakeTargetChecker{exists: map[string]struct{}{}}
	enforce := newGateProcessor(t, TrustGateModeTrustedOnly, trusted, checker)
	tags := [][]string{{"e", "missing", "", "reply"}}

	if d := enforce.evaluateGate(context.Background(), 1, "alice", tags, ""); !d.accept || d.decision != gateDecisionAccept {
		t.Fatalf("trusted reply to missing parent: expected author-gate accept, got %+v", d)
	}
	if d := enforce.evaluateGate(context.Background(), 1, "mallory", tags, ""); d.accept || d.decision != gateDecisionRejectUntrustedAuthor {
		t.Fatalf("untrusted reply to missing parent: expected reject_untrusted_author, got %+v", d)
	}
}

func TestEvaluateGate_Kind1ReplyBypassWorksWhenTrustNeverLoaded(t *testing.T) {
	t.Parallel()
	neverLoaded := &fakeTrustedAuthors{loaded: false}
	checker := &fakeTargetChecker{exists: map[string]struct{}{"note1": {}}}
	enforce := newGateProcessor(t, TrustGateModeTrustedOnly, neverLoaded, checker)
	tags := [][]string{{"e", "note1", "", "reply"}}
	if d := enforce.evaluateGate(context.Background(), 1, "mallory", tags, ""); !d.accept || d.decision != gateDecisionAccept {
		t.Fatalf("reply to local note with unloaded trust set: expected accept, got %+v", d)
	}
}

func TestReplyParentTargetIDs(t *testing.T) {
	t.Parallel()
	if got := replyParentTargetIDs([][]string{{"e", "only"}}); len(got) != 1 || got[0] != "only" {
		t.Fatalf("single unmarked e: expected [only], got %v", got)
	}
	if got := replyParentTargetIDs([][]string{
		{"e", "root", "", "root"},
		{"e", "parent", "", "reply"},
	}); len(got) != 1 || got[0] != "parent" {
		t.Fatalf("marked reply: expected [parent], got %v", got)
	}
	if got := replyParentTargetIDs([][]string{{"e", "quoted", "", "mention"}}); len(got) != 0 {
		t.Fatalf("mention-only: expected nil, got %v", got)
	}
}

func TestEvaluateGate_AuthorGatedProductKinds(t *testing.T) {
	t.Parallel()
	trusted := &fakeTrustedAuthors{loaded: true, members: map[string]struct{}{"alice": {}}}
	enforce := newGateProcessor(t, TrustGateModeTrustedOnly, trusted, &fakeTargetChecker{})

	for _, kind := range []int{4, 5, 20, 21, 22, 1984, 9802, 10000, 10001, 10003, 10019, 10050, 30000, 30023, 30311, 34235, 34236} {
		if d := enforce.evaluateGate(context.Background(), kind, "alice", nil, ""); !d.accept || d.decision != gateDecisionAccept {
			t.Fatalf("kind %d trusted author: expected accept, got %+v", kind, d)
		}
		if d := enforce.evaluateGate(context.Background(), kind, "mallory", nil, ""); d.accept || d.decision != gateDecisionRejectUntrustedAuthor {
			t.Fatalf("kind %d untrusted author enforce: expected reject_untrusted_author, got %+v", kind, d)
		}
		if d := enforce.evaluateGate(context.Background(), kind, "mallory", nil, ""); d.kindLabel != gateKindLabel(kind) || d.kindLabel == "other" {
			t.Fatalf("kind %d: expected dedicated metric label, got %q", kind, d.kindLabel)
		}
	}
}

func TestEvaluateGate_Kind1FailClosedWhenNeverLoaded(t *testing.T) {
	t.Parallel()
	neverLoaded := &fakeTrustedAuthors{loaded: false}

	enforce := newGateProcessor(t, TrustGateModeTrustedOnly, neverLoaded, &fakeTargetChecker{})
	if d := enforce.evaluateGate(context.Background(), 1, "alice", nil, ""); d.accept || d.decision != gateDecisionFailClosed {
		t.Fatalf("never-loaded enforce: expected fail_closed, got %+v", d)
	}

	shadow := newGateProcessor(t, TrustGateModeOpen, neverLoaded, &fakeTargetChecker{})
	if d := shadow.evaluateGate(context.Background(), 1, "alice", nil, ""); !d.accept || d.decision != gateDecisionShadowReject {
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
		if d := enforce.evaluateGate(context.Background(), kind, "anyone", tags, ""); !d.accept || d.decision != gateDecisionAccept {
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
	if d := enforce.evaluateGate(context.Background(), 7, "anyone", tags, ""); d.accept || d.decision != gateDecisionRejectMissingTarget {
		t.Fatalf("missing target enforce: expected reject_missing_target, got %+v", d)
	}

	shadow := newGateProcessor(t, TrustGateModeOpen, trusted, checker)
	if d := shadow.evaluateGate(context.Background(), 7, "anyone", tags, ""); !d.accept || d.decision != gateDecisionShadowReject {
		t.Fatalf("missing target shadow: expected accept+shadow_reject, got %+v", d)
	}
}

func TestEvaluateGate_EngagementNoTagsRejectedWhenEnforced(t *testing.T) {
	t.Parallel()
	checker := &fakeTargetChecker{exists: map[string]struct{}{}}
	enforce := newGateProcessor(t, TrustGateModeTrustedOnly, &fakeTrustedAuthors{loaded: true}, checker)
	if d := enforce.evaluateGate(context.Background(), 6, "anyone", nil, ""); d.accept || d.decision != gateDecisionRejectMissingTarget {
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
	if d := enforce.evaluateGate(context.Background(), 7, "anyone", tags, ""); !d.accept || d.decision != gateDecisionAccept {
		t.Fatalf("target check error: expected fail-open accept, got %+v", d)
	}
}

func TestEvaluateGate_Kind16AddressOrEventTarget(t *testing.T) {
	t.Parallel()
	pubkey := strings.Repeat("ab", 32)
	addr := nostr.AddressCoordinate{Kind: 30023, Pubkey: pubkey, DTag: "post"}
	trusted := &fakeTrustedAuthors{loaded: true}
	checker := &fakeTargetChecker{addresses: map[string]struct{}{addr.String(): {}}}
	enforce := newGateProcessor(t, TrustGateModeTrustedOnly, trusted, checker)

	if d := enforce.evaluateGate(context.Background(), 16, "mallory", [][]string{{"a", addr.String()}}, ""); !d.accept || d.decision != gateDecisionAccept {
		t.Fatalf("address target: expected accept, got %+v", d)
	}
	missing := newGateProcessor(t, TrustGateModeTrustedOnly, trusted, &fakeTargetChecker{})
	if d := missing.evaluateGate(context.Background(), 16, "mallory", [][]string{{"e", "gone"}, {"a", addr.String()}}, ""); d.accept || d.decision != gateDecisionRejectMissingTarget {
		t.Fatalf("neither target: expected reject_missing_target, got %+v", d)
	}
	if d := missing.evaluateGate(context.Background(), 16, "mallory", nil, ""); d.accept || d.decision != gateDecisionRejectMissingTarget {
		t.Fatalf("no tags: expected reject_missing_target, got %+v", d)
	}
}

func TestEvaluateGate_Kind1111RootOrAuthor(t *testing.T) {
	t.Parallel()
	trusted := &fakeTrustedAuthors{loaded: true, members: map[string]struct{}{"alice": {}}}
	checker := &fakeTargetChecker{exists: map[string]struct{}{"root1": {}}}
	enforce := newGateProcessor(t, TrustGateModeTrustedOnly, trusted, checker)
	tags := [][]string{{"E", "root1"}, {"e", "parent1"}, {"K", "1"}}
	if d := enforce.evaluateGate(context.Background(), 1111, "mallory", tags, ""); !d.accept || d.decision != gateDecisionAccept {
		t.Fatalf("untrusted comment on stored root: expected accept, got %+v", d)
	}

	unrooted := newGateProcessor(t, TrustGateModeTrustedOnly, trusted, &fakeTargetChecker{})
	if d := unrooted.evaluateGate(context.Background(), 1111, "mallory", [][]string{{"e", "parent1"}}, ""); d.accept || d.decision != gateDecisionRejectUntrustedAuthor {
		t.Fatalf("untrusted comment without root: expected reject_untrusted_author, got %+v", d)
	}
	if d := unrooted.evaluateGate(context.Background(), 1111, "alice", nil, ""); !d.accept || d.decision != gateDecisionAccept {
		t.Fatalf("trusted comment without root: expected accept, got %+v", d)
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

func TestEvaluateGate_GiftWrapAndNutzap(t *testing.T) {
	t.Parallel()
	trusted := &fakeTrustedAuthors{loaded: true, members: map[string]struct{}{"alice": {}}}
	enforce := newGateProcessor(t, TrustGateModeTrustedOnly, trusted, &fakeTargetChecker{exists: map[string]struct{}{"note1": {}}})

	if d := enforce.evaluateGate(context.Background(), 1059, "ephemeral", [][]string{{"p", "alice"}}, ""); !d.accept || d.decision != gateDecisionAccept {
		t.Fatalf("gift wrap to trusted recipient: %+v", d)
	}
	if d := enforce.evaluateGate(context.Background(), 1059, "ephemeral", [][]string{{"p", "mallory"}}, ""); d.accept || d.decision != gateDecisionRejectUntrustedAuthor {
		t.Fatalf("gift wrap to untrusted recipient: %+v", d)
	}
	if d := enforce.evaluateGate(context.Background(), 1059, "ephemeral", nil, ""); d.accept || d.decision != gateDecisionRejectMissingTarget {
		t.Fatalf("gift wrap without recipient: %+v", d)
	}
	if d := enforce.evaluateGate(context.Background(), 9321, "mallory", [][]string{{"e", "note1"}, {"p", "alice"}}, ""); !d.accept || d.decision != gateDecisionAccept {
		t.Fatalf("nutzap with stored target: %+v", d)
	}
	if d := enforce.evaluateGate(context.Background(), 9321, "mallory", [][]string{{"p", "alice"}}, ""); !d.accept || d.decision != gateDecisionAccept {
		t.Fatalf("nutzap to trusted recipient: %+v", d)
	}
	if d := enforce.evaluateGate(context.Background(), 9321, "mallory", [][]string{{"p", "bob"}}, ""); d.accept || d.decision != gateDecisionRejectUntrustedAuthor {
		t.Fatalf("nutzap to untrusted recipient: %+v", d)
	}
}

func TestEvaluateGate_GiftWrapRecipientBudget(t *testing.T) {
	t.Parallel()
	trusted := &fakeTrustedAuthors{loaded: true, members: map[string]struct{}{"alice": {}}}
	for _, mode := range []string{TrustGateModeTrustedOnly, TrustGateModeOpen} {
		p := newGateProcessor(t, mode, trusted, &fakeTargetChecker{})
		p.SetGiftWrapRecipientLimit(2)
		for i := 0; i < 2; i++ {
			d := p.evaluateGate(context.Background(), 1059, "ephemeral", [][]string{{"p", "alice"}}, "")
			if !d.accept || d.decision != gateDecisionAccept {
				t.Fatalf("%s wrap %d: %+v", mode, i, d)
			}
		}
		d := p.evaluateGate(context.Background(), 1059, "ephemeral", [][]string{{"p", "alice"}}, "")
		if d.accept || d.decision != gateDecisionRejectRecipientBudget {
			t.Fatalf("%s over budget: %+v", mode, d)
		}
		// A different trusted recipient still has a full budget.
		trusted.members["bob"] = struct{}{}
		d = p.evaluateGate(context.Background(), 1059, "ephemeral", [][]string{{"p", "bob"}}, "")
		if !d.accept || d.decision != gateDecisionAccept {
			t.Fatalf("%s other recipient: %+v", mode, d)
		}
		delete(trusted.members, "bob")
	}
}

type fakeRelayInfo struct{ pubkey string }

func (f fakeRelayInfo) RelayInfoPubkey(context.Context, string) (string, bool, error) {
	if f.pubkey == "" {
		return "", false, nil
	}
	return f.pubkey, true, nil
}

func TestEvaluateGate_GroupsPollsAndLiveChat(t *testing.T) {
	t.Parallel()
	trusted := &fakeTrustedAuthors{loaded: true, members: map[string]struct{}{"alice": {}}}
	pubkey := strings.Repeat("ab", 32)
	addr := nostr.AddressCoordinate{Kind: 30311, Pubkey: pubkey, DTag: "live"}
	checker := &fakeTargetChecker{exists: map[string]struct{}{"poll1": {}}, addresses: map[string]struct{}{addr.String(): {}}}
	enforce := newGateProcessor(t, TrustGateModeTrustedOnly, trusted, checker)
	enforce.SetRelayInfo(fakeRelayInfo{pubkey: "relay-admin"})

	if d := enforce.evaluateGate(context.Background(), 9, "alice", nil, ""); !d.accept || d.decision != gateDecisionAccept {
		t.Fatalf("trusted group message: %+v", d)
	}
	if d := enforce.evaluateGate(context.Background(), 9, "mallory", nil, ""); d.accept || d.decision != gateDecisionRejectUntrustedAuthor {
		t.Fatalf("untrusted group message: %+v", d)
	}
	if d := enforce.evaluateGate(context.Background(), 39000, "relay-admin", nil, "wss://relay.example"); !d.accept || d.kindLabel != "39000" {
		t.Fatalf("relay metadata from relay pubkey: %+v", d)
	}
	if d := enforce.evaluateGate(context.Background(), 39002, "mallory", nil, "wss://relay.example"); d.accept || d.decision != gateDecisionRejectUntrustedAuthor {
		t.Fatalf("relay metadata from other pubkey: %+v", d)
	}
	if d := enforce.evaluateGate(context.Background(), 1311, "anyone", [][]string{{"a", addr.String()}}, ""); !d.accept {
		t.Fatalf("live chat on stored activity: %+v", d)
	}
	if d := enforce.evaluateGate(context.Background(), 1311, "anyone", nil, ""); d.accept || d.decision != gateDecisionRejectMissingTarget {
		t.Fatalf("live chat without activity: %+v", d)
	}
	if d := enforce.evaluateGate(context.Background(), 1018, "anyone", [][]string{{"e", "poll1"}}, ""); !d.accept {
		t.Fatalf("poll response on stored poll: %+v", d)
	}
	if d := enforce.evaluateGate(context.Background(), 1068, "mallory", nil, ""); d.accept {
		t.Fatalf("untrusted poll: %+v", d)
	}
}
