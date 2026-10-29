package live

import (
	"context"
	"strings"

	"github.com/xdzczk/nostrmash/internal/nostr"
)

// Trust gate modes. "open" runs in shadow (records would-be decisions as
// metrics but never rejects); "trusted_only" enforces.
const (
	TrustGateModeOpen        = "open"
	TrustGateModeTrustedOnly = "trusted_only"
)

// TrustedAuthors reports trusted-author membership for the ingest gate.
// Satisfied by *runtime.TrustedAuthorSet.
type TrustedAuthors interface {
	// Contains reports whether the pubkey is currently trusted.
	Contains(pubkey string) bool
	// Loaded reports whether the trusted set has ever loaded successfully.
	Loaded() bool
}

// TargetExistenceChecker reports whether referenced events exist locally.
// Satisfied by *store.PostgresStore.
type TargetExistenceChecker interface {
	EventsExist(ctx context.Context, ids []string) (bool, error)
	ReplaceableAddressesExist(ctx context.Context, addresses []nostr.AddressCoordinate) (bool, error)
}

// Gate decision labels. Kept to a fixed, bounded set so the gate-decision
// metric does not explode label cardinality.
const (
	gateDecisionAccept                = "accept"
	gateDecisionRejectUntrustedAuthor = "reject_untrusted_author"
	gateDecisionRejectMissingTarget   = "reject_missing_target"
	gateDecisionRejectBlockedAuthor   = "reject_blocked_author"
	gateDecisionShadowReject          = "shadow_reject"
	gateDecisionFailClosed            = "fail_closed"
)

type gateDecision struct {
	accept    bool
	kindLabel string
	decision  string
}

// gateKindLabel normalizes an event kind to a bounded metric label. The label
// set is fixed (no caller-supplied values) so the gate-decision metric cannot
// explode cardinality.
func gateKindLabel(kind int) string {
	switch kind {
	case 1:
		return "1"
	case 4:
		return "4"
	case 6:
		return "6"
	case 7:
		return "7"
	case 16:
		return "16"
	case 20:
		return "20"
	case 21:
		return "21"
	case 22:
		return "22"
	case 1111:
		return "1111"
	case 1984:
		return "1984"
	case 9735:
		return "9735"
	case 9802:
		return "9802"
	case 10000:
		return "10000"
	case 10001:
		return "10001"
	case 10003:
		return "10003"
	case 30000:
		return "30000"
	case 30023:
		return "30023"
	case 30311:
		return "30311"
	case 34235:
		return "34235"
	case 34236:
		return "34236"
	case 5:
		return "5"
	case 0, 3, 10002:
		return "open_kind"
	default:
		return "other"
	}
}

// isAuthorGatedKind reports whether an event kind is persisted only when its
// author is in the trusted set (unless a more specific branch handles it
// first). Kind 1 root notes use this path; kind 1 replies with a locally
// stored thread parent are accepted earlier via target-exists. Also covers
// encrypted DMs (4, gated on sender trust), deletion tombstones (5),
// highlights (9802), mute and pin lists (10000, 10001), bookmark lists
// (10003), categorized people lists (30000), pictures and video (20, 21, 22,
// 34235, 34236), reports (1984), live activities (30311), and long-form
// articles (30023).
//
// Kind 5 used to be a special case (accept if trusted OR any referenced
// e-tag target exists locally). That target-exists path still let untrusted
// tombstone spam dominate the events table in production (~24M kind-5 rows
// from outside the trust graph). Author-gating deletions means only WoT
// authors' NIP-09 tombstones are kept; engagement from untrusted authors
// that we previously stored may no longer be deletable via their own kind-5
// — an accepted trade-off against unbounded tombstone growth.
func isAuthorGatedKind(kind int) bool {
	switch kind {
	case 1, 4, 5, 20, 21, 22, 1984, 9802, 10000, 10001, 10003, 30000, 30023, 30311, 34235, 34236:
		return true
	default:
		return false
	}
}

func isEngagementKind(kind int) bool {
	switch kind {
	case 6, 7, 16, 9735:
		return true
	default:
		return false
	}
}

// evaluateGate decides whether a valid event should be persisted under the
// configured gate mode. It never rejects in open mode (records shadow_reject
// for would-be drops); in trusted_only mode it enforces, including
// fail-closed-on-never-loaded for author-gated kinds.
func (p *Processor) evaluateGate(ctx context.Context, kind int, pubkey string, tags [][]string) gateDecision {
	kindLabel := gateKindLabel(kind)
	enforce := p.gateMode == TrustGateModeTrustedOnly

	switch {
	case kind == 1:
		// Replies to already-stored notes are accepted from any author (like
		// engagement), so conversation totals are not capped to the trust
		// graph. Missing parents do not reject here: trusted authors still
		// pass via the author gate below (root notes and replies-before-parent).
		if p.replyParentExists(ctx, kind, tags) {
			return gateDecision{accept: true, kindLabel: kindLabel, decision: gateDecisionAccept}
		}
		return p.evaluateAuthorGate(kindLabel, pubkey, enforce)

	case kind == 1111:
		// NIP-22 comments follow the kind-1 split: a stored root accepts any
		// author; otherwise only a trusted author is kept.
		ids, addrs := nip22RootTargets(tags)
		if len(ids) > 0 || len(addrs) > 0 {
			decision, ok := p.evaluateAnyTarget(ctx, kindLabel, kind, ids, addrs, enforce)
			if ok && decision.decision == gateDecisionAccept {
				return decision
			}
		}
		return p.evaluateAuthorGate(kindLabel, pubkey, enforce)

	case isAuthorGatedKind(kind):
		return p.evaluateAuthorGate(kindLabel, pubkey, enforce)

	case isEngagementKind(kind):
		ids := engagementTargetIDs(kind, tags)
		var addrs []nostr.AddressCoordinate
		if kind == 16 {
			addrs = addressCoordinates(tags, "a")
		}
		if len(ids) == 0 && len(addrs) == 0 {
			if enforce {
				return gateDecision{accept: false, kindLabel: kindLabel, decision: gateDecisionRejectMissingTarget}
			}
			return gateDecision{accept: true, kindLabel: kindLabel, decision: gateDecisionShadowReject}
		}
		decision, _ := p.evaluateAnyTarget(ctx, kindLabel, kind, ids, addrs, enforce)
		return decision

	default:
		// Open kinds (0,3,10002) and any other subscribed kind always pass.
		return gateDecision{accept: true, kindLabel: kindLabel, decision: gateDecisionAccept}
	}
}

// replyParentExists reports whether a kind-1 event's thread parent is already
// stored locally. Store lookup errors fail open (treat as exists) so a DB blip
// does not drop replies that would otherwise be accepted.
func (p *Processor) replyParentExists(ctx context.Context, kind int, tags [][]string) bool {
	ids := replyParentTargetIDs(tags)
	if len(ids) == 0 {
		return false
	}
	exists, err := p.targetChecker.EventsExist(ctx, ids)
	if err != nil {
		p.log.Warn("ingest_gate_target_check_failed", "error", err, "kind", kind)
		return true
	}
	return exists
}

func (p *Processor) evaluateAuthorGate(kindLabel, pubkey string, enforce bool) gateDecision {
	if !p.trustedAuthors.Loaded() {
		// Never loaded: in trusted_only fail CLOSED rather than guessing.
		if enforce {
			return gateDecision{accept: false, kindLabel: kindLabel, decision: gateDecisionFailClosed}
		}
		return gateDecision{accept: true, kindLabel: kindLabel, decision: gateDecisionShadowReject}
	}
	if p.trustedAuthors.Contains(pubkey) {
		return gateDecision{accept: true, kindLabel: kindLabel, decision: gateDecisionAccept}
	}
	if enforce {
		return gateDecision{accept: false, kindLabel: kindLabel, decision: gateDecisionRejectUntrustedAuthor}
	}
	return gateDecision{accept: true, kindLabel: kindLabel, decision: gateDecisionShadowReject}
}

// evaluateTargetExistsGate accepts when any candidate target exists locally.
// When there are no candidate ids it returns handled=false so the caller can
// apply a different policy (kind-1 roots fall through to author gating).
func (p *Processor) evaluateTargetExistsGate(
	ctx context.Context,
	kindLabel string,
	kind int,
	ids []string,
	enforce bool,
) (gateDecision, bool) {
	if len(ids) == 0 {
		return gateDecision{}, false
	}
	exists, err := p.targetChecker.EventsExist(ctx, ids)
	if err != nil {
		// Transient store error: fail OPEN so a DB blip does not silently
		// drop engagement/replies. The subsequent persist would surface the
		// real error downstream anyway.
		p.log.Warn("ingest_gate_target_check_failed", "error", err, "kind", kind)
		return gateDecision{accept: true, kindLabel: kindLabel, decision: gateDecisionAccept}, true
	}
	if exists {
		return gateDecision{accept: true, kindLabel: kindLabel, decision: gateDecisionAccept}, true
	}
	if enforce {
		return gateDecision{accept: false, kindLabel: kindLabel, decision: gateDecisionRejectMissingTarget}, true
	}
	return gateDecision{accept: true, kindLabel: kindLabel, decision: gateDecisionShadowReject}, true
}

// evaluateAnyTarget accepts when any event id or replaceable address exists.
// A store error fails open. When nothing matches and enforce is set, the
// decision is reject_missing_target. The bool is false only when there was
// nothing to check.
func (p *Processor) evaluateAnyTarget(
	ctx context.Context,
	kindLabel string,
	kind int,
	ids []string,
	addrs []nostr.AddressCoordinate,
	enforce bool,
) (gateDecision, bool) {
	if len(ids) == 0 && len(addrs) == 0 {
		return gateDecision{}, false
	}
	if len(ids) > 0 {
		decision, handled := p.evaluateTargetExistsGate(ctx, kindLabel, kind, ids, enforce)
		// Accept covers a stored id and a fail-open store error. A miss
		// (reject or shadow) still tries address targets before giving up.
		if handled && decision.decision == gateDecisionAccept {
			return decision, true
		}
	}
	if len(addrs) > 0 {
		exists, err := p.targetChecker.ReplaceableAddressesExist(ctx, addrs)
		if err != nil {
			p.log.Warn("ingest_gate_target_check_failed", "error", err, "kind", kind)
			return gateDecision{accept: true, kindLabel: kindLabel, decision: gateDecisionAccept}, true
		}
		if exists {
			return gateDecision{accept: true, kindLabel: kindLabel, decision: gateDecisionAccept}, true
		}
	}
	if enforce {
		return gateDecision{accept: false, kindLabel: kindLabel, decision: gateDecisionRejectMissingTarget}, true
	}
	return gateDecision{accept: true, kindLabel: kindLabel, decision: gateDecisionShadowReject}, true
}

// engagementTargetIDs returns the candidate target event ids referenced by an
// engagement event. For zaps (9735) it uses the same first-`e`-tag rule as zap
// derivation (see internal/derivation/handlers_zap.go); for reactions (7) and
// reposts (6, 16) it returns all referenced `e` tag ids. Kind 16 address
// targets are resolved separately.
func engagementTargetIDs(kind int, tags [][]string) []string {
	switch kind {
	case 9735:
		if id := firstETagValue(tags); id != "" {
			return []string{id}
		}
		return nil
	case 6, 7, 16:
		return allETagValues(tags)
	default:
		return nil
	}
}

// nip22RootTargets returns the NIP-22 root scope. Uppercase E is an event id
// and uppercase A is an address. Lowercase tags are the parent and do not
// admit the comment on their own.
func nip22RootTargets(tags [][]string) ([]string, []nostr.AddressCoordinate) {
	return exactTagValues(tags, "E"), addressCoordinates(tags, "A")
}

func addressCoordinates(tags [][]string, name string) []nostr.AddressCoordinate {
	out := make([]nostr.AddressCoordinate, 0, 1)
	seen := make(map[string]struct{}, 1)
	for _, tag := range tags {
		if len(tag) < 2 || tag[0] != name {
			continue
		}
		addr, ok := nostr.ParseAddressCoordinate(tag[1])
		if !ok {
			continue
		}
		key := addr.String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, addr)
	}
	return out
}

func exactTagValues(tags [][]string, name string) []string {
	out := make([]string, 0, 1)
	seen := make(map[string]struct{}, 1)
	for _, tag := range tags {
		if len(tag) < 2 || tag[0] != name {
			continue
		}
		value := strings.TrimSpace(tag[1])
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

// replyParentTargetIDs returns the thread-parent event id for a kind-1 note.
// Uses the shared NIP-10 parent rule in internal/nostr (prefer reply, else root;
// legacy single unmarked #e counts; mentions alone do not).
func replyParentTargetIDs(tags [][]string) []string {
	parent := nostr.ReplyParentEventID(tags)
	if parent == "" {
		return nil
	}
	return []string{parent}
}

func firstETagValue(tags [][]string) string {
	for _, tag := range tags {
		if len(tag) < 2 || tag[0] != "e" {
			continue
		}
		if v := strings.TrimSpace(tag[1]); v != "" {
			return v
		}
	}
	return ""
}

func allETagValues(tags [][]string) []string {
	out := make([]string, 0, 2)
	seen := make(map[string]struct{}, 2)
	for _, tag := range tags {
		if len(tag) < 2 || tag[0] != "e" {
			continue
		}
		v := strings.TrimSpace(tag[1])
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}
