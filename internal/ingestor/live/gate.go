package live

import (
	"context"
	"strings"
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
	case 9735:
		return "9735"
	case 9802:
		return "9802"
	case 10000:
		return "10000"
	case 10003:
		return "10003"
	case 30023:
		return "30023"
	case 5:
		return "5"
	case 0, 3, 10002:
		return "open_kind"
	default:
		return "other"
	}
}

// isAuthorGatedKind reports whether an event kind is persisted only when its
// author is in the trusted set. This covers kind 1 notes plus the authored
// product kinds in the live filter group: encrypted DMs (4, gated on sender
// trust), highlights (9802), mute lists (10000), bookmark lists (10003), and
// long-form articles (30023).
func isAuthorGatedKind(kind int) bool {
	switch kind {
	case 1, 4, 9802, 10000, 10003, 30023:
		return true
	default:
		return false
	}
}

func isEngagementKind(kind int) bool {
	switch kind {
	case 6, 7, 9735:
		return true
	default:
		return false
	}
}

// maxDeletionTargetChecks bounds how many e-tag target ids a single kind-5
// event may probe in the existence check, so a spam deletion carrying
// thousands of tags cannot turn the gate into an unbounded ANY() query. A
// genuine deletion whose real target is beyond the cap is rejected exactly
// like one whose targets are all absent — an acceptable miss, since deletions
// from untrusted authors can only ever match the engagement/open-kind rows we
// chose to store.
const maxDeletionTargetChecks = 100

// evaluateGate decides whether a valid event should be persisted under the
// configured gate mode. It never rejects in open mode (records shadow_reject
// for would-be drops); in trusted_only mode it enforces, including
// fail-closed-on-never-loaded for author-gated kinds.
func (p *Processor) evaluateGate(ctx context.Context, kind int, pubkey string, tags [][]string) gateDecision {
	kindLabel := gateKindLabel(kind)
	enforce := p.gateMode == TrustGateModeTrustedOnly

	switch {
	case isAuthorGatedKind(kind):
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

	case kind == 5:
		// Deletion tombstones (NIP-09). A deletion for an event we never
		// stored is useless to serve, and open ingestion of kind 5 let
		// tombstone spam grow to ~79% of the events table in production. A
		// trusted author's deletion is always kept (it may cover an a-tag
		// addressable target or an event hydrated later); otherwise the
		// deletion must reference at least one locally-stored event, the same
		// existence rule the engagement kinds use.
		if p.trustedAuthors.Loaded() && p.trustedAuthors.Contains(pubkey) {
			return gateDecision{accept: true, kindLabel: kindLabel, decision: gateDecisionAccept}
		}
		ids := allETagValues(tags)
		if len(ids) > maxDeletionTargetChecks {
			ids = ids[:maxDeletionTargetChecks]
		}
		exists := false
		if len(ids) > 0 {
			var err error
			exists, err = p.targetChecker.EventsExist(ctx, ids)
			if err != nil {
				// Transient store error: fail OPEN, same as engagement kinds.
				p.log.Warn("ingest_gate_target_check_failed", "error", err, "kind", kind)
				return gateDecision{accept: true, kindLabel: kindLabel, decision: gateDecisionAccept}
			}
		}
		if exists {
			return gateDecision{accept: true, kindLabel: kindLabel, decision: gateDecisionAccept}
		}
		if enforce {
			return gateDecision{accept: false, kindLabel: kindLabel, decision: gateDecisionRejectMissingTarget}
		}
		return gateDecision{accept: true, kindLabel: kindLabel, decision: gateDecisionShadowReject}

	case isEngagementKind(kind):
		ids := engagementTargetIDs(kind, tags)
		exists := false
		if len(ids) > 0 {
			var err error
			exists, err = p.targetChecker.EventsExist(ctx, ids)
			if err != nil {
				// Transient store error: fail OPEN so a DB blip does not silently
				// drop engagement. The subsequent persist would surface the real
				// error downstream anyway.
				p.log.Warn("ingest_gate_target_check_failed", "error", err, "kind", kind)
				return gateDecision{accept: true, kindLabel: kindLabel, decision: gateDecisionAccept}
			}
		}
		if exists {
			return gateDecision{accept: true, kindLabel: kindLabel, decision: gateDecisionAccept}
		}
		if enforce {
			return gateDecision{accept: false, kindLabel: kindLabel, decision: gateDecisionRejectMissingTarget}
		}
		return gateDecision{accept: true, kindLabel: kindLabel, decision: gateDecisionShadowReject}

	default:
		// Open kinds (0,3,10002) and any other subscribed kind always pass.
		return gateDecision{accept: true, kindLabel: kindLabel, decision: gateDecisionAccept}
	}
}

// engagementTargetIDs returns the candidate target event ids referenced by an
// engagement event. For zaps (9735) it uses the same first-`e`-tag rule as zap
// derivation (see internal/derivation/handlers_zap.go); for reactions (7) and
// reposts (6) it returns all referenced `e` tag ids.
func engagementTargetIDs(kind int, tags [][]string) []string {
	switch kind {
	case 9735:
		if id := firstETagValue(tags); id != "" {
			return []string{id}
		}
		return nil
	case 6, 7:
		return allETagValues(tags)
	default:
		return nil
	}
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
