// Package account models the NostrMash account lifecycle: the eight states an
// account can occupy, how a derived state is computed from cheap signals, and
// how an effective state is resolved from the derived state plus manual
// overrides and tracking. All logic here is pure and side-effect free so it can
// be unit tested and reused across the worker (recompute), ingestor (gate), and
// query/api (coverage) layers without import cycles.
package account

import "strings"

// State is an account lifecycle state.
type State string

const (
	StateUnknown    State = "unknown"
	StateObserved   State = "observed"
	StateCandidate  State = "candidate"
	StateMeaningful State = "meaningful"
	StateTrusted    State = "trusted"
	StateTracked    State = "tracked"
	StateStrategic  State = "strategic"
	StateBlocked    State = "blocked"
)

// rank orders states by retention/priority weight. Higher means "keep longer /
// more important". Blocked is a special sentinel below unknown so it never wins
// a max() against a derived/promotion state by accident; it is only ever set
// explicitly via override or block list.
var rank = map[State]int{
	StateBlocked:    -1,
	StateUnknown:    0,
	StateObserved:   1,
	StateCandidate:  2,
	StateMeaningful: 3,
	StateTrusted:    4,
	StateTracked:    5,
	StateStrategic:  6,
}

// Valid reports whether s is a known state.
func Valid(s State) bool {
	_, ok := rank[s]
	return ok
}

// Parse normalizes and validates a state string, returning ok=false for
// unknown values.
func Parse(s string) (State, bool) {
	st := State(strings.TrimSpace(strings.ToLower(s)))
	if !Valid(st) {
		return StateUnknown, false
	}
	return st, true
}

// Rank returns the ordering weight of a state (unknown states rank 0).
func Rank(s State) int {
	return rank[s]
}

// AtLeast reports whether a ranks >= b. Blocked never satisfies AtLeast for any
// non-blocked target.
func AtLeast(a, b State) bool {
	return rank[a] >= rank[b]
}

// IngestAccepted reports whether an account in this state should have its
// author-gated content accepted into canonical storage by the ingest gate
// (in addition to graph-trusted authors). Tracked and strategic accounts are
// explicitly requested/important and so are accepted; blocked is never
// accepted (handled separately as a hard drop).
func IngestAccepted(s State) bool {
	switch s {
	case StateTracked, StateStrategic:
		return true
	default:
		return false
	}
}

// Signals is the cheap, already-available evidence used to derive a state. It
// deliberately avoids anything requiring an expensive per-pubkey scan on the
// hot path; the worker recompute loop assembles these from existing
// projections.
type Signals struct {
	// TrustHops is the BFS hop distance from a seed in trust_graph_snapshot, or
	// -1 when the pubkey is not in the trust graph.
	TrustHops int
	// ObservedCount is how many times the pubkey has been seen at ingest.
	ObservedCount int64
	// EngagedByTrustedCount is the number of distinct trusted accounts that
	// have engaged (reply/reaction/repost/zap) with this account's content.
	EngagedByTrustedCount int
	// HasProfileMetadata is true when a kind-0 profile has been seen.
	HasProfileMetadata bool
	// NoteCount is the number of authored notes known locally.
	NoteCount int
}

// Params tunes the derived-state thresholds. DefaultParams are sane starting
// values; operators/tests can override.
type Params struct {
	TrustedMaxHops         int
	CandidateMinObserved   int64
	CandidateMinEngagedBy  int
	MeaningfulMinNotes     int
	MeaningfulMinEngagedBy int
	ObservedMinCount       int64
}

// DefaultParams are the baseline derived-state thresholds.
var DefaultParams = Params{
	TrustedMaxHops:         2,
	CandidateMinObserved:   5,
	CandidateMinEngagedBy:  1,
	MeaningfulMinNotes:     10,
	MeaningfulMinEngagedBy: 3,
	ObservedMinCount:       2,
}

// Resolve computes the derived state from signals. It only ever returns states
// in {unknown, observed, candidate, meaningful, trusted}. The promotion-only
// states (tracked, strategic) and blocked are never derived; they are applied
// by EffectiveState via tracking and overrides.
func Resolve(sig Signals, p Params) State {
	if sig.TrustHops >= 0 && sig.TrustHops <= p.TrustedMaxHops {
		return StateTrusted
	}
	meaningful := sig.HasProfileMetadata &&
		sig.NoteCount >= p.MeaningfulMinNotes &&
		sig.EngagedByTrustedCount >= p.MeaningfulMinEngagedBy
	if meaningful {
		return StateMeaningful
	}
	candidate := (sig.EngagedByTrustedCount >= p.CandidateMinEngagedBy) ||
		(sig.HasProfileMetadata && sig.ObservedCount >= p.CandidateMinObserved)
	if candidate {
		return StateCandidate
	}
	if sig.ObservedCount >= p.ObservedMinCount {
		return StateObserved
	}
	return StateUnknown
}

// EffectiveState resolves the state actually applied to an account:
//   - a manual override always wins (including blocked/strategic/tracked);
//   - otherwise the derived state is floored at "tracked" when the account has
//     been explicitly tracked (e.g. hydrated on demand), so tracking is sticky
//     and the ingest gate keeps accepting its content;
//   - otherwise the derived state is used as-is.
func EffectiveState(derived State, override State, tracked bool) State {
	if Valid(override) && override != "" {
		return override
	}
	base := derived
	if !Valid(base) {
		base = StateUnknown
	}
	if tracked && rank[StateTracked] > rank[base] {
		base = StateTracked
	}
	return base
}
