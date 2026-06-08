package account

import "time"

// Completeness describes how much NostrMash actually knows about an account, so
// the API can be honest instead of implying it holds a full archive.
type Completeness string

const (
	// CompletenessComplete: hydrated recently with a full coverage window.
	CompletenessComplete Completeness = "complete"
	// CompletenessPartial: tracked, but coverage is bounded/incomplete.
	CompletenessPartial Completeness = "partial"
	// CompletenessStale: previously hydrated, but the data is now old.
	CompletenessStale Completeness = "stale"
	// CompletenessHydrating: a hydration run is currently in flight.
	CompletenessHydrating Completeness = "hydrating"
	// CompletenessNotTracked: below candidate / no data worth surfacing.
	CompletenessNotTracked Completeness = "not_tracked"
	// CompletenessUnsupported: the requested data class is not modeled.
	CompletenessUnsupported Completeness = "unsupported"
)

// CompletenessInputs is the evidence used to classify coverage. All time
// pointers are nil when unknown.
type CompletenessInputs struct {
	State                     State
	Exists                    bool
	InFlightHydration         bool
	LastHydratedAt            *time.Time
	LastSuccessfulHydrationAt *time.Time
	CoverageWindowDays        *int
	// StaleAfter is how old the last successful hydration may be before the
	// account is considered stale.
	StaleAfter time.Duration
	// FullCoverageDays is the coverage_window_days at/above which coverage is
	// considered complete (smaller windows are partial).
	FullCoverageDays int
	Now              time.Time
}

// ResolveCompleteness classifies an account's coverage. It never returns
// CompletenessUnsupported (that is reserved for data-class-specific callers);
// account-level coverage is always one of the other five.
func ResolveCompleteness(in CompletenessInputs) Completeness {
	now := in.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	// In-flight hydration always wins: we are actively changing what we know.
	if in.InFlightHydration {
		return CompletenessHydrating
	}
	// Below candidate (or no row) means we have nothing worth presenting as
	// coverage.
	if !in.Exists || rank[in.State] < rank[StateCandidate] {
		return CompletenessNotTracked
	}
	// Tracked but never successfully hydrated: we may have live data but no
	// backfill, so coverage is partial at best.
	if in.LastSuccessfulHydrationAt == nil {
		return CompletenessPartial
	}
	if in.StaleAfter > 0 && now.Sub(*in.LastSuccessfulHydrationAt) > in.StaleAfter {
		return CompletenessStale
	}
	if in.CoverageWindowDays != nil && in.FullCoverageDays > 0 && *in.CoverageWindowDays < in.FullCoverageDays {
		return CompletenessPartial
	}
	return CompletenessComplete
}
