package account

import (
	"testing"
	"time"
)

func ptrTime(t time.Time) *time.Time { return &t }
func ptrInt(i int) *int              { return &i }

func TestResolveCompleteness(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	const staleAfter = 7 * 24 * time.Hour
	const fullDays = 90

	cases := []struct {
		name string
		in   CompletenessInputs
		want Completeness
	}{
		{
			name: "in-flight hydration wins over everything",
			in: CompletenessInputs{
				State:                     StateTracked,
				Exists:                    true,
				InFlightHydration:         true,
				LastSuccessfulHydrationAt: ptrTime(now),
				CoverageWindowDays:        ptrInt(90),
				StaleAfter:                staleAfter,
				FullCoverageDays:          fullDays,
				Now:                       now,
			},
			want: CompletenessHydrating,
		},
		{
			name: "no row is not_tracked",
			in:   CompletenessInputs{State: StateUnknown, Exists: false, Now: now},
			want: CompletenessNotTracked,
		},
		{
			name: "below candidate is not_tracked",
			in:   CompletenessInputs{State: StateObserved, Exists: true, Now: now},
			want: CompletenessNotTracked,
		},
		{
			name: "tracked but never hydrated is partial",
			in: CompletenessInputs{
				State:            StateTracked,
				Exists:           true,
				StaleAfter:       staleAfter,
				FullCoverageDays: fullDays,
				Now:              now,
			},
			want: CompletenessPartial,
		},
		{
			name: "stale when last successful hydration is old",
			in: CompletenessInputs{
				State:                     StateTracked,
				Exists:                    true,
				LastSuccessfulHydrationAt: ptrTime(now.Add(-8 * 24 * time.Hour)),
				CoverageWindowDays:        ptrInt(90),
				StaleAfter:                staleAfter,
				FullCoverageDays:          fullDays,
				Now:                       now,
			},
			want: CompletenessStale,
		},
		{
			name: "partial when coverage window below full",
			in: CompletenessInputs{
				State:                     StateTracked,
				Exists:                    true,
				LastSuccessfulHydrationAt: ptrTime(now.Add(-1 * time.Hour)),
				CoverageWindowDays:        ptrInt(30),
				StaleAfter:                staleAfter,
				FullCoverageDays:          fullDays,
				Now:                       now,
			},
			want: CompletenessPartial,
		},
		{
			name: "complete when fresh and full coverage",
			in: CompletenessInputs{
				State:                     StateTracked,
				Exists:                    true,
				LastSuccessfulHydrationAt: ptrTime(now.Add(-1 * time.Hour)),
				CoverageWindowDays:        ptrInt(90),
				StaleAfter:                staleAfter,
				FullCoverageDays:          fullDays,
				Now:                       now,
			},
			want: CompletenessComplete,
		},
		{
			name: "candidate counts as tracked-enough to be classified (partial when unhydrated)",
			in: CompletenessInputs{
				State:            StateCandidate,
				Exists:           true,
				StaleAfter:       staleAfter,
				FullCoverageDays: fullDays,
				Now:              now,
			},
			want: CompletenessPartial,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ResolveCompleteness(tc.in); got != tc.want {
				t.Fatalf("ResolveCompleteness(%s) = %q, want %q", tc.name, got, tc.want)
			}
		})
	}
}

func TestResolveCompletenessDefaultsNow(t *testing.T) {
	t.Parallel()
	// Zero Now should default to time.Now and still classify a fresh full-coverage
	// account as complete.
	got := ResolveCompleteness(CompletenessInputs{
		State:                     StateTracked,
		Exists:                    true,
		LastSuccessfulHydrationAt: ptrTime(time.Now().Add(-time.Minute)),
		CoverageWindowDays:        ptrInt(90),
		StaleAfter:                7 * 24 * time.Hour,
		FullCoverageDays:          90,
	})
	if got != CompletenessComplete {
		t.Fatalf("expected complete with zero Now, got %q", got)
	}
}
