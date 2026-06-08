package account

import "testing"

func TestParseAndValid(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		in    string
		want  State
		valid bool
	}{
		"lower":      {"tracked", StateTracked, true},
		"mixed case": {"  Trusted ", StateTrusted, true},
		"blocked":    {"blocked", StateBlocked, true},
		"unknown":    {"unknown", StateUnknown, true},
		"bogus":      {"superstar", StateUnknown, false},
		"empty":      {"", StateUnknown, false},
	}
	for name, tc := range cases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got, ok := Parse(tc.in)
			if ok != tc.valid {
				t.Fatalf("Parse(%q) valid = %v, want %v", tc.in, ok, tc.valid)
			}
			if ok && got != tc.want {
				t.Fatalf("Parse(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestRankOrderingAndAtLeast(t *testing.T) {
	t.Parallel()
	// Promotion ordering, ascending.
	order := []State{StateUnknown, StateObserved, StateCandidate, StateMeaningful, StateTrusted, StateTracked, StateStrategic}
	for i := 1; i < len(order); i++ {
		if !(Rank(order[i-1]) < Rank(order[i])) {
			t.Fatalf("expected rank(%s) < rank(%s)", order[i-1], order[i])
		}
		if !AtLeast(order[i], order[i-1]) {
			t.Fatalf("expected AtLeast(%s, %s)", order[i], order[i-1])
		}
	}
	// Blocked is a sentinel below unknown and never satisfies AtLeast for a real state.
	if Rank(StateBlocked) >= Rank(StateUnknown) {
		t.Fatal("blocked must rank below unknown")
	}
	if AtLeast(StateBlocked, StateCandidate) {
		t.Fatal("blocked must never be AtLeast candidate")
	}
}

func TestIngestAccepted(t *testing.T) {
	t.Parallel()
	accepted := map[State]bool{
		StateUnknown:    false,
		StateObserved:   false,
		StateCandidate:  false,
		StateMeaningful: false,
		StateTrusted:    false, // trusted handled by the trust graph, not the account accept-set
		StateTracked:    true,
		StateStrategic:  true,
		StateBlocked:    false,
	}
	for state, want := range accepted {
		if got := IngestAccepted(state); got != want {
			t.Fatalf("IngestAccepted(%s) = %v, want %v", state, got, want)
		}
	}
}

func TestResolve(t *testing.T) {
	t.Parallel()
	p := DefaultParams
	cases := []struct {
		name string
		sig  Signals
		want State
	}{
		{
			name: "trusted within hops",
			sig:  Signals{TrustHops: 2},
			want: StateTrusted,
		},
		{
			name: "trust hops zero is trusted",
			sig:  Signals{TrustHops: 0},
			want: StateTrusted,
		},
		{
			name: "beyond trusted hops not trusted by that path",
			sig:  Signals{TrustHops: 3},
			want: StateUnknown,
		},
		{
			name: "meaningful: profile + notes + engaged-by-trusted",
			sig:  Signals{TrustHops: -1, HasProfileMetadata: true, NoteCount: 12, EngagedByTrustedCount: 4},
			want: StateMeaningful,
		},
		{
			name: "candidate via engaged-by-trusted",
			sig:  Signals{TrustHops: -1, EngagedByTrustedCount: 1},
			want: StateCandidate,
		},
		{
			name: "candidate via profile + observation",
			sig:  Signals{TrustHops: -1, HasProfileMetadata: true, ObservedCount: 5},
			want: StateCandidate,
		},
		{
			name: "observed",
			sig:  Signals{TrustHops: -1, ObservedCount: 2},
			want: StateObserved,
		},
		{
			name: "unknown when seen once",
			sig:  Signals{TrustHops: -1, ObservedCount: 1},
			want: StateUnknown,
		},
		{
			name: "trusted wins over meaningful signals",
			sig:  Signals{TrustHops: 1, HasProfileMetadata: true, NoteCount: 100, EngagedByTrustedCount: 50},
			want: StateTrusted,
		},
		{
			name: "profile + notes but no engagement/observation falls through to unknown",
			sig:  Signals{TrustHops: -1, HasProfileMetadata: true, NoteCount: 12, EngagedByTrustedCount: 0, ObservedCount: 0},
			want: StateUnknown,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := Resolve(tc.sig, p); got != tc.want {
				t.Fatalf("Resolve(%+v) = %q, want %q", tc.sig, got, tc.want)
			}
		})
	}
}

func TestResolveNeverReturnsPromotionOnlyStates(t *testing.T) {
	t.Parallel()
	p := DefaultParams
	// Exhaustively poke a range of signals; Resolve must never emit
	// tracked/strategic/blocked (those are promotion/override only).
	for hops := -1; hops <= 5; hops++ {
		for _, obs := range []int64{0, 1, 5, 50} {
			for _, notes := range []int{0, 10, 100} {
				for _, eng := range []int{0, 1, 3, 10} {
					for _, prof := range []bool{false, true} {
						got := Resolve(Signals{
							TrustHops:             hops,
							ObservedCount:         obs,
							NoteCount:             notes,
							EngagedByTrustedCount: eng,
							HasProfileMetadata:    prof,
						}, p)
						switch got {
						case StateTracked, StateStrategic, StateBlocked:
							t.Fatalf("Resolve emitted promotion-only state %q", got)
						}
					}
				}
			}
		}
	}
}

func TestEffectiveState(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		derived  State
		override State
		tracked  bool
		want     State
	}{
		{"no override, not tracked", StateObserved, "", false, StateObserved},
		{"override wins over derived", StateObserved, StateStrategic, false, StateStrategic},
		{"blocked override wins even when tracked", StateTrusted, StateBlocked, true, StateBlocked},
		{"tracked floors derived up to tracked", StateCandidate, "", true, StateTracked},
		{"tracked does not lower a higher derived", StateStrategic, "", true, StateStrategic},
		{"trusted derived stays below tracked floor", StateTrusted, "", true, StateTracked},
		{"invalid derived treated as unknown", State("bogus"), "", false, StateUnknown},
		{"tracked from unknown derived", StateUnknown, "", true, StateTracked},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := EffectiveState(tc.derived, tc.override, tc.tracked); got != tc.want {
				t.Fatalf("EffectiveState(%q, %q, %v) = %q, want %q", tc.derived, tc.override, tc.tracked, got, tc.want)
			}
		})
	}
}
