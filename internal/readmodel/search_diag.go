package readmodel

import (
	"context"
	"sync"
	"time"
)

// ProfileSearchSignal is the ranking and response payload for one pubkey.
// Follower and activity fields come from projections; Bot is the kind-0 flag.
type ProfileSearchSignal struct {
	Pubkey           string
	Name             string
	DisplayName      string
	NIP05            string
	FollowerCount    int64
	RecentPostCount  int64
	RecentActivityAt *int64
	Bot              bool
}

// SearchSectionStat is one search sub-query's timing and corpus estimate.
type SearchSectionStat struct {
	Duration        time.Duration
	EstimatedTotal  int64
	TotalIsEstimate bool
	Error           string
}

// SearchDiagnosticsSnapshot is a mutex-free copy of SearchDiagnostics for
// logging and response fields. Handlers must not share the live bag by value.
type SearchDiagnosticsSnapshot struct {
	Notes    SearchSectionStat
	Profiles SearchSectionStat
	Hashtags SearchSectionStat
	Relays   SearchSectionStat
	Suggest  SearchSectionStat

	NoteEngineScore    map[string]float64
	ProfileSignals     map[string]ProfileSearchSignal
	HashtagFrequency   int64
	CorrectedQuery     string
	CorrectionApplied  bool
	CorrectionChecked  bool
	SurfaceErrors      map[string]string
	obviousMiss        bool
	obviousMissChecked bool
}

// ProfileSignal returns stored signals. The boolean is false when absent.
func (s SearchDiagnosticsSnapshot) ProfileSignal(pubkey string) (ProfileSearchSignal, bool) {
	if pubkey == "" {
		return ProfileSearchSignal{}, false
	}
	signal, ok := s.ProfileSignals[pubkey]
	return signal, ok
}

// SearchDiagnostics collects per-request search timings, engine scores, and
// profile signals. Handlers install it on the request context; Meilisearch
// and the query service record into the same value.
type SearchDiagnostics struct {
	mu sync.Mutex
	SearchDiagnosticsSnapshot
}

type searchDiagKey struct{}

// WithSearchDiagnostics returns a context that carries a fresh diagnostics bag.
func WithSearchDiagnostics(ctx context.Context) (context.Context, *SearchDiagnostics) {
	diag := &SearchDiagnostics{
		SearchDiagnosticsSnapshot: SearchDiagnosticsSnapshot{
			NoteEngineScore: map[string]float64{},
			ProfileSignals:  map[string]ProfileSearchSignal{},
			SurfaceErrors:   map[string]string{},
		},
	}
	return context.WithValue(ctx, searchDiagKey{}, diag), diag
}

// SearchDiagnosticsFrom returns the bag installed by WithSearchDiagnostics.
func SearchDiagnosticsFrom(ctx context.Context) *SearchDiagnostics {
	if ctx == nil {
		return nil
	}
	diag, _ := ctx.Value(searchDiagKey{}).(*SearchDiagnostics)
	return diag
}

// AddDuration accumulates sub-query time. Parallel sections each call it once.
func (d *SearchDiagnostics) AddDuration(section string, elapsed time.Duration) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	switch section {
	case "notes":
		d.Notes.Duration += elapsed
	case "profiles":
		d.Profiles.Duration += elapsed
	case "hashtags":
		d.Hashtags.Duration += elapsed
	case "relays":
		d.Relays.Duration += elapsed
	case "suggest":
		d.Suggest.Duration += elapsed
	}
}

// ObserveTotal keeps the largest corpus estimate seen for a section.
// A recency-window query reports a smaller hit count than the unfiltered
// query; the larger figure is the one clients should display.
func (d *SearchDiagnostics) ObserveTotal(section string, total int64, estimate bool) {
	if d == nil || total <= 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	stat := d.section(section)
	if stat == nil {
		return
	}
	if total > stat.EstimatedTotal {
		stat.EstimatedTotal = total
	}
	if estimate {
		stat.TotalIsEstimate = true
	}
}

// SetSectionError records a sub-query failure without discarding other sections.
func (d *SearchDiagnostics) SetSectionError(section string, err error) {
	if d == nil || err == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	msg := err.Error()
	if stat := d.section(section); stat != nil {
		stat.Error = msg
	}
	if d.SurfaceErrors == nil {
		d.SurfaceErrors = map[string]string{}
	}
	d.SurfaceErrors[section] = msg
}

// ObserveNoteScore stores a Meilisearch ranking score for one note id.
func (d *SearchDiagnostics) ObserveNoteScore(id string, score float64) {
	if d == nil || id == "" || score <= 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.NoteEngineScore == nil {
		d.NoteEngineScore = map[string]float64{}
	}
	if prev, ok := d.NoteEngineScore[id]; !ok || score > prev {
		d.NoteEngineScore[id] = score
	}
}

// NoteScore returns a previously observed engine score.
func (d *SearchDiagnostics) NoteScore(id string) float64 {
	if d == nil || id == "" {
		return 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.NoteEngineScore[id]
}

// PutProfileSignal stores ranking signals for a pubkey.
func (d *SearchDiagnostics) PutProfileSignal(signal ProfileSearchSignal) {
	if d == nil || signal.Pubkey == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.ProfileSignals == nil {
		d.ProfileSignals = map[string]ProfileSearchSignal{}
	}
	d.ProfileSignals[signal.Pubkey] = signal
}

// ProfileSignal returns stored signals. The boolean is false when absent.
func (d *SearchDiagnostics) ProfileSignal(pubkey string) (ProfileSearchSignal, bool) {
	if d == nil || pubkey == "" {
		return ProfileSearchSignal{}, false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	signal, ok := d.ProfileSignals[pubkey]
	return signal, ok
}

// SetHashtagFrequency records the document frequency of the raw query token.
func (d *SearchDiagnostics) SetHashtagFrequency(count int64) {
	if d == nil || count <= 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if count > d.HashtagFrequency {
		d.HashtagFrequency = count
	}
}

// HashtagDF returns the raw-token hashtag frequency observed so far.
func (d *SearchDiagnostics) HashtagDF() int64 {
	if d == nil {
		return 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.HashtagFrequency
}

// MarkCorrection records a spelling rewrite. A second call is ignored so a
// corrected query is not corrected again.
func (d *SearchDiagnostics) MarkCorrection(corrected string) bool {
	if d == nil {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.CorrectionChecked {
		return false
	}
	d.CorrectionChecked = true
	if corrected == "" {
		return false
	}
	d.CorrectedQuery = corrected
	d.CorrectionApplied = true
	return true
}

// SetObviousMiss records that the raw token has no postings and the search
// can return without a table scan. A second call is ignored.
func (d *SearchDiagnostics) SetObviousMiss(miss bool) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.obviousMissChecked {
		return
	}
	d.obviousMissChecked = true
	d.obviousMiss = miss
}

// ObviousMiss reports the short-circuit decision. The boolean is false until
// SetObviousMiss has run for this request.
func (d *SearchDiagnostics) ObviousMiss() (bool, bool) {
	if d == nil {
		return false, false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.obviousMiss, d.obviousMissChecked
}

// CorrectionAlreadyChecked reports whether this request already ran correction.
func (d *SearchDiagnostics) CorrectionAlreadyChecked() bool {
	if d == nil {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.CorrectionChecked
}

// Snapshot copies the diagnostics under the lock for logging and responses.
func (d *SearchDiagnostics) Snapshot() SearchDiagnosticsSnapshot {
	if d == nil {
		return SearchDiagnosticsSnapshot{}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	out := d.SearchDiagnosticsSnapshot
	out.NoteEngineScore = nil
	if len(d.SurfaceErrors) > 0 {
		out.SurfaceErrors = make(map[string]string, len(d.SurfaceErrors))
		for key, value := range d.SurfaceErrors {
			out.SurfaceErrors[key] = value
		}
	}
	if len(d.ProfileSignals) > 0 {
		out.ProfileSignals = make(map[string]ProfileSearchSignal, len(d.ProfileSignals))
		for key, value := range d.ProfileSignals {
			out.ProfileSignals[key] = value
		}
	}
	return out
}

func (d *SearchDiagnostics) section(name string) *SearchSectionStat {
	switch name {
	case "notes":
		return &d.Notes
	case "profiles":
		return &d.Profiles
	case "hashtags":
		return &d.Hashtags
	case "relays":
		return &d.Relays
	case "suggest":
		return &d.Suggest
	default:
		return nil
	}
}
