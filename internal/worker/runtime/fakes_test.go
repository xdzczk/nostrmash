package runtime

import (
	"context"
	"sync"
	"time"

	"github.com/xdzczk/nostrmash/internal/jobs"
	storeaccount "github.com/xdzczk/nostrmash/internal/store/account"
)

// recordingLogger is a Logger that captures emitted keys so tests can assert
// which branch a loop took without a real slog sink.
type recordingLogger struct {
	mu   sync.Mutex
	info []string
	errs []string
}

func (l *recordingLogger) Info(msg string, _ ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.info = append(l.info, msg)
}

func (l *recordingLogger) Error(msg string, _ ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errs = append(l.errs, msg)
}

func (l *recordingLogger) sawInfo(msg string) bool  { return l.saw(l.info, msg) }
func (l *recordingLogger) sawError(msg string) bool { return l.saw(l.errs, msg) }

func (l *recordingLogger) saw(list []string, msg string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, m := range list {
		if m == msg {
			return true
		}
	}
	return false
}

// fakeGovernorStore records calls and returns programmable errors/counts.
type fakeGovernorStore struct {
	dbBytes       int64
	dbBytesErr    error
	upserts       []upsertCall
	upsertErr     error
	drainCalls    map[string]int
	engagementErr error
}

type upsertCall struct {
	level         int
	ratio         float64
	databaseBytes int64
	capacityBytes int64
}

func newFakeGovernorStore() *fakeGovernorStore {
	return &fakeGovernorStore{drainCalls: map[string]int{}}
}

func (f *fakeGovernorStore) GetDatabaseBytes(context.Context) (int64, error) {
	return f.dbBytes, f.dbBytesErr
}

func (f *fakeGovernorStore) UpsertStoragePressureState(_ context.Context, level int, ratio float64, databaseBytes, capacityBytes int64) error {
	f.upserts = append(f.upserts, upsertCall{level, ratio, databaseBytes, capacityBytes})
	return f.upsertErr
}

func (f *fakeGovernorStore) PurgeExpiredEngagementEvents(context.Context, time.Time, time.Time, int) (int64, error) {
	f.drainCalls["engagement"]++
	return 1, f.engagementErr
}

func (f *fakeGovernorStore) PurgeSupersededReplaceableEvents(context.Context, time.Time, time.Time, int) (int64, error) {
	f.drainCalls["replaceable"]++
	return 1, nil
}

func (f *fakeGovernorStore) PurgeProcessedDeletionEvents(context.Context, time.Time, time.Time, int) (int64, error) {
	f.drainCalls["deletion"]++
	return 1, nil
}

func (f *fakeGovernorStore) PurgeUntrustedAuthorEvents(context.Context, time.Time, time.Time, int) (int64, error) {
	f.drainCalls["untrusted"]++
	return 1, nil
}

func (f *fakeGovernorStore) PruneAuthorRecentEvents(context.Context, time.Time, int, int, int) (int64, error) {
	f.drainCalls["author_recent"]++
	return 1, nil
}

func (f *fakeGovernorStore) PurgeStaleEventRelays(context.Context, time.Time, int) (int64, error) {
	f.drainCalls["event_relays"]++
	return 1, nil
}

func (f *fakeGovernorStore) PruneFilteredEventTags(context.Context, int) (int64, error) {
	f.drainCalls["event_tags"]++
	return 1, nil
}

type fakeGovernorQueue struct {
	purgeCalls int
	purgeErr   error
}

func (f *fakeGovernorQueue) PurgeTerminalJobs(context.Context, time.Time, time.Time, int) (int64, error) {
	f.purgeCalls++
	return 2, f.purgeErr
}

// fakeAccountStateStore drives the recompute loop without Postgres.
type fakeAccountStateStore struct {
	signals    []storeaccount.AccountSignalRow
	listErr    error
	applied    []appliedState
	applyErr   error
	counts     map[string]int64
	countErr   error
	purgeCount int64
	purgeErr   error
}

type appliedState struct {
	pubkey    string
	fromState string
	derived   string
	effective string
}

func (f *fakeAccountStateStore) ListAccountSignalsForRecompute(context.Context, int, time.Time) ([]storeaccount.AccountSignalRow, error) {
	return f.signals, f.listErr
}

func (f *fakeAccountStateStore) ApplyAccountState(_ context.Context, pubkey, fromState, derived, effective, _, _ string) error {
	if f.applyErr != nil {
		return f.applyErr
	}
	f.applied = append(f.applied, appliedState{pubkey, fromState, derived, effective})
	return nil
}

func (f *fakeAccountStateStore) CountAccountStates(context.Context) (map[string]int64, error) {
	return f.counts, f.countErr
}

func (f *fakeAccountStateStore) PurgeAccountStateTransitionsOlderThan(context.Context, time.Time, int) (int64, error) {
	return f.purgeCount, f.purgeErr
}

// fakeInvalidEventRetentionStore records purge/trim calls.
type fakeInvalidEventRetentionStore struct {
	purged    int64
	purgeErr  error
	trimmed   int64
	trimErr   error
	purgeSeen int
	trimSeen  int
}

func (f *fakeInvalidEventRetentionStore) PurgeInvalidEventsOlderThan(context.Context, time.Time, int) (int64, error) {
	f.purgeSeen++
	return f.purged, f.purgeErr
}

func (f *fakeInvalidEventRetentionStore) TrimInvalidEventPayloadsOlderThan(context.Context, time.Time, int) (int64, error) {
	f.trimSeen++
	return f.trimmed, f.trimErr
}

// fakeQueue implements the runtime Queue interface for loop tests. Only the
// methods exercised by the tested loops do anything useful.
type fakeQueue struct {
	recoverResult jobs.RecoveryResult
	recoverErr    error
	recoverCalls  int
}

func (f *fakeQueue) ClaimAvailableForPool(context.Context, string, string, int) ([]jobs.Job, error) {
	return nil, nil
}
func (f *fakeQueue) CompleteJob(context.Context, int64, string) error { return nil }
func (f *fakeQueue) FailJob(context.Context, int64, string, string, time.Duration) (jobs.FailureResult, error) {
	return jobs.FailureResult{}, nil
}
func (f *fakeQueue) RecoverStaleRunningJobs(context.Context, string, time.Time, int) (jobs.RecoveryResult, error) {
	f.recoverCalls++
	return f.recoverResult, f.recoverErr
}
func (f *fakeQueue) PurgeTerminalJobs(context.Context, time.Time, time.Time, int) (int64, error) {
	return 0, nil
}
