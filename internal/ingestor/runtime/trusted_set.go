package runtime

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// trustedAuthorLoader loads the trusted-author pubkey set from storage.
// Satisfied by *store.PostgresStore.
type trustedAuthorLoader interface {
	LoadTrustedSnapshotPubkeys(ctx context.Context, maxHops int) ([]string, error)
}

// accountStateLoader optionally augments the trusted set with account-state
// signals: tracked/strategic accounts are unioned into the accept set (so the
// gate accepts on-demand-hydrated authors) and blocked accounts are recorded
// for a hard drop. Satisfied by *store.PostgresStore. The trusted-set refresh
// degrades gracefully when the loader does not implement this (e.g. test
// fakes).
type accountStateLoader interface {
	LoadIngestAcceptPubkeys(ctx context.Context) ([]string, error)
	LoadBlockedPubkeys(ctx context.Context) ([]string, error)
}

// TrustedAuthorSet is an in-memory, periodically-refreshed view of the
// trusted-author pubkeys the ingest gate enforces against. Holding it in
// memory avoids a per-event DB lookup on the live hot path.
//
// Failure semantics (intentional):
//   - The last successfully-loaded set is retained across refresh failures, so
//     a transient DB blip does not collapse the trusted set.
//   - Loaded() reports whether ANY load has ever succeeded. The gate uses this
//     for fail-closed behavior in trusted_only mode: a never-loaded set rejects
//     kind 1 rather than silently accepting or dropping everything.
//   - LastRefreshAt() tracks the last SUCCESSFUL load for staleness metrics.
type TrustedAuthorSet struct {
	maxHops int

	mu            sync.RWMutex
	members       map[string]struct{}
	blocked       map[string]struct{}
	loaded        bool
	lastRefreshAt time.Time
}

// NewTrustedAuthorSet returns an empty, not-yet-loaded trusted set bounded to
// authors within maxHops of a seed.
func NewTrustedAuthorSet(maxHops int) *TrustedAuthorSet {
	if maxHops < 0 {
		maxHops = 0
	}
	return &TrustedAuthorSet{
		maxHops: maxHops,
		members: make(map[string]struct{}),
		blocked: make(map[string]struct{}),
	}
}

// Blocked reports whether pubkey is explicitly blocked. Blocked authors have
// all their events dropped at ingest regardless of gate mode.
func (t *TrustedAuthorSet) Blocked(pubkey string) bool {
	if t == nil {
		return false
	}
	pubkey = strings.ToLower(strings.TrimSpace(pubkey))
	if pubkey == "" {
		return false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.blocked[pubkey]
	return ok
}

// Contains reports whether pubkey is in the current trusted set. The pubkey is
// lowercased before lookup to match the stored, normalized membership.
func (t *TrustedAuthorSet) Contains(pubkey string) bool {
	if t == nil {
		return false
	}
	pubkey = strings.ToLower(strings.TrimSpace(pubkey))
	if pubkey == "" {
		return false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.members[pubkey]
	return ok
}

// Loaded reports whether at least one successful load has occurred.
func (t *TrustedAuthorSet) Loaded() bool {
	if t == nil {
		return false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.loaded
}

// Size returns the number of trusted pubkeys currently held.
func (t *TrustedAuthorSet) Size() int {
	if t == nil {
		return 0
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.members)
}

// LastRefreshAt returns the time of the last successful load, or the zero time
// if the set has never loaded.
func (t *TrustedAuthorSet) LastRefreshAt() time.Time {
	if t == nil {
		return time.Time{}
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.lastRefreshAt
}

// Refresh loads the trusted set from the loader. On success it atomically swaps
// in the new membership, marks the set loaded, and records the refresh time. On
// failure it keeps the previous (last-good) membership and returns the error so
// the caller can record a stale-snapshot metric.
func (t *TrustedAuthorSet) Refresh(ctx context.Context, loader trustedAuthorLoader) error {
	if t == nil {
		return fmt.Errorf("trusted author set is nil")
	}
	if loader == nil {
		return fmt.Errorf("trusted author loader is nil")
	}
	pubkeys, err := loader.LoadTrustedSnapshotPubkeys(ctx, t.maxHops)
	if err != nil {
		return err
	}
	members := make(map[string]struct{}, len(pubkeys))
	for _, p := range pubkeys {
		p = strings.ToLower(strings.TrimSpace(p))
		if p == "" {
			continue
		}
		members[p] = struct{}{}
	}

	// Best-effort augmentation from account states: union tracked/strategic
	// accounts into the accept set and record blocked accounts. Failures here
	// must not collapse the (already-loaded) trusted set, so they are logged via
	// the returned error path only when the trusted load itself failed; here we
	// simply skip augmentation on error.
	blocked := make(map[string]struct{})
	if asl, ok := loader.(accountStateLoader); ok {
		if accept, acceptErr := asl.LoadIngestAcceptPubkeys(ctx); acceptErr == nil {
			for _, p := range accept {
				p = strings.ToLower(strings.TrimSpace(p))
				if p != "" {
					members[p] = struct{}{}
				}
			}
		}
		if blk, blockedErr := asl.LoadBlockedPubkeys(ctx); blockedErr == nil {
			for _, p := range blk {
				p = strings.ToLower(strings.TrimSpace(p))
				if p != "" {
					blocked[p] = struct{}{}
				}
			}
		}
	}

	t.mu.Lock()
	t.members = members
	t.blocked = blocked
	t.loaded = true
	t.lastRefreshAt = time.Now().UTC()
	t.mu.Unlock()
	return nil
}
