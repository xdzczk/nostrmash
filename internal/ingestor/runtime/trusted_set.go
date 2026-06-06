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
	}
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
	t.mu.Lock()
	t.members = members
	t.loaded = true
	t.lastRefreshAt = time.Now().UTC()
	t.mu.Unlock()
	return nil
}
