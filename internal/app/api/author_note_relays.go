package appapi

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/nostr"
	"github.com/xdzczk/nostrmash/internal/relaylookup"
	"github.com/xdzczk/nostrmash/internal/relayregistry"
	"github.com/xdzczk/nostrmash/internal/store"
)

// authorNoteRelaysOption is nil when profile fallback is off, so a request
// never looks up write relays it cannot dial.
func authorNoteRelaysOption(
	eventStore *store.PostgresStore,
	pool *pgxpool.Pool,
	client *relaylookup.Client,
	disabled []string,
) func(context.Context, string) ([]string, error) {
	if eventStore == nil || pool == nil || client == nil {
		return nil
	}
	return authorNoteRelays(eventStore, relayregistry.NewStore(pool), client.EventRelays, disabled)
}

// authorNoteRelays returns up to three write relays for pubkey that are not
// already in the profile fallback set. Kind 10002 wins over the legacy kind-3
// relay map. A relay is returned only when the registry's latest probe
// reached EOSE and the operator has not disabled it.
func authorNoteRelays(
	eventStore *store.PostgresStore,
	registry *relayregistry.Store,
	base func() []string,
	disabled []string,
) func(context.Context, string) ([]string, error) {
	return func(ctx context.Context, pubkey string) ([]string, error) {
		kind10002, err := eventStore.LatestKind10002Raw(ctx, pubkey)
		if err != nil {
			return nil, err
		}
		kind3, err := eventStore.LatestKind3Raw(ctx, pubkey)
		if err != nil {
			return nil, err
		}
		candidates := nostr.NormalizedAuthorWriteRelays(kind10002, kind3, true)
		if len(candidates) == 0 {
			return nil, nil
		}
		healthy, err := registry.ProbeOKRelaySet(ctx, candidates)
		if err != nil {
			return nil, err
		}
		var current []string
		if base != nil {
			current = base()
		}
		extras := nostr.PickExtraWriteRelays(candidates, current, disabled, healthy, true, nostr.MaxExtraAuthorWriteRelays)
		return relaylookup.WithoutDirectoryRelays(extras), nil
	}
}
