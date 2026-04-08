package query

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/xdzczk/nostrmash/internal/store"
)

func TestGetProfilesNormalizesInputOrder(t *testing.T) {
	t.Parallel()
	svc := NewProfileService(fakeProfileReader{
		getProfilesByPubkeysFn: func(_ context.Context, pubkeys []string) (map[string]Profile, error) {
			if len(pubkeys) != 2 || pubkeys[0] != "pk1" || pubkeys[1] != "pk2" {
				t.Fatalf("unexpected normalized pubkeys: %#v", pubkeys)
			}
			return map[string]Profile{
				"pk1": {Pubkey: "pk1"},
			}, nil
		},
	})

	out, err := svc.GetProfiles(context.Background(), []string{" pk1 ", "", "pk2", "pk1"})
	if err != nil {
		t.Fatalf("GetProfiles returned error: %v", err)
	}
	if len(out.Profiles) != 1 || out.Profiles[0].Pubkey != "pk1" {
		t.Fatalf("unexpected profiles result: %#v", out.Profiles)
	}
	if len(out.MissingPubkeys) != 1 || out.MissingPubkeys[0] != "pk2" {
		t.Fatalf("unexpected missing pubkeys: %#v", out.MissingPubkeys)
	}
}

func TestGetContactListReturnsQueryModel(t *testing.T) {
	t.Parallel()
	svc := NewService(fakeReader{
		getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
			return nil, store.ErrNotFound
		},
	})
	svc.reader = readerWithContactRelay{
		Reader: svc.reader,
		getContactListByPubkeyFn: func(context.Context, string) (ContactList, error) {
			return ContactList{
				Pubkey:          "pk-1",
				EventID:         "evt-1",
				CreatedAt:       123,
				DerivationVer:   7,
				ContactsJSONRaw: json.RawMessage(`["pk-2"]`),
			}, nil
		},
	}
	out, err := svc.GetContactList(context.Background(), "pk-1")
	if err != nil {
		t.Fatalf("GetContactList returned error: %v", err)
	}
	if out.Pubkey != "pk-1" || out.EventID != "evt-1" || out.CreatedAt != 123 || out.DerivationVer != 7 {
		t.Fatalf("unexpected contact list projection: %#v", out)
	}
	if string(out.ContactsJSONRaw) != `["pk-2"]` {
		t.Fatalf("unexpected contacts payload: %s", string(out.ContactsJSONRaw))
	}
}

func TestGetRelayListReturnsQueryModel(t *testing.T) {
	t.Parallel()
	svc := NewService(fakeReader{
		getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
			return nil, store.ErrNotFound
		},
	})
	svc.reader = readerWithContactRelay{
		Reader: svc.reader,
		getRelayListByPubkeyFn: func(context.Context, string) (RelayList, error) {
			return RelayList{
				Pubkey:        "pk-1",
				EventID:       "evt-2",
				CreatedAt:     456,
				DerivationVer: 8,
				RelaysJSONRaw: json.RawMessage(`[{"url":"wss://relay.example"}]`),
			}, nil
		},
	}
	out, err := svc.GetRelayList(context.Background(), "pk-1")
	if err != nil {
		t.Fatalf("GetRelayList returned error: %v", err)
	}
	if out.Pubkey != "pk-1" || out.EventID != "evt-2" || out.CreatedAt != 456 || out.DerivationVer != 8 {
		t.Fatalf("unexpected relay list projection: %#v", out)
	}
	if string(out.RelaysJSONRaw) != `[{"url":"wss://relay.example"}]` {
		t.Fatalf("unexpected relays payload: %s", string(out.RelaysJSONRaw))
	}
}

type readerWithContactRelay struct {
	Reader
	getContactListByPubkeyFn func(context.Context, string) (ContactList, error)
	getRelayListByPubkeyFn   func(context.Context, string) (RelayList, error)
}

func (r readerWithContactRelay) GetContactListByPubkey(ctx context.Context, pubkey string) (ContactList, error) {
	if r.getContactListByPubkeyFn == nil {
		return ContactList{}, store.ErrNotFound
	}
	return r.getContactListByPubkeyFn(ctx, pubkey)
}

func (r readerWithContactRelay) GetRelayListByPubkey(ctx context.Context, pubkey string) (RelayList, error) {
	if r.getRelayListByPubkeyFn == nil {
		return RelayList{}, store.ErrNotFound
	}
	return r.getRelayListByPubkeyFn(ctx, pubkey)
}
