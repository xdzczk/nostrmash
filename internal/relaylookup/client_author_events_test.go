package relaylookup

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
)

func TestFetchEventsByAuthor_FiltersAuthorKindsAndDedupes(t *testing.T) {
	targetPriv, err := btcec.NewPrivateKey()
	if err != nil {
		t.Fatalf("new target private key: %v", err)
	}
	targetPubkey := hex.EncodeToString(schnorr.SerializePubKey(targetPriv.PubKey()))
	otherPriv, err := btcec.NewPrivateKey()
	if err != nil {
		t.Fatalf("new other private key: %v", err)
	}

	note := buildSignedEvent(t, targetPriv, 1700000100, 1, "note from target")
	article := buildSignedEvent(t, targetPriv, 1700000090, 30023, "article from target")
	wrongKind := buildSignedEvent(t, targetPriv, 1700000080, 7, "+")
	wrongAuthor := buildSignedEvent(t, otherPriv, 1700000070, 1, "note from other")

	relay := newProfileRelayServer(t, []map[string]any{note, article, wrongKind, wrongAuthor, note})
	defer relay.Close()

	client := NewClient([]string{wsURLForServer(relay)}, time.Second, 1)
	got, err := client.FetchEventsByAuthor(context.Background(), targetPubkey, []int{1, 30023}, 10)
	if err != nil {
		t.Fatalf("fetch events by author: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected two validated author events, got %d", len(got))
	}
	gotIDs := map[string]bool{}
	for _, raw := range got {
		evt, _, validateErr := validateFetchedEvent(raw)
		if validateErr != nil {
			t.Fatalf("returned event failed validation: %v", validateErr)
		}
		gotIDs[evt.ID] = true
	}
	if !gotIDs[note["id"].(string)] || !gotIDs[article["id"].(string)] {
		t.Fatalf("expected note and article ids, got %v", gotIDs)
	}
}

func TestFetchEventsByAuthor_EmptyInputsReturnNoEvents(t *testing.T) {
	client := NewClient(nil, time.Second, 1)
	got, err := client.FetchEventsByAuthor(context.Background(), "any", []int{1}, 10)
	if err != nil {
		t.Fatalf("disabled client must not error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no events from disabled client, got %d", len(got))
	}

	enabled := NewClient([]string{"ws://127.0.0.1:1"}, 100*time.Millisecond, 1)
	got, err = enabled.FetchEventsByAuthor(context.Background(), "   ", []int{1}, 10)
	if err != nil {
		t.Fatalf("blank pubkey must not error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no events for blank pubkey, got %d", len(got))
	}
}
