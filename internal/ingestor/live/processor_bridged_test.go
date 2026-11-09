package live

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
	"github.com/xdzczk/nostrmash/internal/nostr"
)

// A bridged event is a valid Nostr event, so the only thing standing between
// it and the events table is the drop. These tests pin the drop's contract:
// nothing is stored, the author is not observed, the checkpoint still
// advances, and the switch can turn it off.
func TestProcessorHandle_DropsBridgedEvents(t *testing.T) {
	t.Parallel()

	fake := &fakeStore{}
	checkpoints := &checkpointRecorder{}
	observed := &observationRecorder{}
	processor, err := NewProcessor(silentLogger(), fake, nostr.Options{})
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	processor.SetCheckpointWriter(checkpoints)
	processor.SetObservationSink(observed)
	processor.SetDropBridged(true)

	bridged := signedEventPayload(t, 1, 1700000100, [][]string{
		{"proxy", "https://example.social/users/alice/statuses/1", "activitypub"},
	})
	if err := processor.Handle(context.Background(), "wss://relay.one", bridged); err != nil {
		t.Fatalf("bridged handle: %v", err)
	}
	if fake.canonicalWrites != 0 {
		t.Fatalf("bridged event must not be stored, got %d writes", fake.canonicalWrites)
	}
	if len(fake.invalidWrites) != 0 {
		t.Fatalf("bridged event is valid Nostr and must not be quarantined, got %d", len(fake.invalidWrites))
	}
	if observed.count != 0 {
		t.Fatalf("bridged author must not be observed, got %d", observed.count)
	}
	if checkpoints.calls != 1 || checkpoints.maxCreatedAt != 1700000100 {
		t.Fatalf("checkpoint must advance past the dropped event: %+v", checkpoints)
	}
	if got := processor.Snapshot(); got.Gated != 1 || got.Valid != 0 {
		t.Fatalf("unexpected counters: %+v", got)
	}

	// A bridged kind-0 is dropped too, so bridged accounts never get a profile.
	profile := signedEventPayload(t, 0, 1700000101, [][]string{
		{"proxy", "https://example.social/users/alice", "activitypub"},
	})
	if err := processor.Handle(context.Background(), "wss://relay.one", profile); err != nil {
		t.Fatalf("bridged profile handle: %v", err)
	}
	if fake.canonicalWrites != 0 {
		t.Fatalf("bridged kind-0 must not be stored, got %d writes", fake.canonicalWrites)
	}

	native := signedEventPayload(t, 1, 1700000102, [][]string{{"client", "nostrmash"}})
	if err := processor.Handle(context.Background(), "wss://relay.one", native); err != nil {
		t.Fatalf("native handle: %v", err)
	}
	if fake.canonicalWrites != 1 {
		t.Fatalf("native event must be stored, got %d writes", fake.canonicalWrites)
	}
	if observed.count != 1 {
		t.Fatalf("native author must be observed once, got %d", observed.count)
	}
}

func TestProcessorHandle_BridgedDropDisabled(t *testing.T) {
	t.Parallel()

	fake := &fakeStore{}
	processor, err := NewProcessor(silentLogger(), fake, nostr.Options{})
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	processor.SetDropBridged(false)

	bridged := signedEventPayload(t, 1, 1700000100, [][]string{
		{"proxy", "https://example.social/users/alice/statuses/1", "activitypub"},
	})
	if err := processor.Handle(context.Background(), "wss://relay.one", bridged); err != nil {
		t.Fatalf("bridged handle: %v", err)
	}
	if fake.canonicalWrites != 1 {
		t.Fatalf("with the drop off the bridged event is stored, got %d writes", fake.canonicalWrites)
	}
}

type observationRecorder struct{ count int }

func (o *observationRecorder) Observe(string) { o.count++ }

func signedEventPayload(t *testing.T, kind int, createdAt int64, tags [][]string) []byte {
	t.Helper()
	priv, err := btcec.NewPrivateKey()
	if err != nil {
		t.Fatalf("new private key: %v", err)
	}
	pubkey := hex.EncodeToString(schnorr.SerializePubKey(priv.PubKey()))
	content := "hello"
	canonical, err := json.Marshal([]any{0, pubkey, createdAt, kind, tags, content})
	if err != nil {
		t.Fatalf("marshal canonical event: %v", err)
	}
	sum := sha256.Sum256(canonical)
	sig, err := schnorr.Sign(priv, sum[:])
	if err != nil {
		t.Fatalf("sign event: %v", err)
	}
	payload, err := json.Marshal(map[string]any{
		"id":         hex.EncodeToString(sum[:]),
		"pubkey":     pubkey,
		"created_at": createdAt,
		"kind":       kind,
		"tags":       tags,
		"content":    content,
		"sig":        hex.EncodeToString(sig.Serialize()),
	})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return payload
}
