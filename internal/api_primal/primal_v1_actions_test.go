package api_primal

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/xdzczk/nostrmash/internal/store"
)

func TestEventActionsReturnAuthorMetadata(t *testing.T) {
	actor := strings.Repeat("ab", 32)
	gateway := mustNewWSGateway(t, fakeEventReader{
		listEventActionPubkeysFn: func(_ context.Context, eventID string, kind int, limit int) ([]string, error) {
			if eventID != "note1" || kind != 7 || limit != 20 {
				t.Fatalf("query %s kind %d limit %d", eventID, kind, limit)
			}
			return []string{actor}, nil
		},
		getProfilesByBatch: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			out := map[string]store.ProfileProjection{}
			for _, pubkey := range pubkeys {
				out[pubkey] = store.ProfileProjection{
					Pubkey:      pubkey,
					ProfileJSON: json.RawMessage(`{"name":"ada","display_name":"Ada"}`),
				}
			}
			return out, nil
		},
	}, WSGatewayOptions{Dialect: WSDialectPrimalV1})

	events, err := gateway.primalV1EventActions(context.Background(), map[string]any{
		"event_id": "note1",
		"kind":     7,
		"limit":    20,
	})
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, event := range events {
		item, ok := event.(map[string]any)
		if !ok {
			continue
		}
		kind, _ := kindOf(item["kind"])
		if kind != 0 || item["pubkey"] != actor {
			continue
		}
		if !strings.Contains(stringValue(item["content"]), `"name":"ada"`) {
			t.Fatalf("metadata %#v", item)
		}
		found = true
	}
	if !found {
		t.Fatalf("missing author metadata: %#v", events)
	}
}

func TestZapPageNamesTheZapper(t *testing.T) {
	zapper := strings.Repeat("cd", 32)
	service := strings.Repeat("ef", 32)
	receiver := strings.Repeat("11", 32)
	receipt, err := json.Marshal(map[string]any{
		"id":         "receipt1",
		"pubkey":     service,
		"kind":       9735,
		"created_at": 10,
		"tags": []any{
			[]any{"description", `{"id":"req","pubkey":"` + zapper + `","kind":9734,"content":"nice","tags":[]}`},
			[]any{"p", receiver},
			[]any{"e", "note1"},
			[]any{"amount", "21000"},
		},
		"content": "",
		"sig":     "",
	})
	if err != nil {
		t.Fatal(err)
	}
	wrapper, err := json.Marshal(map[string]any{
		"event_id":        "receipt1",
		"sender_pubkey":   service,
		"receiver_pubkey": receiver,
		"target_event_id": "note1",
		"sats":            21,
		"created_at":      10,
		"event":           json.RawMessage(receipt),
	})
	if err != nil {
		t.Fatal(err)
	}
	gateway := mustNewWSGateway(t, fakeEventReader{
		getEventZapsFn: func(context.Context, string, int) ([]json.RawMessage, error) {
			return []json.RawMessage{wrapper}, nil
		},
	}, WSGatewayOptions{Dialect: WSDialectPrimalV1})

	events, err := gateway.primalV1EventZaps(context.Background(), map[string]any{
		"event_id": "note1",
		"limit":    10,
	})
	if err != nil {
		t.Fatal(err)
	}
	var sawReceipt bool
	var sender string
	for _, event := range events {
		switch typed := event.(type) {
		case json.RawMessage:
			if rawKind(typed) == 9735 {
				sawReceipt = true
			}
		case map[string]any:
			kind, _ := kindOf(typed["kind"])
			if kind != primalKindPostZaps {
				continue
			}
			var body map[string]any
			if err := json.Unmarshal([]byte(stringValue(typed["content"])), &body); err != nil {
				t.Fatal(err)
			}
			sender = stringValue(body["sender"])
			if body["zap_receipt_id"] != "receipt1" || body["amount_sats"] != float64(21) {
				t.Fatalf("zap body %#v", body)
			}
		}
	}
	if !sawReceipt {
		t.Fatal("missing zap receipt")
	}
	if sender != zapper {
		t.Fatalf("sender %s want zapper %s", sender, zapper)
	}
}
