package store

import (
	"encoding/json"
	"reflect"
	"testing"
)

func jsonEqual(left []byte, right []byte) bool {
	var leftV any
	var rightV any
	if err := json.Unmarshal(left, &leftV); err != nil {
		return false
	}
	if err := json.Unmarshal(right, &rightV); err != nil {
		return false
	}
	return reflect.DeepEqual(leftV, rightV)
}

func decodeEventIDs(t *testing.T, raws []json.RawMessage) []string {
	t.Helper()
	ids := make([]string, 0, len(raws))
	for _, raw := range raws {
		var payload struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Fatalf("decode event payload: %v", err)
		}
		ids = append(ids, payload.ID)
	}
	return ids
}
