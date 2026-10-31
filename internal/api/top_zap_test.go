package api

import (
	"context"
	"testing"
)

func TestAttachTopZapsSkipsWhenReaderHasNoTopZaps(t *testing.T) {
	h := Handlers{}
	items := []map[string]any{{"id": "abc", "content": "hi"}}
	h.attachTopZaps(context.Background(), items)
	if _, ok := items[0]["top_zap"]; ok {
		t.Fatalf("top_zap = %#v", items[0]["top_zap"])
	}
}
