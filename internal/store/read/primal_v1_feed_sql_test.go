package read

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestAuthorFeedPageSQLUsesPerKindIndexWalk(t *testing.T) {
	sql := authorFeedPageSQL([]int{1, 6, 1, -1}, false, false, false)
	if !strings.Contains(sql, "e.kind = 1") || !strings.Contains(sql, "e.kind = 6") {
		t.Fatalf("kinds not inlined: %s", sql)
	}
	if strings.Contains(sql, "ANY(") {
		t.Fatalf("kind list must be equality predicates, got %s", sql)
	}
	if strings.Count(sql, "raw_json") != 1 {
		t.Fatalf("raw_json should be loaded once for the page, got %s", sql)
	}
	if strings.Count(sql, "NOT EXISTS (SELECT 1 FROM thread_edges") != 2 {
		t.Fatalf("replies should be excluded once per kind: %s", sql)
	}
	if strings.Count(sql, "event_attributes") != 1 {
		t.Fatalf("expiry should be checked once on the page, got %s", sql)
	}

	withReplies := authorFeedPageSQL([]int{1}, true, false, false)
	if strings.Contains(withReplies, "thread_edges") {
		t.Fatalf("include_replies should not probe thread_edges: %s", withReplies)
	}
	if strings.Count(withReplies, "event_attributes") != 1 {
		t.Fatalf("expiry should be checked once on the page, got %s", withReplies)
	}
	repliesOnly := authorFeedPageSQL([]int{1}, false, true, false)
	if strings.Contains(repliesOnly, "NOT EXISTS (SELECT 1 FROM thread_edges") || !strings.Contains(repliesOnly, "EXISTS (SELECT 1 FROM thread_edges") {
		t.Fatalf("replies-only filter: %s", repliesOnly)
	}
	if strings.Count(repliesOnly, "event_attributes") != 1 {
		t.Fatalf("expired events should be hidden once: %s", repliesOnly)
	}
}

func TestAuthorsAtFeedCutoffKeepsOnlyAuthorsWhoCanReachThePage(t *testing.T) {
	heads := []authorHead{
		{pubkey: "old", id: "1", created: 10},
		{pubkey: "mid", id: "2", created: 20},
		{pubkey: "new", id: "3", created: 30},
		{pubkey: "tied", id: "4", created: 20},
	}
	got := authorsAtFeedCutoff(heads, 2)
	if len(got) != 3 || got[0] != "new" || got[1] != "tied" || got[2] != "mid" {
		t.Fatalf("cutoff authors = %#v", got)
	}
	if len(authorsAtFeedCutoff(heads, 10)) != 4 {
		t.Fatalf("a window larger than the heads keeps every author")
	}
}

func TestMergeAuthorFeedPagesKeepsNewestAcrossChunks(t *testing.T) {
	pages := [][]json.RawMessage{
		{
			json.RawMessage(`{"id":"a","created_at":10}`),
			json.RawMessage(`{"id":"b","created_at":8}`),
		},
		{
			json.RawMessage(`{"id":"c","created_at":12}`),
			json.RawMessage(`{"id":"d","created_at":9}`),
		},
	}
	got := mergeAuthorFeedPages(pages, 2, 1)
	if len(got) != 2 {
		t.Fatalf("len %d", len(got))
	}
	if !strings.Contains(string(got[0]), `"id":"a"`) || !strings.Contains(string(got[1]), `"id":"d"`) {
		t.Fatalf("page = %s, %s", got[0], got[1])
	}
}

func TestPrimalTrendingSQLWalksRankIndex(t *testing.T) {
	sql := primalTrendingSQL("s.score_7d", "ROUND(s.score_7d)::float8", "AND s.score_7d < $4")
	if strings.Contains(sql, "score::bigint") || strings.Contains(sql, "FROM (") {
		t.Fatalf("trending page must not score the whole window: %s", sql)
	}
	if !strings.Contains(sql, "ORDER BY s.score_7d DESC, s.created_at DESC, s.event_id ASC") {
		t.Fatalf("order: %s", sql)
	}
	if !strings.Contains(sql, "LIMIT $2 OFFSET $3") || !strings.Contains(sql, "AND s.score_7d < $4") {
		t.Fatalf("cursor: %s", sql)
	}
}

func TestPrimalTrendingCursorValue(t *testing.T) {
	if got, ok := primalTrendingCursorValue(10, false).(float64); !ok || got != 10.5 {
		t.Fatalf("score cursor %v", primalTrendingCursorValue(10, false))
	}
	if got, ok := primalTrendingCursorValue(10, true).(int64); !ok || got != 10500 {
		t.Fatalf("zap cursor %v", primalTrendingCursorValue(10, true))
	}
}
