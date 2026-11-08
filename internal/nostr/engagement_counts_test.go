package nostr

import "strings"
import "testing"

func TestEngagementCountSQLUsesIndexArms(t *testing.T) {
	for _, sql := range []string{
		CommentCountSQL("x.id"),
		Kind1QuoteCountSQL("$1"),
		ZapCountSQL("e.id"),
		ZapMSatsSQL("e.id"),
	} {
		if strings.Contains(sql, "LEFT JOIN") {
			t.Fatalf("count SQL still joins before filtering:\n%s", sql)
		}
		if !strings.Contains(sql, "UNION") {
			t.Fatalf("count SQL missing index arms:\n%s", sql)
		}
	}
	comments := CommentCountSQL("x.id")
	if !strings.Contains(comments, "root_event_id <> ''") || !strings.Contains(comments, "parent_address <> ''") {
		t.Fatalf("comment arms must match the partial indexes:\n%s", comments)
	}
	if !strings.Contains(ZapMSatsSQL("e.id"), "SUM(z.amount_sats * 1000)") {
		t.Fatal("zap millisats aggregate was dropped")
	}
}
