package nostr

import "fmt"

// CommentCountSQL is a scalar added to reply totals. idExpr is a SQL event-id
// expression owned by the caller (a placeholder or a column), never user input.
// A comment counts once even when its root and parent are the same event, and
// an address-tagged comment still counts after the replaceable event is revised.
func CommentCountSQL(idExpr string) string {
	addr := addressExpr("rs")
	return fmt.Sprintf(`COALESCE((
		SELECT COUNT(DISTINCT c.source_event_id)
		FROM comment_edges c
		LEFT JOIN replaceable_state rs ON rs.event_id = %s
		WHERE c.root_event_id = %s
		   OR c.parent_event_id = %s
		   OR (
		        rs.event_id IS NOT NULL
		        AND (c.root_address = %s OR c.parent_address = %s)
		   )
	), 0)`, idExpr, idExpr, idExpr, addr, addr)
}

// AddressRepostCountSQL is a scalar added to repost totals for the current
// replaceable. An address contribution is skipped when the same source already
// counted an e-tag repost of this event id.
func AddressRepostCountSQL(idExpr string) string {
	return fmt.Sprintf(`COALESCE((
		SELECT COUNT(*)
		FROM address_repost_contributions a
		JOIN replaceable_state rs ON rs.event_id = %s
		WHERE a.target_address = %s
		  AND NOT EXISTS (
		      SELECT 1
		      FROM repost_count_contributions r
		      WHERE r.source_event_id = a.source_event_id
		        AND r.target_event_id = %s
		  )
	), 0)`, idExpr, addressExpr("rs"), idExpr)
}

func addressExpr(alias string) string {
	return alias + ".kind::text || ':' || " + alias + ".pubkey || ':' || " + alias + ".d_tag"
}

// replaceableAddressExpr is the canonical NIP-33 coordinate stored on quote
// and zap rows. The pubkey is lowercased so it matches ParseAddressCoordinate.
func replaceableAddressExpr(alias string) string {
	return alias + ".kind::text || ':' || lower(" + alias + ".pubkey) || ':' || " + alias + ".d_tag"
}

// Kind1QuoteCountSQL counts kind-1 notes whose q tag points at this event id
// or, for the current replaceable, at its address. Kind-6 quote reposts stay
// inside repost_count and are not added here.
func Kind1QuoteCountSQL(idExpr string) string {
	addr := replaceableAddressExpr("rs")
	return fmt.Sprintf(`COALESCE((
		SELECT COUNT(DISTINCT q.source_event_id)
		FROM quote_contributions q
		LEFT JOIN replaceable_state rs ON rs.event_id = %s
		WHERE (q.target_event_id <> '' AND q.target_event_id = %s)
		   OR (
		        rs.event_id IS NOT NULL
		        AND q.target_address <> ''
		        AND q.target_address = %s
		        AND q.target_event_id <> %s
		   )
	), 0)`, idExpr, idExpr, addr, idExpr)
}

// ZapCountSQL counts receipts that name this event or, when this row is the
// current replaceable, receipts that name its address instead.
func ZapCountSQL(idExpr string) string {
	return fmt.Sprintf(`COALESCE((%s), 0)`, zapMatchAggregateSQL(idExpr, "COUNT(*)"))
}

// ZapMSatsSQL is the millisat total for the same receipt set as ZapCountSQL.
func ZapMSatsSQL(idExpr string) string {
	return fmt.Sprintf(`COALESCE((%s), 0)`, zapMatchAggregateSQL(idExpr, "SUM(z.amount_sats * 1000)"))
}

func zapMatchAggregateSQL(idExpr string, aggregate string) string {
	addr := replaceableAddressExpr("rs")
	return fmt.Sprintf(`
		SELECT %s
		FROM zap_receipts z
		LEFT JOIN replaceable_state rs ON rs.event_id = %s
		WHERE z.event_id = %s
		   OR (
		        rs.event_id IS NOT NULL
		        AND z.target_address <> ''
		        AND z.target_address = %s
		        AND z.event_id IS DISTINCT FROM %s
		   )`, aggregate, idExpr, idExpr, addr, idExpr)
}
