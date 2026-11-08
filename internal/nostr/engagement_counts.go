package nostr

import "fmt"

// CommentCountSQL is a scalar added to reply totals. idExpr is a SQL event-id
// expression owned by the caller (a placeholder or a column), never user input.
// A comment counts once even when its root and parent are the same event, and
// an address-tagged comment still counts after the replaceable event is revised.
//
// The address is resolved once, then each arm is a separate index lookup.
// OR-ing those predicates across a LEFT JOIN made Postgres seq-scan
// comment_edges once per event id.
func CommentCountSQL(idExpr string) string {
	return fmt.Sprintf(`COALESCE((
		WITH addr AS (
			SELECT %s AS address
			FROM replaceable_state rs
			WHERE rs.event_id = %s
		)
		SELECT COUNT(*)
		FROM (
			SELECT c.source_event_id
			FROM comment_edges c
			WHERE c.root_event_id <> '' AND c.root_event_id = %s
			UNION
			SELECT c.source_event_id
			FROM comment_edges c
			WHERE c.parent_event_id <> '' AND c.parent_event_id = %s
			UNION
			SELECT c.source_event_id
			FROM comment_edges c
			JOIN addr ON c.root_address = addr.address
			WHERE c.root_address <> ''
			UNION
			SELECT c.source_event_id
			FROM comment_edges c
			JOIN addr ON c.parent_address = addr.address
			WHERE c.parent_address <> ''
		) matched
	), 0)`, addressExpr("rs"), idExpr, idExpr, idExpr)
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
	return fmt.Sprintf(`COALESCE((
		WITH addr AS (
			SELECT %s AS address
			FROM replaceable_state rs
			WHERE rs.event_id = %s
		)
		SELECT COUNT(*)
		FROM (
			SELECT q.source_event_id
			FROM quote_contributions q
			WHERE q.target_event_id <> '' AND q.target_event_id = %s
			UNION
			SELECT q.source_event_id
			FROM quote_contributions q
			JOIN addr ON q.target_address = addr.address
			WHERE q.target_address <> ''
			  AND q.target_event_id <> %s
		) matched
	), 0)`, replaceableAddressExpr("rs"), idExpr, idExpr, idExpr)
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
	// UNION ALL keeps equal amounts from different receipts. The address arm
	// excludes receipts already matched by event id, so a receipt that names
	// both is counted once. Selecting amount_sats (not the receipt id) lets
	// COUNT(*) and SUM(z.amount_sats * 1000) share this shape.
	return fmt.Sprintf(`
		WITH addr AS (
			SELECT %s AS address
			FROM replaceable_state rs
			WHERE rs.event_id = %s
		)
		SELECT %s
		FROM (
			SELECT z.amount_sats
			FROM zap_receipts z
			WHERE z.event_id = %s
			UNION ALL
			SELECT z.amount_sats
			FROM zap_receipts z
			JOIN addr ON z.target_address = addr.address
			WHERE z.target_address <> ''
			  AND z.event_id IS DISTINCT FROM %s
		) z`, replaceableAddressExpr("rs"), idExpr, aggregate, idExpr, idExpr)
}
