package derivation

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// authorOutsideTrustGraph reports whether authorPubkey should be excluded
// from Web-of-Trust-gated projections (event_urls, event_hashtags) because
// it is absent from trust_graph_snapshot.
//
// Fail-safe: if trust_graph_snapshot is empty (never loaded, mid-rebuild,
// or the trust worker isn't running on this deployment), every author is
// treated as trusted rather than silently dropping every URL/hashtag —
// the same fail-safe rule internal/store/retention/events_retention_untrusted.go
// uses for the untrusted-author event purge. Otherwise an empty graph
// would classify every author as untrusted and stop recording links and
// hashtags for the entire network.
func authorOutsideTrustGraph(ctx context.Context, tx pgx.Tx, authorPubkey string) (bool, error) {
	var excluded bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM trust_graph_snapshot)
		   AND NOT EXISTS (SELECT 1 FROM trust_graph_snapshot WHERE pubkey = $1)
	`, authorPubkey).Scan(&excluded); err != nil {
		return false, fmt.Errorf("check trust graph membership: %w", err)
	}
	return excluded, nil
}
