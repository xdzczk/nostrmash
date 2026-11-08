package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/nostr"
)

// indexNewAuthorFeedEventTx puts a newly stored kind-1 note or kind-6 repost
// on the follows-feed index in the same transaction as the event. Replies are
// left out; thread projection removes a row later if it decides the note is
// a reply the tag parse missed, and adds one if a reply stops being a reply.
func indexNewAuthorFeedEventTx(ctx context.Context, tx pgx.Tx, event model.Event, tags [][]string) error {
	switch event.Kind {
	case 1:
		if nostr.ReplyParentEventID(tags) != "" {
			return nil
		}
	case 6:
	default:
		return nil
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO author_feed_events (event_id, pubkey, kind, created_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (event_id) DO UPDATE
		SET pubkey = EXCLUDED.pubkey,
		    kind = EXCLUDED.kind,
		    created_at = EXCLUDED.created_at
	`, event.ID, event.Pubkey, event.Kind, event.CreatedAt); err != nil {
		return fmt.Errorf("index author feed event: %w", err)
	}
	return nil
}
