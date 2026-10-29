package derivation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type primalNotice struct {
	Recipient string
	Type      int
	Args      map[string]any
}

func (h *Handlers) ProjectPrimalNotification(ctx context.Context, eventID string) error {
	err := h.projectPrimalNotification(ctx, eventID)
	if isUndefinedTable(err) {
		return nil
	}
	return err
}

func (h *Handlers) projectPrimalNotification(ctx context.Context, eventID string) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil
	}
	var kind int
	var author string
	var createdAt int64
	var content string
	err := h.pool.QueryRow(ctx, `SELECT kind, pubkey, created_at, content FROM events WHERE id = $1`, eventID).Scan(&kind, &author, &createdAt, &content)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("load event for primal notifications: %w", err)
	}
	tags, err := h.loadEventTags(ctx, eventID)
	if err != nil {
		return err
	}
	parentID, parentAuthor, parentIsReply := h.threadParent(ctx, eventID)
	drafts := primalNotificationDrafts(kind, author, eventID, content, tags, parentID, parentAuthor, parentIsReply)
	if kind == 3 {
		rows, err := h.pool.Query(ctx, `
			SELECT followed_pubkey FROM follower_gain_events
			WHERE follower_pubkey = $1 AND gained_at = $2
		`, author, createdAt)
		if err != nil && !isUndefinedTable(err) {
			return fmt.Errorf("load follower gains: %w", err)
		}
		if err == nil {
			for rows.Next() {
				var followed string
				if err := rows.Scan(&followed); err != nil {
					rows.Close()
					return err
				}
				if followed != "" && followed != author {
					drafts = append(drafts, primalNotice{
						Recipient: followed,
						Type:      1,
						Args:      map[string]any{"follower": author},
					})
				}
			}
			rows.Close()
		}
	}
	if len(drafts) == 0 {
		return nil
	}
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin primal notifications: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, draft := range drafts {
		if draft.Type == 8 {
			mentioned, _ := draft.Args["event_id"].(string)
			var mentionedAuthor string
			if err := tx.QueryRow(ctx, `SELECT pubkey FROM events WHERE id = $1`, mentioned).Scan(&mentionedAuthor); err != nil || mentionedAuthor == "" || mentionedAuthor == author {
				continue
			}
			draft.Recipient = mentionedAuthor
			draft.Args["mentioned_by"] = eventID
		}
		args, err := json.Marshal(draft.Args)
		if err != nil {
			return err
		}
		key := fmt.Sprintf("%d:%s:%s", draft.Type, draft.Recipient, eventID)
		tag, err := tx.Exec(ctx, `
			INSERT INTO primal_notifications (dedupe_key, pubkey, type, created_at, args)
			VALUES ($1, $2, $3, $4, $5::jsonb)
			ON CONFLICT (dedupe_key) DO NOTHING
		`, key, draft.Recipient, draft.Type, createdAt, string(args))
		if err != nil {
			return fmt.Errorf("insert primal notification: %w", err)
		}
		if tag.RowsAffected() == 0 {
			continue
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO primal_notification_counts (pubkey, type, cnt)
			SELECT $1, $2, 1
			WHERE NOT EXISTS (
				SELECT 1 FROM primal_notifications_seen WHERE pubkey = $1 AND seen_until >= $3
			)
			ON CONFLICT (pubkey, type) DO UPDATE
			SET cnt = primal_notification_counts.cnt + 1
		`, draft.Recipient, draft.Type, createdAt); err != nil {
			return fmt.Errorf("increment primal notification count: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit primal notifications: %w", err)
	}
	return nil
}

func (h *Handlers) threadParent(ctx context.Context, eventID string) (string, string, bool) {
	var parentID string
	err := h.pool.QueryRow(ctx, `SELECT parent_event_id FROM thread_edges WHERE child_event_id = $1`, eventID).Scan(&parentID)
	if err != nil || strings.TrimSpace(parentID) == "" {
		return "", "", false
	}
	var parentAuthor string
	_ = h.pool.QueryRow(ctx, `SELECT pubkey FROM events WHERE id = $1`, parentID).Scan(&parentAuthor)
	var parentIsReply bool
	_ = h.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM thread_edges WHERE child_event_id = $1)`, parentID).Scan(&parentIsReply)
	return parentID, parentAuthor, parentIsReply
}

func primalNotificationDrafts(kind int, author string, eventID string, content string, tags [][]string, parentID string, parentAuthor string, parentIsReply bool) []primalNotice {
	author = strings.TrimSpace(author)
	out := make([]primalNotice, 0)
	switch kind {
	case 1:
		if parentAuthor != "" && parentAuthor != author {
			typ := 6
			if parentIsReply {
				typ = 601
			}
			out = append(out, primalNotice{Recipient: parentAuthor, Type: typ, Args: map[string]any{
				"your_post":         parentID,
				"reply":             eventID,
				"who_replied_to_it": author,
			}})
		}
		for _, pubkey := range tagValueList(tags, "p") {
			if pubkey == author || pubkey == parentAuthor {
				continue
			}
			out = append(out, primalNotice{Recipient: pubkey, Type: 7, Args: map[string]any{
				"you_were_mentioned_in": eventID,
			}})
		}
		for _, mentioned := range mentionEventIDs(tags) {
			out = append(out, primalNotice{Recipient: "", Type: 8, Args: map[string]any{
				"your_post":                   mentioned,
				"your_post_were_mentioned_in": eventID,
			}})
		}
	case 6, 16:
		target := firstTag(tags, "e")
		if target == "" {
			target = firstTag(tags, "a")
		}
		recipient := firstTag(tags, "p")
		if recipient != "" && recipient != author {
			out = append(out, primalNotice{Recipient: recipient, Type: 5, Args: map[string]any{
				"your_post":       target,
				"who_reposted_it": author,
			}})
		}
	case 7:
		target := firstTag(tags, "e")
		recipient := firstTag(tags, "p")
		if recipient != "" && recipient != author {
			out = append(out, primalNotice{Recipient: recipient, Type: 4, Args: map[string]any{
				"your_post":    target,
				"who_liked_it": author,
				"reaction":     content,
			}})
		}
	case 9735:
		recipient := firstTag(tags, "p")
		if recipient != "" && recipient != author {
			sats := parseZapAmountSats(firstTag(tags, "amount"))
			out = append(out, primalNotice{Recipient: recipient, Type: 3, Args: map[string]any{
				"your_post":     firstTag(tags, "e"),
				"who_zapped_it": author,
				"satszapped":    sats,
			}})
		}
	}
	filtered := out[:0]
	for _, draft := range out {
		if draft.Type == 8 {
			filtered = append(filtered, draft)
			continue
		}
		if strings.TrimSpace(draft.Recipient) == "" || draft.Recipient == author {
			continue
		}
		filtered = append(filtered, draft)
	}
	return filtered
}

func tagValueList(tags [][]string, name string) []string {
	out := make([]string, 0)
	seen := map[string]struct{}{}
	for _, tag := range tags {
		if len(tag) < 2 || tag[0] != name {
			continue
		}
		value := strings.TrimSpace(tag[1])
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func mentionEventIDs(tags [][]string) []string {
	out := make([]string, 0)
	for _, tag := range tags {
		if len(tag) < 4 || tag[0] != "e" || tag[3] != "mention" {
			continue
		}
		if id := strings.TrimSpace(tag[1]); id != "" {
			out = append(out, id)
		}
	}
	return out
}

func firstTag(tags [][]string, name string) string {
	values := tagValueList(tags, name)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func isUndefinedTable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P01"
}

// RebuildPrimalNotifications is an explicit backfill. It is not run on boot.
func (h *Handlers) RebuildPrimalNotifications(ctx context.Context, limit int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	if limit <= 0 {
		limit = 5000
	}
	statements := []string{
		`INSERT INTO primal_notifications (dedupe_key, pubkey, type, created_at, args)
		 SELECT '4:' || target_pubkey || ':' || event_id, target_pubkey, 4, created_at,
		        jsonb_build_object('your_post', target_event_id, 'who_liked_it', reactor_pubkey, 'reaction', content)
		 FROM reaction_events
		 WHERE target_pubkey IS NOT NULL AND target_pubkey <> reactor_pubkey
		 ORDER BY created_at DESC LIMIT $1
		 ON CONFLICT (dedupe_key) DO NOTHING`,
		`INSERT INTO primal_notifications (dedupe_key, pubkey, type, created_at, args)
		 SELECT '5:' || target_pubkey || ':' || event_id, target_pubkey, 5, created_at,
		        jsonb_build_object('your_post', target_event_id, 'who_reposted_it', reposter_pubkey)
		 FROM repost_events
		 WHERE target_pubkey IS NOT NULL AND target_pubkey <> reposter_pubkey
		 ORDER BY created_at DESC LIMIT $1
		 ON CONFLICT (dedupe_key) DO NOTHING`,
		`INSERT INTO primal_notifications (dedupe_key, pubkey, type, created_at, args)
		 SELECT '3:' || receiver_pubkey || ':' || zap_receipt_id, receiver_pubkey, 3, created_at,
		        jsonb_build_object('your_post', event_id, 'who_zapped_it', sender_pubkey, 'satszapped', amount_sats)
		 FROM zap_receipts
		 WHERE receiver_pubkey IS NOT NULL AND receiver_pubkey <> sender_pubkey
		 ORDER BY created_at DESC LIMIT $1
		 ON CONFLICT (dedupe_key) DO NOTHING`,
		`INSERT INTO primal_notifications (dedupe_key, pubkey, type, created_at, args)
		 SELECT '1:' || followed_pubkey || ':' || follower_pubkey, followed_pubkey, 1, gained_at,
		        jsonb_build_object('follower', follower_pubkey)
		 FROM follower_gain_events
		 ORDER BY gained_at DESC LIMIT $1
		 ON CONFLICT (dedupe_key) DO NOTHING`,
	}
	for _, statement := range statements {
		if _, err := h.pool.Exec(ctx, statement, limit); err != nil {
			return fmt.Errorf("rebuild primal notifications: %w", err)
		}
	}
	return nil
}
