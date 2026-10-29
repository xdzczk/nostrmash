package derivation

import (
	"context"
	"fmt"
	"strings"

	"github.com/xdzczk/nostrmash/internal/nostr"
)

func (h *Handlers) ProjectCommentEdges(ctx context.Context, eventID string) error {
	return h.projectCommentEdgesWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectCommentEdgesWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}
	var kind int
	var author string
	var createdAt int64
	if err := h.pool.QueryRow(ctx, `
		SELECT kind, pubkey, created_at FROM events WHERE id = $1
	`, eventID).Scan(&kind, &author, &createdAt); err != nil {
		return fmt.Errorf("load event for comment edges: %w", err)
	}
	tags, err := h.loadEventTags(ctx, eventID)
	if err != nil {
		return err
	}
	rootID, rootAddr, parentID, parentAddr := "", "", "", ""
	if kind == 1111 {
		rootID = firstExactTag(tags, "E")
		rootAddr = firstAddress(tags, "A")
		parentID = firstExactTag(tags, "e")
		parentAddr = firstAddress(tags, "a")
	}

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin comment edges tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	writeVersion, err := resolveDerivationWriteVersion(
		ctx, tx, DerivationCommentEdges, CommentEdgesVersion,
		"Project NIP-22 comment edges from kind=1111 root and parent tags",
		versionOverride,
	)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM comment_edges WHERE source_event_id = $1`, eventID); err != nil {
		return fmt.Errorf("delete comment edge: %w", err)
	}
	if kind == 1111 {
		if _, err := tx.Exec(ctx, `
			INSERT INTO comment_edges (
				source_event_id, root_event_id, root_address, parent_event_id, parent_address,
				author_pubkey, created_at, derivation_version
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		`, eventID, rootID, rootAddr, parentID, parentAddr, author, createdAt, writeVersion); err != nil {
			return fmt.Errorf("insert comment edge: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit comment edges: %w", err)
	}
	return nil
}

func (h *Handlers) ProjectReportTargets(ctx context.Context, eventID string) error {
	return h.projectReportTargetsWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectReportTargetsWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}
	var kind int
	var author string
	var createdAt int64
	var content string
	if err := h.pool.QueryRow(ctx, `
		SELECT kind, pubkey, created_at, content FROM events WHERE id = $1
	`, eventID).Scan(&kind, &author, &createdAt, &content); err != nil {
		return fmt.Errorf("load event for report targets: %w", err)
	}
	tags, err := h.loadEventTags(ctx, eventID)
	if err != nil {
		return err
	}
	type target struct {
		name  string
		value string
		kind  string
	}
	var targets []target
	if kind == 1984 {
		seen := map[string]struct{}{}
		for _, tag := range tags {
			if len(tag) < 2 || (tag[0] != "e" && tag[0] != "p") {
				continue
			}
			value := strings.TrimSpace(tag[1])
			if value == "" {
				continue
			}
			key := tag[0] + "\x00" + value
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			targets = append(targets, target{name: tag[0], value: value, kind: reportTypeFromTag(tag)})
		}
	}

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin report targets tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	writeVersion, err := resolveDerivationWriteVersion(
		ctx, tx, DerivationReportTargets, ReportTargetsVersion,
		"Project NIP-56 report targets from kind=1984 e and p tags",
		versionOverride,
	)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM report_targets WHERE report_event_id = $1`, eventID); err != nil {
		return fmt.Errorf("delete report targets: %w", err)
	}
	for _, item := range targets {
		if _, err := tx.Exec(ctx, `
			INSERT INTO report_targets (
				report_event_id, tag_name, target_value, reporter_pubkey, report_type,
				content, created_at, derivation_version
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		`, eventID, item.name, item.value, author, item.kind, content, createdAt, writeVersion); err != nil {
			return fmt.Errorf("insert report target: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit report targets: %w", err)
	}
	return nil
}

func (h *Handlers) projectAddressRepostContributions(ctx context.Context, eventID string, versionOverride *int) error {
	var kind int
	if err := h.pool.QueryRow(ctx, `SELECT kind FROM events WHERE id = $1`, eventID).Scan(&kind); err != nil {
		return fmt.Errorf("load event for address reposts: %w", err)
	}
	tags, err := h.loadEventTags(ctx, eventID)
	if err != nil {
		return err
	}
	var addresses []string
	if kind == 16 {
		seen := map[string]struct{}{}
		for _, tag := range tags {
			if len(tag) < 2 || tag[0] != "a" {
				continue
			}
			addr, ok := nostr.ParseAddressCoordinate(tag[1])
			if !ok {
				continue
			}
			key := addr.String()
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			addresses = append(addresses, key)
		}
	}

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin address repost tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	writeVersion, err := resolveDerivationWriteVersion(
		ctx, tx, DerivationRepostCounts, RepostCountsVersion,
		"Project eventually-consistent repost counts from kind=6 and kind=16 references",
		versionOverride,
	)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM address_repost_contributions WHERE source_event_id = $1`, eventID); err != nil {
		return fmt.Errorf("delete address reposts: %w", err)
	}
	for _, addr := range addresses {
		if _, err := tx.Exec(ctx, `
			INSERT INTO address_repost_contributions (source_event_id, target_address, derivation_version)
			VALUES ($1, $2, $3)
		`, eventID, addr, writeVersion); err != nil {
			return fmt.Errorf("insert address repost: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit address reposts: %w", err)
	}
	return nil
}

func firstExactTag(tags [][]string, name string) string {
	for _, tag := range tags {
		if len(tag) >= 2 && tag[0] == name {
			if value := strings.TrimSpace(tag[1]); value != "" {
				return value
			}
		}
	}
	return ""
}

func firstAddress(tags [][]string, name string) string {
	for _, tag := range tags {
		if len(tag) < 2 || tag[0] != name {
			continue
		}
		if addr, ok := nostr.ParseAddressCoordinate(tag[1]); ok {
			return addr.String()
		}
	}
	return ""
}

func reportTypeFromTag(tag []string) string {
	if len(tag) < 3 {
		return "other"
	}
	last := strings.TrimSpace(tag[len(tag)-1])
	if last == "" || last == strings.TrimSpace(tag[1]) {
		return "other"
	}
	lower := strings.ToLower(last)
	if strings.Contains(lower, "://") || strings.HasPrefix(lower, "ws") {
		return "other"
	}
	switch lower {
	case "nudity", "malware", "profanity", "illegal", "spam", "impersonation", "other":
		return lower
	default:
		return "other"
	}
}
