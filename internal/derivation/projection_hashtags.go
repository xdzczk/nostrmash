package derivation

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
)

var hashtagPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

func (h *Handlers) ProjectEventHashtags(ctx context.Context, eventID string) error {
	return h.projectEventHashtagsWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectEventHashtagsWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	var authorPubkey string
	var createdAt int64
	var kind int
	if err := h.pool.QueryRow(ctx, `
		SELECT pubkey, created_at, kind
		FROM events
		WHERE id = $1
	`, eventID).Scan(&authorPubkey, &createdAt, &kind); err != nil {
		return fmt.Errorf("load event for hashtag projection: %w", err)
	}

	tags, err := h.loadEventTags(ctx, eventID)
	if err != nil {
		return err
	}
	hashtags := extractNormalizedHashtags(tags)

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationEventHashtags,
		EventHashtagsVersion,
		"Project normalized hashtags from note-like events",
		versionOverride,
	)
	if err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `DELETE FROM event_hashtags WHERE event_id = $1`, eventID); err != nil {
		return fmt.Errorf("delete prior event hashtags: %w", err)
	}

	// Hashtags from authors outside the Web of Trust are never recorded
	// here (as opposed to just being excluded from the homepage trending
	// snapshot — see trustedAuthorJoinClause in
	// projection_relay_window_snapshots.go). The DELETE above still runs
	// unconditionally so re-deriving an event whose author has since
	// dropped out of the trust graph cleans up any hashtags recorded
	// while they were still trusted.
	if isHashtagProjectableKind(kind) && len(hashtags) > 0 {
		excluded, err := authorOutsideTrustGraph(ctx, tx, authorPubkey)
		if err != nil {
			return err
		}
		if excluded {
			hashtags = nil
		}
	}

	if !isHashtagProjectableKind(kind) || len(hashtags) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit no-op hashtag projection tx: %w", err)
		}
		return nil
	}

	for _, hashtag := range hashtags {
		if _, err := tx.Exec(ctx, `
			INSERT INTO event_hashtags (
				event_id, author_pubkey, created_at, hashtag, derivation_version
			)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (event_id, hashtag) DO UPDATE
			SET author_pubkey = EXCLUDED.author_pubkey,
			    created_at = EXCLUDED.created_at,
			    derivation_version = EXCLUDED.derivation_version,
			    projected_at = now()
		`, eventID, authorPubkey, createdAt, hashtag, writeVersion); err != nil {
			return fmt.Errorf("upsert event hashtag: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit hashtag projection tx: %w", err)
	}
	return nil
}

func isHashtagProjectableKind(kind int) bool {
	return kind == 1 || kind == 30023
}

func extractNormalizedHashtags(tags [][]string) []string {
	if len(tags) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	out := make([]string, 0, len(tags))
	for _, tag := range tags {
		if len(tag) < 2 {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(tag[0]), "t") {
			continue
		}
		normalized := normalizeHashtag(tag[1])
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	slices.Sort(out)
	return out
}

func normalizeHashtag(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	normalized = strings.TrimPrefix(normalized, "#")
	normalized = strings.TrimSpace(normalized)
	if normalized == "" || !hashtagPattern.MatchString(normalized) {
		return ""
	}
	return normalized
}
