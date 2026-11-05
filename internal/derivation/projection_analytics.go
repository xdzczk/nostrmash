package derivation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

var analyticsURLPattern = regexp.MustCompile(`(?i)https?://\S+`)
var analyticsMentionPattern = regexp.MustCompile(`(?i)nostr:(?:npub|nprofile|note|nevent)1[a-z0-9]+`)

func recordDeletionDailyTx(ctx context.Context, tx pgx.Tx, eventID, targetID string, createdAt int64) error {
	claimed, err := claimStatDeltaTx(ctx, tx, eventID, "deletion_daily")
	if err != nil || !claimed {
		return err
	}
	var kind int
	var targetCreated int64
	if err := tx.QueryRow(ctx, `SELECT kind, created_at FROM events WHERE id = $1`, targetID).Scan(&kind, &targetCreated); err != nil {
		kind = 0
		targetCreated = createdAt
	}
	latency := createdAt - targetCreated
	if latency < 0 {
		latency = 0
	}
	day := time.Unix(createdAt, 0).UTC().Format("2006-01-02")
	_, err = tx.Exec(ctx, `
		INSERT INTO deletion_daily (day, kind, count, latency_total_s)
		VALUES ($1::date, $2, 1, $3)
		ON CONFLICT (day, kind) DO UPDATE
		SET count = deletion_daily.count + 1,
		    latency_total_s = deletion_daily.latency_total_s + EXCLUDED.latency_total_s
	`, day, kind, latency)
	return err
}

func recordContentFingerprintTx(ctx context.Context, tx pgx.Tx, eventID, pubkey, content string, kind int, createdAt int64) error {
	if kind != 1 {
		return nil
	}
	fingerprint, ok := normalizedContentFingerprint(content)
	if !ok {
		return nil
	}
	claimed, err := claimStatDeltaTx(ctx, tx, eventID, "content_fingerprint")
	if err != nil || !claimed {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO content_fingerprints (fingerprint, first_event_id, first_pubkey, event_count, pubkey_count, last_seen_at)
		VALUES ($1, $2, $3, 1, 1, $4)
		ON CONFLICT (fingerprint) DO UPDATE
		SET event_count = content_fingerprints.event_count + 1,
		    last_seen_at = GREATEST(content_fingerprints.last_seen_at, EXCLUDED.last_seen_at)
	`, fingerprint, eventID, pubkey, createdAt); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO content_fingerprint_pubkeys (fingerprint, pubkey)
		VALUES ($1, $2)
		ON CONFLICT DO NOTHING
	`, fingerprint, pubkey)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil
	}
	_, err = tx.Exec(ctx, `
		UPDATE content_fingerprints SET pubkey_count = pubkey_count + 1 WHERE fingerprint = $1
	`, fingerprint)
	return err
}

func normalizedContentFingerprint(content string) (string, bool) {
	cleaned := analyticsURLPattern.ReplaceAllString(content, " ")
	cleaned = analyticsMentionPattern.ReplaceAllString(cleaned, " ")
	cleaned = strings.Join(strings.Fields(strings.ToLower(cleaned)), " ")
	if utf8.RuneCountInString(cleaned) < 40 {
		return "", false
	}
	sum := sha256.Sum256([]byte(cleaned))
	return hex.EncodeToString(sum[:]), true
}

func (h *Handlers) refreshProfileGraphStatsTx(ctx context.Context, tx pgx.Tx, pubkey string) error {
	cutoff := time.Now().UTC().AddDate(0, 0, -90).Unix()
	_, err := tx.Exec(ctx, `
		INSERT INTO profile_graph_stats (
			pubkey, mutual_count, reciprocity_bps, follow_back_rate_bps,
			ghost_follower_bps, mentioned_not_followed_count, computed_at
		)
		SELECT
			$1,
			mutual.n,
			CASE WHEN following.n = 0 THEN 0 ELSE mutual.n * 10000 / following.n END,
			CASE WHEN followers.n = 0 THEN 0 ELSE mutual.n * 10000 / followers.n END,
			CASE WHEN followers.n = 0 THEN 0 ELSE ghosts.n * 10000 / followers.n END,
			0,
			now()
		FROM (SELECT count(*)::bigint AS n FROM follower_edges WHERE follower_pubkey = $1 AND followed_pubkey <> $1) following
		CROSS JOIN (SELECT count(*)::bigint AS n FROM follower_edges WHERE followed_pubkey = $1 AND follower_pubkey <> $1) followers
		CROSS JOIN (
			SELECT count(*)::bigint AS n
			FROM follower_edges out_edge
			JOIN follower_edges back
			  ON back.follower_pubkey = out_edge.followed_pubkey
			 AND back.followed_pubkey = out_edge.follower_pubkey
			WHERE out_edge.follower_pubkey = $1
			  AND out_edge.followed_pubkey <> $1
		) mutual
		CROSS JOIN (
			SELECT count(*)::bigint AS n
			FROM follower_edges f
			LEFT JOIN profile_public_stats s ON s.pubkey = f.follower_pubkey
			WHERE f.followed_pubkey = $1
			  AND f.follower_pubkey <> $1
			  AND (s.recent_activity_at IS NULL OR s.recent_activity_at < $2)
		) ghosts
		ON CONFLICT (pubkey) DO UPDATE
		SET mutual_count = EXCLUDED.mutual_count,
		    reciprocity_bps = EXCLUDED.reciprocity_bps,
		    follow_back_rate_bps = EXCLUDED.follow_back_rate_bps,
		    ghost_follower_bps = EXCLUDED.ghost_follower_bps,
		    computed_at = now()
	`, pubkey, cutoff)
	if err != nil {
		return fmt.Errorf("refresh profile graph stats: %w", err)
	}
	return nil
}
