package derivation

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/xdzczk/nostrmash/internal/relayurl"
)

type selfLabel struct {
	Namespace string
	Label     string
}

type externalIdentity struct {
	Platform string
	Identity string
	Proof    string
}

func contentShape(kind int, content string, tags [][]string) (words, mentions, hashtags, length, reading int) {
	length = utf8.RuneCountInString(content)
	words = len(strings.Fields(content))
	mentions = countTagName(tags, "p")
	hashtags = countTagName(tags, "t")
	if kind == 30023 && words > 0 {
		reading = words * 60 / 200
		if reading < 1 {
			reading = 1
		}
	}
	return words, mentions, hashtags, length, reading
}

func countTagName(tags [][]string, name string) int {
	n := 0
	for _, tag := range tags {
		if len(tag) >= 2 && tag[0] == name && strings.TrimSpace(tag[1]) != "" {
			n++
		}
	}
	return n
}

func parseSelfLabels(tags [][]string) []selfLabel {
	namespaces := make([]string, 0)
	labels := make([]selfLabel, 0)
	for _, tag := range tags {
		if len(tag) < 2 {
			continue
		}
		switch tag[0] {
		case "L":
			if ns := strings.TrimSpace(tag[1]); ns != "" {
				namespaces = append(namespaces, trimRunes(ns, 80))
			}
		case "l":
			label := strings.TrimSpace(tag[1])
			if label == "" {
				continue
			}
			ns := ""
			if len(tag) >= 3 {
				ns = strings.TrimSpace(tag[2])
			}
			labels = append(labels, selfLabel{Namespace: trimRunes(ns, 80), Label: trimRunes(label, 80)})
		}
	}
	if len(namespaces) == 1 {
		for i := range labels {
			if labels[i].Namespace == "" {
				labels[i].Namespace = namespaces[0]
			}
		}
	}
	return labels
}

func parseExternalIdentities(tags [][]string) []externalIdentity {
	out := make([]externalIdentity, 0)
	seen := map[string]struct{}{}
	for _, tag := range tags {
		if len(tag) < 2 || tag[0] != "i" {
			continue
		}
		platform, name, ok := strings.Cut(strings.TrimSpace(tag[1]), ":")
		platform = strings.ToLower(strings.TrimSpace(platform))
		name = strings.TrimSpace(name)
		if !ok || platform == "" || name == "" {
			continue
		}
		if _, dup := seen[platform]; dup {
			continue
		}
		seen[platform] = struct{}{}
		proof := ""
		if len(tag) >= 3 {
			proof = trimRunes(strings.TrimSpace(tag[2]), 200)
		}
		out = append(out, externalIdentity{
			Platform: trimRunes(platform, 32),
			Identity: trimRunes(name, 120),
			Proof:    proof,
		})
	}
	return out
}

func relayHints(tags [][]string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0)
	for _, tag := range tags {
		if len(tag) < 3 || (tag[0] != "e" && tag[0] != "p") {
			continue
		}
		raw := strings.TrimSpace(tag[2])
		if !strings.Contains(raw, "://") {
			continue
		}
		normalized, err := relayurl.Normalize(raw, relayurl.NormalizeOptions{})
		if err != nil {
			continue
		}
		if err := relayurl.Validate(normalized, relayurl.ValidateOptions{}); err != nil {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out
}

func emojiShortcodes(kind int, tags [][]string) []string {
	if kind != 0 && kind != 1 && kind != 7 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0)
	for _, tag := range tags {
		if len(tag) < 2 || tag[0] != "emoji" {
			continue
		}
		code := strings.Trim(strings.TrimSpace(tag[1]), ":")
		code = trimRunes(code, 64)
		if code == "" {
			continue
		}
		if _, ok := seen[code]; ok {
			continue
		}
		seen[code] = struct{}{}
		out = append(out, code)
	}
	return out
}

func trimRunes(value string, maxRunes int) string {
	if maxRunes <= 0 || utf8.RuneCountInString(value) <= maxRunes {
		return value
	}
	return string([]rune(value)[:maxRunes])
}

func projectTagEnrichmentTx(
	ctx context.Context,
	tx pgx.Tx,
	eventID, author string,
	kind int,
	createdAt int64,
	tags [][]string,
) error {
	if _, err := tx.Exec(ctx, `
		DELETE FROM event_labels
		WHERE event_id = $1 AND is_self
	`, eventID); err != nil {
		return err
	}
	for _, label := range parseSelfLabels(tags) {
		if _, err := tx.Exec(ctx, `
			INSERT INTO event_labels (
				event_id, labeler_pubkey, namespace, label, is_self, target_type, target_value
			)
			VALUES ($1, $2, $3, $4, TRUE, 'e', $1)
			ON CONFLICT DO NOTHING
		`, eventID, author, label.Namespace, label.Label); err != nil {
			return err
		}
	}
	claimed, err := claimStatDeltaTx(ctx, tx, eventID, "tag_enrichment")
	if err != nil || !claimed {
		return err
	}
	day := time.Unix(createdAt, 0).UTC().Format("2006-01-02")
	if n := len(parseSelfLabels(tags)); n > 0 {
		if _, err := tx.Exec(ctx, `
			INSERT INTO labels_daily (day, label_count) VALUES ($1::date, $2)
			ON CONFLICT (day) DO UPDATE SET label_count = labels_daily.label_count + EXCLUDED.label_count
		`, day, n); err != nil {
			return err
		}
	}
	for _, code := range emojiShortcodes(kind, tags) {
		if _, err := tx.Exec(ctx, `
			INSERT INTO custom_emoji_daily (day, shortcode, count)
			VALUES ($1::date, $2, 1)
			ON CONFLICT (day, shortcode) DO UPDATE
			SET count = custom_emoji_daily.count + 1
		`, day, code); err != nil {
			return err
		}
	}
	for _, hint := range relayHints(tags) {
		if _, err := tx.Exec(ctx, `
			INSERT INTO relay_hint_daily (day, relay_url, hint_count)
			VALUES ($1::date, $2, 1)
			ON CONFLICT (day, relay_url) DO UPDATE
			SET hint_count = relay_hint_daily.hint_count + 1
		`, day, hint); err != nil {
			return err
		}
	}
	for _, label := range parseSelfLabels(tags) {
		if !strings.EqualFold(label.Namespace, "ISO-639-1") || label.Label == "" {
			continue
		}
		var detected string
		err := tx.QueryRow(ctx, `
			SELECT COALESCE(primary_language, '')
			FROM note_discovery_stats
			WHERE event_id = $1
		`, eventID).Scan(&detected)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			return err
		}
		if detected == "" {
			continue
		}
		column := "disagree"
		if strings.EqualFold(detected, label.Label) {
			column = "agree"
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO language_label_agreement_daily (day, agree, disagree)
			VALUES ($1::date, CASE WHEN $2 THEN 1 ELSE 0 END, CASE WHEN $2 THEN 0 ELSE 1 END)
			ON CONFLICT (day) DO UPDATE
			SET agree = language_label_agreement_daily.agree + EXCLUDED.agree,
			    disagree = language_label_agreement_daily.disagree + EXCLUDED.disagree
		`, day, column == "agree"); err != nil {
			return err
		}
	}
	return nil
}

func replaceExternalIdentitiesTx(ctx context.Context, tx pgx.Tx, pubkey string, tags [][]string) error {
	if _, err := tx.Exec(ctx, `DELETE FROM profile_external_identities WHERE pubkey = $1`, pubkey); err != nil {
		return err
	}
	for _, identity := range parseExternalIdentities(tags) {
		if _, err := tx.Exec(ctx, `
			INSERT INTO profile_external_identities (pubkey, platform, identity, proof)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (pubkey, platform) DO UPDATE
			SET identity = EXCLUDED.identity,
			    proof = EXCLUDED.proof,
			    updated_at = now()
		`, pubkey, identity.Platform, identity.Identity, identity.Proof); err != nil {
			return err
		}
	}
	return nil
}
