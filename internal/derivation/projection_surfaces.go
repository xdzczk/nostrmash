package derivation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/xdzczk/nostrmash/internal/nostr"
	"github.com/xdzczk/nostrmash/internal/relayurl"
)

func (h *Handlers) ProjectSurfaces(ctx context.Context, eventID string) error {
	return h.projectSurfacesWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectSurfacesWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	var kind int
	var pubkey, content string
	var createdAt int64
	if err := h.pool.QueryRow(ctx, `
		SELECT kind, pubkey, created_at, content FROM events WHERE id = $1
	`, eventID).Scan(&kind, &pubkey, &createdAt, &content); err != nil {
		return fmt.Errorf("load event for surfaces: %w", err)
	}
	if !isSurfaceKind(kind) {
		return nil
	}
	tags, err := h.loadEventTags(ctx, eventID)
	if err != nil {
		return err
	}
	var relayURL string
	_ = h.pool.QueryRow(ctx, `
		SELECT relay_url FROM event_relays WHERE event_id = $1 ORDER BY seen_at ASC LIMIT 1
	`, eventID).Scan(&relayURL)
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin surfaces tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := resolveDerivationWriteVersion(ctx, tx, DerivationSurfaces, SurfacesVersion,
		"Project groups, live chat, polls, badges, goals, lists, labels, and file metadata", versionOverride); err != nil {
		return err
	}
	switch kind {
	case 9, 11:
		err = projectGroupMessageTx(ctx, tx, eventID, pubkey, relayURL, createdAt, kind, tags)
	case 39000, 39001, 39002, 39003:
		err = projectGroupMetadataTx(ctx, tx, eventID, relayURL, kind, tags)
	case 1311:
		err = projectLiveChatTx(ctx, tx, eventID, pubkey, createdAt, tags)
	case 1068:
		err = projectPollTx(ctx, tx, eventID, pubkey, content, tags)
	case 1018:
		err = projectPollVoteTx(ctx, tx, eventID, pubkey, createdAt, tags)
	case 30009:
		err = projectBadgeDefinitionTx(ctx, tx, pubkey, tags)
	case 8:
		err = projectBadgeAwardTx(ctx, tx, eventID, createdAt, tags)
	case 30008:
		err = projectAcceptedBadgesTx(ctx, tx, pubkey, tags)
	case 9041:
		err = projectZapGoalTx(ctx, tx, eventID, pubkey, content, tags)
	case 30315:
		err = projectUserStatusTx(ctx, tx, pubkey, content, tags)
	case 30004, 30005:
		err = projectCurationSetTx(ctx, tx, pubkey, kind, tags)
	case 10015, 30015:
		err = projectInterestListTx(ctx, tx, pubkey, kind, tags)
	case 30002, 10006, 10007, 10012:
		err = projectRelayPreferenceTx(ctx, tx, pubkey, kind, tags)
	case 10063:
		err = projectBlossomTx(ctx, tx, pubkey, tags)
	case 10030:
		err = projectEmojiSetTx(ctx, tx, pubkey, tags)
	case 10009:
		err = projectGroupListTx(ctx, tx, pubkey, tags)
	case 1985:
		err = projectLabelEventTx(ctx, tx, eventID, pubkey, tags)
	case 1063:
		err = projectFileMetadataTx(ctx, tx, eventID, pubkey, createdAt, tags)
	case 10000:
		err = projectMuteListTx(ctx, tx, pubkey, tags)
	}
	if err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit surfaces: %w", err)
	}
	return nil
}

func isSurfaceKind(kind int) bool {
	switch kind {
	case 8, 9, 11, 1018, 1063, 1068, 1311, 1985, 9041,
		10000, 10006, 10007, 10009, 10012, 10015, 10030, 10063,
		30002, 30004, 30005, 30008, 30009, 30015, 30315,
		39000, 39001, 39002, 39003:
		return true
	default:
		return false
	}
}

func projectGroupMessageTx(ctx context.Context, tx pgx.Tx, eventID, pubkey, relayURL string, createdAt int64, kind int, tags [][]string) error {
	groupID := firstTagValue(tags, "h")
	if groupID == "" || relayURL == "" {
		return nil
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO group_messages (event_id, relay_url, group_id, author, created_at, kind)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (event_id) DO NOTHING
	`, eventID, relayURL, groupID, pubkey, createdAt, kind)
	if err != nil {
		return fmt.Errorf("insert group message: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil
	}
	claimed, err := claimStatDeltaTx(ctx, tx, eventID, "group_message")
	if err != nil || !claimed {
		return err
	}
	day := time.Unix(createdAt, 0).UTC().Format("2006-01-02")
	authorTag, err := tx.Exec(ctx, `
		INSERT INTO group_author_daily (relay_url, group_id, day, pubkey)
		VALUES ($1, $2, $3::date, $4)
		ON CONFLICT DO NOTHING
	`, relayURL, groupID, day, pubkey)
	if err != nil {
		return fmt.Errorf("insert group author: %w", err)
	}
	unique := int64(0)
	if authorTag.RowsAffected() > 0 {
		unique = 1
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO group_activity_daily (relay_url, group_id, day, message_count, unique_authors)
		VALUES ($1, $2, $3::date, 1, $4)
		ON CONFLICT (relay_url, group_id, day) DO UPDATE
		SET message_count = group_activity_daily.message_count + 1,
		    unique_authors = group_activity_daily.unique_authors + EXCLUDED.unique_authors
	`, relayURL, groupID, day, unique); err != nil {
		return fmt.Errorf("upsert group activity: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO groups_latest (relay_url, group_id)
		VALUES ($1, $2)
		ON CONFLICT (relay_url, group_id) DO NOTHING
	`, relayURL, groupID)
	return err
}

func projectGroupMetadataTx(ctx context.Context, tx pgx.Tx, eventID, relayURL string, kind int, tags [][]string) error {
	groupID := firstTagValue(tags, "h")
	if groupID == "" {
		groupID = firstTagValue(tags, "d")
	}
	if groupID == "" || relayURL == "" {
		return nil
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO groups_latest (relay_url, group_id) VALUES ($1, $2)
		ON CONFLICT (relay_url, group_id) DO NOTHING
	`, relayURL, groupID); err != nil {
		return err
	}
	switch kind {
	case 39000:
		_, err := tx.Exec(ctx, `
			UPDATE groups_latest
			SET name = $3, about = $4, picture = $5, is_public = $6, is_open = $7,
			    metadata_event_id = $8, updated_at = now()
			WHERE relay_url = $1 AND group_id = $2
		`, relayURL, groupID, trimRunes(firstTagValue(tags, "name"), 160), trimRunes(firstTagValue(tags, "about"), 2000),
			firstTagValue(tags, "picture"), tagNamePresent(tags, "public") && !tagNamePresent(tags, "private"),
			tagNamePresent(tags, "open"), eventID)
		return err
	case 39001:
		_, err := tx.Exec(ctx, `
			UPDATE groups_latest SET admin_count = $3, updated_at = now()
			WHERE relay_url = $1 AND group_id = $2
		`, relayURL, groupID, len(tagValues(tags, "p")))
		return err
	case 39002:
		_, err := tx.Exec(ctx, `
			UPDATE groups_latest SET member_count = $3, updated_at = now()
			WHERE relay_url = $1 AND group_id = $2
		`, relayURL, groupID, len(tagValues(tags, "p")))
		return err
	default:
		return nil
	}
}

func projectLiveChatTx(ctx context.Context, tx pgx.Tx, eventID, pubkey string, createdAt int64, tags [][]string) error {
	addr, ok := nostr.ParseAddressCoordinate(firstTagValue(tags, "a"))
	if !ok {
		return nil
	}
	address := addr.String()
	claimed, err := claimStatDeltaTx(ctx, tx, eventID, "live_chat")
	if err != nil || !claimed {
		return err
	}
	day := time.Unix(createdAt, 0).UTC().Format("2006-01-02")
	authorTag, err := tx.Exec(ctx, `
		INSERT INTO live_chat_author_daily (day, address, pubkey)
		VALUES ($1::date, $2, $3)
		ON CONFLICT DO NOTHING
	`, day, address, pubkey)
	if err != nil {
		return err
	}
	unique := int64(0)
	if authorTag.RowsAffected() > 0 {
		unique = 1
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO live_activity_stats (address, chat_count, unique_chatters, last_chat_at)
		VALUES ($1, 1, $2, $3)
		ON CONFLICT (address) DO UPDATE
		SET chat_count = live_activity_stats.chat_count + 1,
		    unique_chatters = live_activity_stats.unique_chatters + EXCLUDED.unique_chatters,
		    last_chat_at = GREATEST(live_activity_stats.last_chat_at, EXCLUDED.last_chat_at)
	`, address, unique, createdAt); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO live_chat_daily (day, address, chat_count, unique_chatters)
		VALUES ($1::date, $2, 1, $3)
		ON CONFLICT (day, address) DO UPDATE
		SET chat_count = live_chat_daily.chat_count + 1,
		    unique_chatters = live_chat_daily.unique_chatters + EXCLUDED.unique_chatters
	`, day, address, unique)
	return err
}

func projectPollTx(ctx context.Context, tx pgx.Tx, eventID, pubkey, content string, tags [][]string) error {
	options := make([]map[string]string, 0)
	for _, tag := range tags {
		if len(tag) < 2 || tag[0] != "option" {
			continue
		}
		label := tag[1]
		id := tag[1]
		if len(tag) >= 3 && strings.TrimSpace(tag[2]) != "" {
			label = tag[2]
		}
		options = append(options, map[string]string{"id": id, "label": label})
	}
	raw, _ := json.Marshal(options)
	pollType := firstTagValue(tags, "polltype")
	if pollType == "" {
		pollType = "singlechoice"
	}
	var ends *int64
	if rawEnds := firstTagValue(tags, "ends"); rawEnds != "" {
		if n, err := strconv.ParseInt(rawEnds, 10, 64); err == nil {
			ends = &n
		}
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO polls (event_id, author, question, options, poll_type, ends_at)
		VALUES ($1, $2, $3, $4::jsonb, $5, $6)
		ON CONFLICT (event_id) DO UPDATE
		SET question = EXCLUDED.question, options = EXCLUDED.options, poll_type = EXCLUDED.poll_type, ends_at = EXCLUDED.ends_at
	`, eventID, pubkey, trimRunes(content, 2000), raw, pollType, ends)
	return err
}

func projectPollVoteTx(ctx context.Context, tx pgx.Tx, eventID, voter string, createdAt int64, tags [][]string) error {
	pollID := firstTagValue(tags, "e")
	if pollID == "" {
		return nil
	}
	var ends *int64
	err := tx.QueryRow(ctx, `SELECT ends_at FROM polls WHERE event_id = $1`, pollID).Scan(&ends)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if ends != nil && createdAt > *ends {
		return nil
	}
	options := tagValues(tags, "response")
	if len(options) == 0 {
		return nil
	}
	raw, _ := json.Marshal(options)
	var previous []byte
	var previousAt int64
	err = tx.QueryRow(ctx, `
		SELECT option_ids, created_at FROM poll_votes WHERE poll_event_id = $1 AND voter = $2
	`, pollID, voter).Scan(&previous, &previousAt)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if err == nil && previousAt >= createdAt {
		return nil
	}
	claimed, claimErr := claimStatDeltaTx(ctx, tx, eventID, "poll_vote")
	if claimErr != nil || !claimed {
		return claimErr
	}
	if len(previous) > 0 {
		var oldIDs []string
		_ = json.Unmarshal(previous, &oldIDs)
		for _, id := range oldIDs {
			if _, err := tx.Exec(ctx, `
				UPDATE poll_option_counts SET votes = GREATEST(votes - 1, 0)
				WHERE poll_event_id = $1 AND option_id = $2
			`, pollID, id); err != nil {
				return err
			}
		}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO poll_votes (poll_event_id, voter, option_ids, created_at)
		VALUES ($1, $2, $3::jsonb, $4)
		ON CONFLICT (poll_event_id, voter) DO UPDATE
		SET option_ids = EXCLUDED.option_ids, created_at = EXCLUDED.created_at
	`, pollID, voter, raw, createdAt); err != nil {
		return err
	}
	for _, id := range options {
		if _, err := tx.Exec(ctx, `
			INSERT INTO poll_option_counts (poll_event_id, option_id, votes)
			VALUES ($1, $2, 1)
			ON CONFLICT (poll_event_id, option_id) DO UPDATE
			SET votes = poll_option_counts.votes + 1
		`, pollID, id); err != nil {
			return err
		}
	}
	return nil
}

func projectBadgeDefinitionTx(ctx context.Context, tx pgx.Tx, pubkey string, tags [][]string) error {
	dTag := firstTagValue(tags, "d")
	if dTag == "" {
		return nil
	}
	address := "30009:" + strings.ToLower(pubkey) + ":" + dTag
	_, err := tx.Exec(ctx, `
		INSERT INTO badge_definitions (address, issuer, name, description, image, thumb)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (address) DO UPDATE
		SET name = EXCLUDED.name, description = EXCLUDED.description, image = EXCLUDED.image,
		    thumb = EXCLUDED.thumb, updated_at = now()
	`, address, pubkey, firstTagValue(tags, "name"), firstTagValue(tags, "description"),
		firstTagValue(tags, "image"), firstTagValue(tags, "thumb"))
	return err
}

func projectBadgeAwardTx(ctx context.Context, tx pgx.Tx, eventID string, createdAt int64, tags [][]string) error {
	claimed, err := claimStatDeltaTx(ctx, tx, eventID, "badge_award")
	if err != nil || !claimed {
		return err
	}
	for _, address := range tagValues(tags, "a") {
		for _, recipient := range tagValues(tags, "p") {
			if _, err := tx.Exec(ctx, `
				INSERT INTO badge_awards (award_event_id, definition_address, recipient, awarded_at)
				VALUES ($1, $2, $3, $4)
				ON CONFLICT DO NOTHING
			`, eventID, address, strings.ToLower(recipient), createdAt); err != nil {
				return err
			}
		}
	}
	return nil
}

func projectAcceptedBadgesTx(ctx context.Context, tx pgx.Tx, pubkey string, tags [][]string) error {
	raw, _ := json.Marshal(tagValues(tags, "a"))
	_, err := tx.Exec(ctx, `
		INSERT INTO profile_badges_latest (pubkey, accepted)
		VALUES ($1, $2::jsonb)
		ON CONFLICT (pubkey) DO UPDATE
		SET accepted = EXCLUDED.accepted, updated_at = now()
	`, pubkey, raw)
	return err
}

func projectZapGoalTx(ctx context.Context, tx pgx.Tx, eventID, pubkey, content string, tags [][]string) error {
	var amount int64
	if raw := firstTagValue(tags, "amount"); raw != "" {
		amount, _ = strconv.ParseInt(raw, 10, 64)
	}
	var closed *int64
	if raw := firstTagValue(tags, "closed_at"); raw != "" {
		if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
			closed = &n
		}
	}
	relays, _ := json.Marshal(tagValues(tags, "relay"))
	var raised int64
	_ = tx.QueryRow(ctx, `
		SELECT COALESCE(sum(amount_sats), 0) FROM zap_receipts WHERE event_id = $1
	`, eventID).Scan(&raised)
	_, err := tx.Exec(ctx, `
		INSERT INTO zap_goals (event_id, author, amount_msats, raised_sats, closed_at, summary, relays)
		VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
		ON CONFLICT (event_id) DO UPDATE
		SET amount_msats = EXCLUDED.amount_msats, raised_sats = EXCLUDED.raised_sats,
		    closed_at = EXCLUDED.closed_at, summary = EXCLUDED.summary, relays = EXCLUDED.relays
	`, eventID, pubkey, amount, raised, closed, trimRunes(content, 2000), relays)
	return err
}

func projectUserStatusTx(ctx context.Context, tx pgx.Tx, pubkey, content string, tags [][]string) error {
	dTag := firstTagValue(tags, "d")
	link := firstTagValue(tags, "r")
	if link == "" {
		link = firstTagValue(tags, "url")
	}
	var expires *int64
	if raw := firstTagValue(tags, "expiration"); raw != "" {
		if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
			expires = &n
		}
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO user_status_latest (pubkey, d_tag, content, link, expires_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (pubkey, d_tag) DO UPDATE
		SET content = EXCLUDED.content, link = EXCLUDED.link, expires_at = EXCLUDED.expires_at, updated_at = now()
	`, pubkey, dTag, trimRunes(content, 2000), link, expires)
	return err
}

func projectCurationSetTx(ctx context.Context, tx pgx.Tx, pubkey string, kind int, tags [][]string) error {
	dTag := firstTagValue(tags, "d")
	if dTag == "" {
		return nil
	}
	address := strconv.Itoa(kind) + ":" + strings.ToLower(pubkey) + ":" + dTag
	type item struct{ kind, value string }
	items := make([]item, 0)
	for _, tag := range tags {
		if len(tag) < 2 {
			continue
		}
		if tag[0] == "a" || tag[0] == "e" {
			items = append(items, item{tag[0], tag[1]})
		}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO curation_sets (address, author, kind, title, description, image, item_count)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (address) DO UPDATE
		SET title = EXCLUDED.title, description = EXCLUDED.description, image = EXCLUDED.image,
		    item_count = EXCLUDED.item_count, updated_at = now()
	`, address, pubkey, kind, firstTagValue(tags, "title"), firstTagValue(tags, "description"),
		firstTagValue(tags, "image"), len(items)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM curation_set_items WHERE address = $1`, address); err != nil {
		return err
	}
	for _, it := range items {
		if _, err := tx.Exec(ctx, `
			INSERT INTO curation_set_items (address, item_type, item_value)
			VALUES ($1, $2, $3)
			ON CONFLICT DO NOTHING
		`, address, it.kind, it.value); err != nil {
			return err
		}
	}
	return nil
}

func projectInterestListTx(ctx context.Context, tx pgx.Tx, pubkey string, kind int, tags [][]string) error {
	next := make([]string, 0)
	seen := map[string]struct{}{}
	for _, tag := range tagValues(tags, "t") {
		tag = strings.ToLower(strings.TrimSpace(tag))
		if tag == "" {
			continue
		}
		if _, ok := seen[tag]; ok {
			continue
		}
		seen[tag] = struct{}{}
		next = append(next, tag)
	}
	var previousRaw []byte
	_ = tx.QueryRow(ctx, `
		SELECT hashtags FROM interest_lists_latest WHERE pubkey = $1 AND kind = $2
	`, pubkey, kind).Scan(&previousRaw)
	var previous []string
	_ = json.Unmarshal(previousRaw, &previous)
	prevSet := map[string]struct{}{}
	for _, tag := range previous {
		prevSet[tag] = struct{}{}
	}
	for _, tag := range previous {
		if _, ok := seen[tag]; ok {
			continue
		}
		if _, err := tx.Exec(ctx, `
			UPDATE hashtag_interest_counts SET follower_count = GREATEST(follower_count - 1, 0) WHERE hashtag = $1
		`, tag); err != nil {
			return err
		}
	}
	for _, tag := range next {
		if _, ok := prevSet[tag]; ok {
			continue
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO hashtag_interest_counts (hashtag, follower_count) VALUES ($1, 1)
			ON CONFLICT (hashtag) DO UPDATE SET follower_count = hashtag_interest_counts.follower_count + 1
		`, tag); err != nil {
			return err
		}
	}
	raw, _ := json.Marshal(next)
	_, err := tx.Exec(ctx, `
		INSERT INTO interest_lists_latest (pubkey, kind, hashtags)
		VALUES ($1, $2, $3::jsonb)
		ON CONFLICT (pubkey, kind) DO UPDATE
		SET hashtags = EXCLUDED.hashtags, updated_at = now()
	`, pubkey, kind, raw)
	return err
}

func projectRelayPreferenceTx(ctx context.Context, tx pgx.Tx, pubkey string, kind int, tags [][]string) error {
	column := relayRefColumn(kind)
	next := normalizeRelayList(append(tagValues(tags, "relay"), tagValues(tags, "r")...))
	var previousRaw []byte
	_ = tx.QueryRow(ctx, `
		SELECT relays FROM relay_preference_lists_latest WHERE pubkey = $1 AND kind = $2
	`, pubkey, kind).Scan(&previousRaw)
	if err := diffRelayRefColumnTx(ctx, tx, column, previousRaw, next); err != nil {
		return err
	}
	raw, _ := json.Marshal(next)
	_, err := tx.Exec(ctx, `
		INSERT INTO relay_preference_lists_latest (pubkey, kind, relays)
		VALUES ($1, $2, $3::jsonb)
		ON CONFLICT (pubkey, kind) DO UPDATE
		SET relays = EXCLUDED.relays, updated_at = now()
	`, pubkey, kind, raw)
	return err
}

func projectBlossomTx(ctx context.Context, tx pgx.Tx, pubkey string, tags [][]string) error {
	raw, _ := json.Marshal(tagValues(tags, "server"))
	_, err := tx.Exec(ctx, `
		INSERT INTO blossom_servers_latest (pubkey, servers)
		VALUES ($1, $2::jsonb)
		ON CONFLICT (pubkey) DO UPDATE SET servers = EXCLUDED.servers, updated_at = now()
	`, pubkey, raw)
	return err
}

func projectEmojiSetTx(ctx context.Context, tx pgx.Tx, pubkey string, tags [][]string) error {
	raw, _ := json.Marshal(tagValues(tags, "emoji"))
	if string(raw) == "null" || string(raw) == "[]" {
		raw, _ = json.Marshal(tagValues(tags, "a"))
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO emoji_sets_latest (pubkey, shortcodes)
		VALUES ($1, $2::jsonb)
		ON CONFLICT (pubkey) DO UPDATE SET shortcodes = EXCLUDED.shortcodes, updated_at = now()
	`, pubkey, raw)
	return err
}

func projectGroupListTx(ctx context.Context, tx pgx.Tx, pubkey string, tags [][]string) error {
	groups := tagValues(tags, "group")
	if len(groups) == 0 {
		groups = tagValues(tags, "a")
	}
	raw, _ := json.Marshal(groups)
	_, err := tx.Exec(ctx, `
		INSERT INTO group_lists_latest (pubkey, groups)
		VALUES ($1, $2::jsonb)
		ON CONFLICT (pubkey) DO UPDATE SET groups = EXCLUDED.groups, updated_at = now()
	`, pubkey, raw)
	return err
}

func projectLabelEventTx(ctx context.Context, tx pgx.Tx, eventID, pubkey string, tags [][]string) error {
	if _, err := tx.Exec(ctx, `DELETE FROM event_labels WHERE event_id = $1 AND is_self = false`, eventID); err != nil {
		return err
	}
	labels := parseSelfLabels(tags)
	targets := labelTargets(tags)
	for _, label := range labels {
		for _, target := range targets {
			if _, err := tx.Exec(ctx, `
				INSERT INTO event_labels (event_id, labeler_pubkey, namespace, label, is_self, target_type, target_value)
				VALUES ($1, $2, $3, $4, false, $5, $6)
				ON CONFLICT DO NOTHING
			`, eventID, pubkey, label.Namespace, label.Label, target.kind, target.value); err != nil {
				return err
			}
		}
	}
	return nil
}

func projectFileMetadataTx(ctx context.Context, tx pgx.Tx, eventID, pubkey string, createdAt int64, tags [][]string) error {
	fileURL := firstTagValue(tags, "url")
	if fileURL == "" {
		fileURL = firstTagValue(tags, "x")
	}
	var size int64
	if raw := firstTagValue(tags, "size"); raw != "" {
		size, _ = strconv.ParseInt(raw, 10, 64)
	}
	mime := firstTagValue(tags, "m")
	if _, err := tx.Exec(ctx, `
		INSERT INTO file_metadata (event_id, url, mime, size_bytes, dim, blurhash)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (event_id) DO UPDATE
		SET url = EXCLUDED.url, mime = EXCLUDED.mime, size_bytes = EXCLUDED.size_bytes,
		    dim = EXCLUDED.dim, blurhash = EXCLUDED.blurhash
	`, eventID, fileURL, mime, size, firstTagValue(tags, "dim"), firstTagValue(tags, "blurhash")); err != nil {
		return err
	}
	if fileURL == "" || !strings.Contains(fileURL, "://") {
		return nil
	}
	parsed, err := url.Parse(fileURL)
	if err != nil || parsed.Host == "" {
		return nil
	}
	host := strings.TrimPrefix(strings.ToLower(parsed.Hostname()), "www.")
	if _, err := tx.Exec(ctx, `
		INSERT INTO event_urls (event_id, author_pubkey, created_at, url, domain, canonical_domain, derivation_version, is_media)
		VALUES ($1, $2, $3, $4, $5, $5, 1, true)
		ON CONFLICT (event_id, url) DO UPDATE
		SET is_media = true, canonical_domain = EXCLUDED.canonical_domain, projected_at = now()
	`, eventID, pubkey, createdAt, fileURL, host); err != nil {
		return err
	}
	claimed, err := claimStatDeltaTx(ctx, tx, eventID, "media_host")
	if err != nil || !claimed {
		return err
	}
	return incrementMediaHostTx(ctx, tx, pubkey, createdAt, host, mime)
}

type labelTarget struct{ kind, value string }

func labelTargets(tags [][]string) []labelTarget {
	out := make([]labelTarget, 0)
	for _, tag := range tags {
		if len(tag) < 2 {
			continue
		}
		switch tag[0] {
		case "e", "p", "a", "r", "t":
			out = append(out, labelTarget{tag[0], tag[1]})
		}
	}
	return out
}

func tagNamePresent(tags [][]string, name string) bool {
	for _, tag := range tags {
		if len(tag) > 0 && tag[0] == name {
			return true
		}
	}
	return false
}

func relayRefColumn(kind int) string {
	switch kind {
	case 10006:
		return "blocked_ref_count"
	case 10007:
		return "search_ref_count"
	case 10012, 30002:
		return "favorite_ref_count"
	default:
		return ""
	}
}

func normalizeRelayList(raw []string) []string {
	out := make([]string, 0, len(raw))
	seen := map[string]struct{}{}
	for _, item := range raw {
		normalized, err := relayurl.Normalize(item, relayurl.NormalizeOptions{})
		if err != nil {
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

func diffRelayRefColumnTx(ctx context.Context, tx pgx.Tx, column string, previousRaw []byte, next []string) error {
	if column == "" {
		return nil
	}
	var previous []string
	_ = json.Unmarshal(previousRaw, &previous)
	prev := map[string]struct{}{}
	for _, item := range normalizeRelayList(previous) {
		prev[item] = struct{}{}
	}
	nextSet := map[string]struct{}{}
	for _, item := range next {
		nextSet[item] = struct{}{}
	}
	for item := range prev {
		if _, ok := nextSet[item]; ok {
			continue
		}
		if err := addRelayRefTx(ctx, tx, column, item, -1); err != nil {
			return err
		}
	}
	for item := range nextSet {
		if _, ok := prev[item]; ok {
			continue
		}
		if err := addRelayRefTx(ctx, tx, column, item, 1); err != nil {
			return err
		}
	}
	return nil
}

func addRelayRefTx(ctx context.Context, tx pgx.Tx, column, relayURL string, delta int) error {
	switch column {
	case "search_ref_count", "blocked_ref_count", "favorite_ref_count", "dm_ref_count":
	default:
		return fmt.Errorf("unknown relay ref column")
	}
	_, err := tx.Exec(ctx, `
		UPDATE relay_registry
		SET `+column+` = GREATEST(`+column+` + $2, 0)
		WHERE normalized_url = $1
	`, relayURL, delta)
	return err
}

func (h *Handlers) recordProjectedMediaHostsTx(ctx context.Context, tx pgx.Tx, eventID, pubkey string, createdAt int64, urls []normalizedEventURL) error {
	hosts := make([]string, 0)
	seen := map[string]struct{}{}
	for _, row := range urls {
		if row.CanonicalDomain == "" || !mediaURLPattern.MatchString(row.URL) {
			continue
		}
		if _, ok := seen[row.CanonicalDomain]; ok {
			continue
		}
		seen[row.CanonicalDomain] = struct{}{}
		hosts = append(hosts, row.CanonicalDomain)
	}
	tags, err := h.loadEventTags(ctx, eventID)
	if err != nil {
		return err
	}
	mimes := imetaMimes(tags)
	if len(hosts) == 0 && len(mimes) == 0 {
		return nil
	}
	claimed, err := claimStatDeltaTx(ctx, tx, eventID, "media_host")
	if err != nil || !claimed {
		return err
	}
	for _, host := range hosts {
		if err := incrementMediaHostTx(ctx, tx, pubkey, createdAt, host, ""); err != nil {
			return err
		}
	}
	day := time.Unix(createdAt, 0).UTC().Format("2006-01-02")
	for _, mime := range mimes {
		if _, err := tx.Exec(ctx, `
			INSERT INTO media_mime_daily (day, mime, media_count)
			VALUES ($1::date, $2, 1)
			ON CONFLICT (day, mime) DO UPDATE
			SET media_count = media_mime_daily.media_count + 1
		`, day, mime); err != nil {
			return err
		}
	}
	return nil
}

func imetaMimes(tags [][]string) []string {
	out := make([]string, 0)
	seen := map[string]struct{}{}
	for _, tag := range tags {
		if len(tag) == 0 || tag[0] != "imeta" {
			continue
		}
		for _, part := range tag[1:] {
			if !strings.HasPrefix(part, "m ") {
				continue
			}
			mime := strings.TrimSpace(strings.TrimPrefix(part, "m "))
			if mime == "" {
				continue
			}
			if _, ok := seen[mime]; ok {
				continue
			}
			seen[mime] = struct{}{}
			out = append(out, mime)
		}
	}
	return out
}

func incrementMediaHostTx(ctx context.Context, tx pgx.Tx, pubkey string, createdAt int64, host, mime string) error {
	if host == "" {
		return nil
	}
	day := time.Unix(createdAt, 0).UTC().Format("2006-01-02")
	authorTag, err := tx.Exec(ctx, `
		INSERT INTO media_host_author_daily (day, canonical_domain, pubkey)
		VALUES ($1::date, $2, $3)
		ON CONFLICT DO NOTHING
	`, day, host, pubkey)
	if err != nil {
		return err
	}
	authors := int64(0)
	if authorTag.RowsAffected() > 0 {
		authors = 1
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO media_host_daily (day, canonical_domain, media_count, authors)
		VALUES ($1::date, $2, 1, $3)
		ON CONFLICT (day, canonical_domain) DO UPDATE
		SET media_count = media_host_daily.media_count + 1,
		    authors = media_host_daily.authors + EXCLUDED.authors
	`, day, host, authors); err != nil {
		return err
	}
	if mime == "" {
		return nil
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO media_mime_daily (day, mime, media_count)
		VALUES ($1::date, $2, 1)
		ON CONFLICT (day, mime) DO UPDATE
		SET media_count = media_mime_daily.media_count + 1
	`, day, mime)
	return err
}

func projectMuteListTx(ctx context.Context, tx pgx.Tx, pubkey string, tags [][]string) error {
	rows, err := tx.Query(ctx, `SELECT muted FROM mute_edges WHERE muter = $1`, pubkey)
	if err != nil {
		return err
	}
	previous := make([]string, 0)
	for rows.Next() {
		var muted string
		if err := rows.Scan(&muted); err != nil {
			rows.Close()
			return err
		}
		previous = append(previous, muted)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM mute_edges WHERE muter = $1`, pubkey); err != nil {
		return err
	}
	for _, muted := range previous {
		if _, err := tx.Exec(ctx, `
			UPDATE profile_moderation_stats
			SET muted_by_count = GREATEST(muted_by_count - 1, 0)
			WHERE pubkey = $1
		`, muted); err != nil {
			return err
		}
	}
	seen := map[string]struct{}{}
	for _, muted := range tagValues(tags, "p") {
		muted = strings.TrimSpace(muted)
		if muted == "" || muted == pubkey {
			continue
		}
		if _, ok := seen[muted]; ok {
			continue
		}
		seen[muted] = struct{}{}
		if _, err := tx.Exec(ctx, `
			INSERT INTO mute_edges (muter, muted) VALUES ($1, $2)
			ON CONFLICT DO NOTHING
		`, pubkey, muted); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO profile_moderation_stats (pubkey, muted_by_count)
			VALUES ($1, 1)
			ON CONFLICT (pubkey) DO UPDATE
			SET muted_by_count = profile_moderation_stats.muted_by_count + 1
		`, muted); err != nil {
			return err
		}
	}
	return nil
}
