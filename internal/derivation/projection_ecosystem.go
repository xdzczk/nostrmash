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
	"github.com/xdzczk/nostrmash/internal/nip05"
	"github.com/xdzczk/nostrmash/internal/nostr"
)

func (h *Handlers) ProjectEventAttributes(ctx context.Context, eventID string) error {
	return h.projectEventAttributesWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectEventAttributesWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}
	var author string
	var kind int
	var createdAt int64
	var content string
	if err := h.pool.QueryRow(ctx, `SELECT pubkey, kind, created_at, content FROM events WHERE id = $1`, eventID).Scan(&author, &kind, &createdAt, &content); err != nil {
		return fmt.Errorf("load event for attributes: %w", err)
	}
	tags, err := h.loadEventTags(ctx, eventID)
	if err != nil {
		return err
	}
	attrs := nostr.ParseEventAttributes(eventID, tags)
	day := time.Unix(createdAt, 0).UTC().Format("2006-01-02")

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin event attributes tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	writeVersion, err := resolveDerivationWriteVersion(ctx, tx, DerivationEventAttributes, EventAttributesVersion,
		"Project client, expiration, content-warning, proxy, proof-of-work, and target kind", versionOverride)
	if err != nil {
		return err
	}
	var targetKind any
	if attrs.TargetKind != nil {
		targetKind = *attrs.TargetKind
	}
	words, mentions, hashtags, length, reading := contentShape(kind, content, tags)
	if _, err := tx.Exec(ctx, `
		INSERT INTO event_attributes (
			event_id, client, client_pubkey, expires_at, content_warning, content_warning_reason,
			proxy_protocol, proxy_id, pow_bits, pow_target, target_kind, derivation_version,
			subject, has_alt, published_at, zap_split_count, word_count, reading_time_s,
			mention_count, hashtag_count, content_length
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
		ON CONFLICT (event_id) DO UPDATE
		SET client = EXCLUDED.client,
		    client_pubkey = EXCLUDED.client_pubkey,
		    expires_at = EXCLUDED.expires_at,
		    content_warning = EXCLUDED.content_warning,
		    content_warning_reason = EXCLUDED.content_warning_reason,
		    proxy_protocol = EXCLUDED.proxy_protocol,
		    proxy_id = EXCLUDED.proxy_id,
		    pow_bits = EXCLUDED.pow_bits,
		    pow_target = EXCLUDED.pow_target,
		    target_kind = EXCLUDED.target_kind,
		    derivation_version = EXCLUDED.derivation_version,
		    subject = EXCLUDED.subject,
		    has_alt = EXCLUDED.has_alt,
		    published_at = EXCLUDED.published_at,
		    zap_split_count = EXCLUDED.zap_split_count,
		    word_count = EXCLUDED.word_count,
		    reading_time_s = EXCLUDED.reading_time_s,
		    mention_count = EXCLUDED.mention_count,
		    hashtag_count = EXCLUDED.hashtag_count,
		    content_length = EXCLUDED.content_length,
		    projected_at = now()
	`, eventID, attrs.Client, attrs.ClientPubkey, attrs.ExpiresAt, attrs.ContentWarning, attrs.ContentWarningReason,
		attrs.ProxyProtocol, attrs.ProxyID, attrs.PowBits, attrs.PowTarget, targetKind, writeVersion,
		attrs.Subject, attrs.HasAlt, attrs.PublishedAt, attrs.ZapSplitCount, words, reading, mentions, hashtags, length); err != nil {
		return fmt.Errorf("upsert event attributes: %w", err)
	}
	if err := projectTagEnrichmentTx(ctx, tx, eventID, author, kind, createdAt, tags); err != nil {
		return fmt.Errorf("project tag enrichment: %w", err)
	}
	claimed, err := claimStatDeltaTx(ctx, tx, eventID, "event_attribute_daily")
	if err != nil {
		return err
	}
	if claimed {
		client := attrs.Client
		if client == "" {
			client = "(none)"
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO client_daily (day, client, kind, event_count)
			VALUES ($1::date, $2, $3, 1)
			ON CONFLICT (day, client, kind) DO UPDATE
			SET event_count = client_daily.event_count + 1
		`, day, client, kind); err != nil {
			return fmt.Errorf("upsert client daily: %w", err)
		}
		if attrs.Client != "" {
			if _, err := tx.Exec(ctx, `
				INSERT INTO client_author_daily (day, client, pubkey)
				VALUES ($1::date, $2, $3)
				ON CONFLICT DO NOTHING
			`, day, attrs.Client, author); err != nil {
				return fmt.Errorf("upsert client author daily: %w", err)
			}
		}
		protocol := attrs.ProxyProtocol
		bridged := 0
		if protocol != "" {
			bridged = 1
		}
		warning := 0
		if attrs.ContentWarning {
			warning = 1
		}
		pow := 0
		if attrs.PowTarget > 0 && attrs.PowBits >= attrs.PowTarget {
			pow = 1
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO content_flags_daily (day, total_count, bridged_count, content_warning_count, pow_count, bridged_by_protocol)
			VALUES ($1::date, 1, $2, $3, $4, CASE WHEN $5 = '' THEN '{}'::jsonb ELSE jsonb_build_object($5, 1) END)
			ON CONFLICT (day) DO UPDATE
			SET total_count = content_flags_daily.total_count + 1,
			    bridged_count = content_flags_daily.bridged_count + EXCLUDED.bridged_count,
			    content_warning_count = content_flags_daily.content_warning_count + EXCLUDED.content_warning_count,
			    pow_count = content_flags_daily.pow_count + EXCLUDED.pow_count,
			    bridged_by_protocol = CASE
			        WHEN $5 = '' THEN content_flags_daily.bridged_by_protocol
			        ELSE jsonb_set(
			            content_flags_daily.bridged_by_protocol,
			            ARRAY[$5],
			            to_jsonb(COALESCE((content_flags_daily.bridged_by_protocol->>$5)::bigint, 0) + 1)
			        )
			    END
		`, day, bridged, warning, pow, protocol); err != nil {
			return fmt.Errorf("upsert content flags daily: %w", err)
		}
	}
	if err := bumpStoredKindActivityTx(ctx, tx, eventID, author, kind, createdAt); err != nil {
		return err
	}
	if err := recordContentFingerprintTx(ctx, tx, eventID, author, content, kind, createdAt); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit event attributes: %w", err)
	}
	return nil
}

func bumpStoredKindActivityTx(ctx context.Context, tx pgx.Tx, eventID, author string, kind int, createdAt int64) error {
	claimed, err := claimStatDeltaTx(ctx, tx, eventID, "kind_activity")
	if err != nil || !claimed {
		return err
	}
	day := time.Unix(createdAt, 0).UTC().Format("2006-01-02")
	authorTag, err := tx.Exec(ctx, `
		INSERT INTO kind_author_daily (day, kind, pubkey)
		VALUES ($1::date, $2, $3)
		ON CONFLICT DO NOTHING
	`, day, kind, author)
	if err != nil {
		return fmt.Errorf("insert kind author: %w", err)
	}
	unique := int64(0)
	if authorTag.RowsAffected() > 0 {
		unique = 1
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO kind_activity_daily (day, kind, event_count, unique_authors)
		VALUES ($1::date, $2, 1, $3)
		ON CONFLICT (day, kind) DO UPDATE
		SET event_count = kind_activity_daily.event_count + 1,
		    unique_authors = kind_activity_daily.unique_authors + EXCLUDED.unique_authors
	`, day, kind, unique)
	if err != nil {
		return fmt.Errorf("upsert kind activity: %w", err)
	}
	return nil
}

func (h *Handlers) ProjectProfileFields(ctx context.Context, eventID string) error {
	return h.projectProfileFieldsWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectProfileFieldsWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	var pubkey string
	var kind int
	if err := h.pool.QueryRow(ctx, `SELECT pubkey, kind FROM events WHERE id = $1`, eventID).Scan(&pubkey, &kind); err != nil {
		return fmt.Errorf("load event for profile fields: %w", err)
	}
	if kind != 0 {
		return nil
	}
	var createdAt int64
	var content string
	var winnerID string
	if err := h.pool.QueryRow(ctx, `
		SELECT id, created_at, content FROM events
		WHERE pubkey = $1 AND kind = 0
		ORDER BY created_at DESC, id DESC
		LIMIT 1
	`, pubkey).Scan(&winnerID, &createdAt, &content); err != nil {
		return fmt.Errorf("load latest profile content: %w", err)
	}
	fields := parseProfileFields(content)
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin profile fields tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	writeVersion, err := resolveDerivationWriteVersion(ctx, tx, DerivationProfileFields, ProfileFieldsVersion,
		"Decompose kind-0 metadata into queryable identity fields", versionOverride)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO profile_fields (
			pubkey, name, display_name, nip05, nip05_local, nip05_domain, lud16, lud16_domain,
			has_lud06, website_domain, has_picture, has_banner, has_about, is_bot,
			completeness_score, metadata_created_at, derivation_version
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
		ON CONFLICT (pubkey) DO UPDATE
		SET name = EXCLUDED.name,
		    display_name = EXCLUDED.display_name,
		    nip05 = EXCLUDED.nip05,
		    nip05_local = EXCLUDED.nip05_local,
		    nip05_domain = EXCLUDED.nip05_domain,
		    lud16 = EXCLUDED.lud16,
		    lud16_domain = EXCLUDED.lud16_domain,
		    has_lud06 = EXCLUDED.has_lud06,
		    website_domain = EXCLUDED.website_domain,
		    has_picture = EXCLUDED.has_picture,
		    has_banner = EXCLUDED.has_banner,
		    has_about = EXCLUDED.has_about,
		    is_bot = EXCLUDED.is_bot,
		    completeness_score = EXCLUDED.completeness_score,
		    metadata_created_at = EXCLUDED.metadata_created_at,
		    derivation_version = EXCLUDED.derivation_version,
		    updated_at = now()
	`, pubkey, fields.Name, fields.DisplayName, fields.NIP05, fields.NIP05Local, fields.NIP05Domain,
		fields.Lud16, fields.Lud16Domain, fields.HasLud06, fields.WebsiteDomain, fields.HasPicture,
		fields.HasBanner, fields.HasAbout, fields.IsBot, fields.Completeness, createdAt, writeVersion); err != nil {
		return fmt.Errorf("upsert profile fields: %w", err)
	}
	if fields.NIP05 == "" {
		if _, err := tx.Exec(ctx, `DELETE FROM nip05_verifications WHERE pubkey = $1`, pubkey); err != nil {
			return fmt.Errorf("clear nip05 verification: %w", err)
		}
	} else if _, err := tx.Exec(ctx, `
		INSERT INTO nip05_verifications (pubkey, nip05, domain, status, next_check_at, failure_count)
		VALUES ($1, $2, $3, 'pending', now(), 0)
		ON CONFLICT (pubkey) DO UPDATE
		SET nip05 = EXCLUDED.nip05,
		    domain = EXCLUDED.domain,
		    status = 'pending',
		    next_check_at = now(),
		    failure_count = 0,
		    last_error_code = '',
		    updated_at = now()
		WHERE nip05_verifications.nip05 IS DISTINCT FROM EXCLUDED.nip05
	`, pubkey, fields.NIP05, fields.NIP05Domain); err != nil {
		return fmt.Errorf("schedule nip05 verification: %w", err)
	}
	identityTags, err := h.loadEventTags(ctx, winnerID)
	if err != nil {
		return err
	}
	if err := replaceExternalIdentitiesTx(ctx, tx, pubkey, identityTags); err != nil {
		return fmt.Errorf("replace external identities: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit profile fields: %w", err)
	}
	return nil
}

type parsedProfileFields struct {
	Name, DisplayName, NIP05, NIP05Local, NIP05Domain string
	Lud16, Lud16Domain, WebsiteDomain                 string
	HasLud06, HasPicture, HasBanner, HasAbout, IsBot  bool
	Completeness                                      int
}

func parseProfileFields(content string) parsedProfileFields {
	var raw map[string]any
	_ = json.Unmarshal([]byte(content), &raw)
	str := func(key string) string {
		value, _ := raw[key].(string)
		return strings.TrimSpace(value)
	}
	out := parsedProfileFields{
		Name:        str("name"),
		DisplayName: str("display_name"),
		HasAbout:    str("about") != "",
		HasPicture:  str("picture") != "",
		HasBanner:   str("banner") != "",
		HasLud06:    str("lud06") != "",
	}
	if bot, ok := raw["bot"].(bool); ok {
		out.IsBot = bot
	}
	if local, domain, ok := nip05.SplitIdentifier(str("nip05")); ok {
		out.NIP05Local = local
		out.NIP05Domain = domain
		out.NIP05 = local + "@" + domain
	}
	lud16 := strings.ToLower(str("lud16"))
	if local, domain, ok := strings.Cut(lud16, "@"); ok && local != "" && domain != "" && !strings.Contains(domain, "@") {
		out.Lud16 = lud16
		out.Lud16Domain = domain
	}
	if website := str("website"); website != "" {
		if !strings.Contains(website, "://") {
			website = "https://" + website
		}
		if parsed, err := url.Parse(website); err == nil {
			out.WebsiteDomain = strings.ToLower(parsed.Hostname())
		}
	}
	score := 0
	for _, present := range []bool{
		out.Name != "", out.DisplayName != "", out.HasAbout, out.HasPicture, out.HasBanner,
		out.NIP05 != "", out.Lud16 != "" || out.HasLud06, out.WebsiteDomain != "",
	} {
		if present {
			score++
		}
	}
	out.Completeness = score
	return out
}

func (h *Handlers) ProjectDMVolume(ctx context.Context, eventID string) error {
	return h.projectDMVolumeWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectDMVolumeWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	var kind int
	var createdAt int64
	if err := h.pool.QueryRow(ctx, `SELECT kind, created_at FROM events WHERE id = $1`, eventID).Scan(&kind, &createdAt); err != nil {
		return fmt.Errorf("load event for dm volume: %w", err)
	}
	protocol := ""
	switch kind {
	case 4:
		protocol = "nip04"
	case 1059:
		protocol = "nip17"
	default:
		return nil
	}
	tags, err := h.loadEventTags(ctx, eventID)
	if err != nil {
		return err
	}
	receivers := normalizeUniqueIDs(tagValues(tags, "p"))
	if len(receivers) == 0 {
		return nil
	}
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin dm volume tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := resolveDerivationWriteVersion(ctx, tx, DerivationDMVolume, DMVolumeVersion,
		"Count NIP-04 and NIP-17 messages per receiver per day", versionOverride); err != nil {
		return err
	}
	claimed, err := claimStatDeltaTx(ctx, tx, eventID, "dm_volume")
	if err != nil {
		return err
	}
	if claimed {
		day := time.Unix(createdAt, 0).UTC().Format("2006-01-02")
		for _, receiver := range receivers {
			if _, err := tx.Exec(ctx, `
				INSERT INTO dm_volume_daily (day, receiver_pubkey, protocol, message_count)
				VALUES ($1::date, $2, $3, 1)
				ON CONFLICT (day, receiver_pubkey, protocol) DO UPDATE
				SET message_count = dm_volume_daily.message_count + 1
			`, day, receiver, protocol); err != nil {
				return fmt.Errorf("upsert dm volume: %w", err)
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit dm volume: %w", err)
	}
	return nil
}

func (h *Handlers) ProjectNutzapReceipts(ctx context.Context, eventID string) error {
	return h.projectNutzapReceiptsWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectNutzapReceiptsWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	var kind int
	var sender, content string
	var createdAt int64
	if err := h.pool.QueryRow(ctx, `SELECT kind, pubkey, created_at, content FROM events WHERE id = $1`, eventID).Scan(&kind, &sender, &createdAt, &content); err != nil {
		return fmt.Errorf("load event for nutzap: %w", err)
	}
	if kind != 9321 {
		return nil
	}
	tags, err := h.loadEventTags(ctx, eventID)
	if err != nil {
		return err
	}
	amount, unit, mint := nutzapAmount(tags)
	receiver := firstTagValue(tags, "p")
	target := firstTagValue(tags, "e")
	address := ""
	if addr, ok := nostr.ParseAddressCoordinate(firstTagValue(tags, "a")); ok {
		address = addr.String()
	}
	_ = content
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin nutzap tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	writeVersion, err := resolveDerivationWriteVersion(ctx, tx, DerivationNutzapReceipts, NutzapReceiptsVersion,
		"Project NIP-61 nutzap receipts by sender, receiver, mint, and sats", versionOverride)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO nutzap_receipts (
			event_id, created_at, sender_pubkey, receiver_pubkey, target_event_id, target_address,
			amount_sats, mint_url, unit, derivation_version
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (event_id) DO UPDATE
		SET created_at = EXCLUDED.created_at,
		    sender_pubkey = EXCLUDED.sender_pubkey,
		    receiver_pubkey = EXCLUDED.receiver_pubkey,
		    target_event_id = EXCLUDED.target_event_id,
		    target_address = EXCLUDED.target_address,
		    amount_sats = EXCLUDED.amount_sats,
		    mint_url = EXCLUDED.mint_url,
		    unit = EXCLUDED.unit,
		    derivation_version = EXCLUDED.derivation_version,
		    projected_at = now()
	`, eventID, createdAt, sender, receiver, target, address, amount, mint, unit, writeVersion); err != nil {
		return fmt.Errorf("upsert nutzap receipt: %w", err)
	}
	claimed, err := claimStatDeltaTx(ctx, tx, eventID, "nutzap_economics")
	if err != nil {
		return err
	}
	if claimed {
		day := time.Unix(createdAt, 0).UTC().Format("2006-01-02")
		if _, err := tx.Exec(ctx, `
			INSERT INTO zap_daily (day, nutzap_count, nutzap_sats)
			VALUES ($1::date, 1, $2)
			ON CONFLICT (day) DO UPDATE
			SET nutzap_count = zap_daily.nutzap_count + 1,
			    nutzap_sats = zap_daily.nutzap_sats + EXCLUDED.nutzap_sats
		`, day, amount); err != nil {
			return fmt.Errorf("upsert nutzap daily: %w", err)
		}
		if receiver != "" && receiver != sender {
			if _, err := tx.Exec(ctx, `
				INSERT INTO profile_zap_daily (pubkey, day, nutzap_received_count, nutzap_received_sats)
				VALUES ($1, $2::date, 1, $3)
				ON CONFLICT (pubkey, day) DO UPDATE
				SET nutzap_received_count = profile_zap_daily.nutzap_received_count + 1,
				    nutzap_received_sats = profile_zap_daily.nutzap_received_sats + EXCLUDED.nutzap_received_sats
			`, receiver, day, amount); err != nil {
				return fmt.Errorf("upsert profile nutzap daily: %w", err)
			}
		}
		if target != "" {
			if _, err := tx.Exec(ctx, `
				UPDATE note_discovery_stats
				SET nutzap_count = nutzap_count + 1,
				    nutzap_sats = nutzap_sats + $2
				WHERE event_id = $1
			`, target, amount); err != nil {
				return fmt.Errorf("increment note nutzap counters: %w", err)
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit nutzap: %w", err)
	}
	return nil
}

func nutzapAmount(tags [][]string) (sats int64, unit, mint string) {
	unit = "sat"
	var tagged int64
	var proofs int64
	for _, tag := range tags {
		if len(tag) < 2 {
			continue
		}
		switch tag[0] {
		case "amount":
			if n, err := strconv.ParseInt(strings.TrimSpace(tag[1]), 10, 64); err == nil && n > tagged {
				tagged = n
			}
		case "u", "mint":
			if mint == "" {
				mint = strings.TrimSpace(tag[1])
			}
			if tag[0] == "mint" && len(tag) >= 3 && strings.TrimSpace(tag[2]) != "" {
				unit = strings.TrimSpace(tag[2])
			}
		case "proof":
			var proof struct {
				Amount int64 `json:"amount"`
			}
			if err := json.Unmarshal([]byte(tag[1]), &proof); err == nil && proof.Amount > 0 {
				proofs += proof.Amount
			}
		}
	}
	sats = proofs
	if sats == 0 {
		sats = tagged
	}
	if strings.EqualFold(unit, "msat") && sats > 0 {
		sats /= 1000
		unit = "sat"
	}
	return sats, unit, mint
}

func (h *Handlers) ProjectExtendedLists(ctx context.Context, eventID string) error {
	return h.projectExtendedListsWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectExtendedListsWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	var pubkey string
	var kind int
	if err := h.pool.QueryRow(ctx, `SELECT pubkey, kind FROM events WHERE id = $1`, eventID).Scan(&pubkey, &kind); err != nil {
		return fmt.Errorf("load event for extended lists: %w", err)
	}
	if kind != 10050 && kind != 10019 {
		return nil
	}
	var winnerID string
	var createdAt int64
	if err := h.pool.QueryRow(ctx, `
		SELECT id, created_at FROM events
		WHERE pubkey = $1 AND kind = $2
		ORDER BY created_at DESC, id DESC
		LIMIT 1
	`, pubkey, kind).Scan(&winnerID, &createdAt); err != nil {
		return fmt.Errorf("load latest extended list: %w", err)
	}
	tags, err := h.loadEventTags(ctx, winnerID)
	if err != nil {
		return err
	}
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin extended list tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	writeVersion, err := resolveDerivationWriteVersion(ctx, tx, DerivationExtendedLists, ExtendedListsVersion,
		"Project kind-10050 DM relay lists and kind-10019 nutzap wallet info", versionOverride)
	if err != nil {
		return err
	}
	switch kind {
	case 10050:
		relays, _ := json.Marshal(tagValues(tags, "relay"))
		var previousRelays []byte
		_ = tx.QueryRow(ctx, `SELECT relays_json FROM dm_relay_lists_latest WHERE pubkey = $1`, pubkey).Scan(&previousRelays)
		if err := diffRelayRefColumnTx(ctx, tx, "dm_ref_count", previousRelays, normalizeRelayList(tagValues(tags, "relay"))); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO dm_relay_lists_latest (pubkey, event_id, created_at, relays_json, derivation_version)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (pubkey) DO UPDATE
			SET event_id = EXCLUDED.event_id,
			    created_at = EXCLUDED.created_at,
			    relays_json = EXCLUDED.relays_json,
			    derivation_version = EXCLUDED.derivation_version,
			    updated_at = now()
		`, pubkey, winnerID, createdAt, relays, writeVersion); err != nil {
			return fmt.Errorf("upsert dm relay list: %w", err)
		}
	case 10019:
		relays, _ := json.Marshal(tagValues(tags, "relay"))
		mints := make([]map[string]string, 0)
		p2pk := ""
		for _, tag := range tags {
			if len(tag) < 2 {
				continue
			}
			if tag[0] == "mint" {
				unit := ""
				if len(tag) >= 3 {
					unit = tag[2]
				}
				mints = append(mints, map[string]string{"url": tag[1], "unit": unit})
			}
			if tag[0] == "pubkey" && p2pk == "" {
				p2pk = strings.TrimSpace(tag[1])
			}
		}
		mintJSON, _ := json.Marshal(mints)
		if _, err := tx.Exec(ctx, `
			INSERT INTO nutzap_info_latest (pubkey, event_id, created_at, relays_json, mints_json, p2pk, derivation_version)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (pubkey) DO UPDATE
			SET event_id = EXCLUDED.event_id,
			    created_at = EXCLUDED.created_at,
			    relays_json = EXCLUDED.relays_json,
			    mints_json = EXCLUDED.mints_json,
			    p2pk = EXCLUDED.p2pk,
			    derivation_version = EXCLUDED.derivation_version,
			    updated_at = now()
		`, pubkey, winnerID, createdAt, relays, mintJSON, p2pk, writeVersion); err != nil {
			return fmt.Errorf("upsert nutzap info: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit extended lists: %w", err)
	}
	return nil
}

func tagValues(tags [][]string, name string) []string {
	out := make([]string, 0)
	for _, tag := range tags {
		if len(tag) >= 2 && tag[0] == name {
			if value := strings.TrimSpace(tag[1]); value != "" {
				out = append(out, value)
			}
		}
	}
	return out
}

func (h *Handlers) applyZapEconomicsTx(ctx context.Context, tx pgx.Tx, eventID, sender, receiver string, createdAt, sats int64) error {
	claimed, err := claimStatDeltaTx(ctx, tx, eventID, "zap_economics")
	if err != nil || !claimed {
		return err
	}
	day := time.Unix(createdAt, 0).UTC().Format("2006-01-02")
	self := 0
	if sender != "" && sender == receiver {
		self = 1
	}
	bucket := nostr.ZapAmountBucket(sats)
	if _, err := tx.Exec(ctx, `
		INSERT INTO zap_daily (day, zap_count, sats_total, self_zap_count, hist)
		VALUES ($1::date, 1, $2, $3, jsonb_build_object($4::text, 1))
		ON CONFLICT (day) DO UPDATE
		SET zap_count = zap_daily.zap_count + 1,
		    sats_total = zap_daily.sats_total + EXCLUDED.sats_total,
		    self_zap_count = zap_daily.self_zap_count + EXCLUDED.self_zap_count,
		    hist = jsonb_set(
		        zap_daily.hist,
		        ARRAY[$4::text],
		        to_jsonb(COALESCE((zap_daily.hist->>$4)::bigint, 0) + 1)
		    )
	`, day, sats, self, bucket); err != nil {
		return fmt.Errorf("upsert zap daily: %w", err)
	}
	if sender != "" {
		if _, err := tx.Exec(ctx, `
			INSERT INTO zap_participant_daily (day, pubkey, role) VALUES ($1::date, $2, 'sender')
			ON CONFLICT DO NOTHING
		`, day, sender); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO profile_zap_daily (pubkey, day, sent_count, sent_sats)
			VALUES ($1, $2::date, 1, $3)
			ON CONFLICT (pubkey, day) DO UPDATE
			SET sent_count = profile_zap_daily.sent_count + 1,
			    sent_sats = profile_zap_daily.sent_sats + EXCLUDED.sent_sats
		`, sender, day, sats); err != nil {
			return err
		}
	}
	if receiver != "" {
		if _, err := tx.Exec(ctx, `
			INSERT INTO zap_participant_daily (day, pubkey, role) VALUES ($1::date, $2, 'receiver')
			ON CONFLICT DO NOTHING
		`, day, receiver); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO profile_zap_daily (pubkey, day, received_count, received_sats)
			VALUES ($1, $2::date, 1, $3)
			ON CONFLICT (pubkey, day) DO UPDATE
			SET received_count = profile_zap_daily.received_count + 1,
			    received_sats = profile_zap_daily.received_sats + EXCLUDED.received_sats
		`, receiver, day, sats); err != nil {
			return err
		}
	}
	if sender != "" && receiver != "" && sender != receiver {
		if _, err := tx.Exec(ctx, `
			INSERT INTO zap_edges (sender_pubkey, receiver_pubkey, zap_count, sats_total, first_at, last_at)
			VALUES ($1, $2, 1, $3, $4, $4)
			ON CONFLICT (sender_pubkey, receiver_pubkey) DO UPDATE
			SET zap_count = zap_edges.zap_count + 1,
			    sats_total = zap_edges.sats_total + EXCLUDED.sats_total,
			    first_at = LEAST(zap_edges.first_at, EXCLUDED.first_at),
			    last_at = GREATEST(zap_edges.last_at, EXCLUDED.last_at)
		`, sender, receiver, sats, createdAt); err != nil {
			return fmt.Errorf("upsert zap edge: %w", err)
		}
	}
	return nil
}

func (h *Handlers) applyReactionBreakdownTx(ctx context.Context, tx pgx.Tx, sourceID, targetID, reactor, content string, createdAt int64) error {
	key := nostr.NormalizeReactionContent(content)
	if _, err := tx.Exec(ctx, `DELETE FROM reaction_content_edges WHERE source_event_id = $1`, sourceID); err != nil {
		return fmt.Errorf("delete reaction content edge: %w", err)
	}
	if targetID == "" {
		return nil
	}
	var distinct int
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM reaction_content_counts WHERE target_event_id = $1 AND content_key <> $2
	`, targetID, key).Scan(&distinct); err != nil {
		return err
	}
	if distinct >= 50 {
		var exists bool
		_ = tx.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM reaction_content_counts WHERE target_event_id = $1 AND content_key = $2)
		`, targetID, key).Scan(&exists)
		if !exists && key != "+" && key != "-" {
			key = "other"
		}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO reaction_content_edges (source_event_id, target_event_id, content_key)
		VALUES ($1, $2, $3)
	`, sourceID, targetID, key); err != nil {
		return fmt.Errorf("insert reaction content edge: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO reaction_content_counts (target_event_id, content_key, count)
		VALUES ($1, $2, 1)
		ON CONFLICT (target_event_id, content_key) DO UPDATE
		SET count = reaction_content_counts.count + 1
	`, targetID, key); err != nil {
		return fmt.Errorf("increment reaction content count: %w", err)
	}
	claimed, err := claimStatDeltaTx(ctx, tx, sourceID, "reaction_daily")
	if err != nil || !claimed || reactor == "" {
		return err
	}
	day := time.Unix(createdAt, 0).UTC().Format("2006-01-02")
	plus, minus, emoji := 0, 0, 0
	switch key {
	case "+":
		plus = 1
	case "-":
		minus = 1
	default:
		emoji = 1
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO profile_reaction_daily (pubkey, day, plus_count, minus_count, emoji_count)
		VALUES ($1, $2::date, $3, $4, $5)
		ON CONFLICT (pubkey, day) DO UPDATE
		SET plus_count = profile_reaction_daily.plus_count + EXCLUDED.plus_count,
		    minus_count = profile_reaction_daily.minus_count + EXCLUDED.minus_count,
		    emoji_count = profile_reaction_daily.emoji_count + EXCLUDED.emoji_count
	`, reactor, day, plus, minus, emoji)
	if err != nil {
		return err
	}
	var author string
	if err := tx.QueryRow(ctx, `SELECT pubkey FROM events WHERE id = $1`, targetID).Scan(&author); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO author_reaction_received_daily (pubkey, day, plus_count, minus_count, emoji_count)
		VALUES ($1, $2::date, $3, $4, $5)
		ON CONFLICT (pubkey, day) DO UPDATE
		SET plus_count = author_reaction_received_daily.plus_count + EXCLUDED.plus_count,
		    minus_count = author_reaction_received_daily.minus_count + EXCLUDED.minus_count,
		    emoji_count = author_reaction_received_daily.emoji_count + EXCLUDED.emoji_count
	`, author, day, plus, minus, emoji); err != nil {
		return err
	}
	var trusted bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM trust_pubkeys_latest WHERE pubkey = $1)`, reactor).Scan(&trusted); err != nil {
		return err
	}
	if !trusted {
		return nil
	}
	_, err = tx.Exec(ctx, `
		UPDATE note_discovery_stats
		SET trusted_engagement_count = trusted_engagement_count + 1
		WHERE event_id = $1
	`, targetID)
	return err
}

func (h *Handlers) recordAuthorFirstSeenTx(ctx context.Context, tx pgx.Tx, pubkey string, createdAt int64, eventID string) error {
	if pubkey == "" || createdAt <= 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO author_first_seen (pubkey, first_seen_at, first_event_id)
		VALUES ($1, to_timestamp($2::double precision), $3)
		ON CONFLICT (pubkey) DO UPDATE
		SET first_event_id = CASE
		        WHEN EXCLUDED.first_seen_at < author_first_seen.first_seen_at THEN EXCLUDED.first_event_id
		        ELSE author_first_seen.first_event_id
		    END,
		    first_seen_at = LEAST(author_first_seen.first_seen_at, EXCLUDED.first_seen_at)
	`, pubkey, createdAt, eventID)
	if err != nil {
		return fmt.Errorf("upsert author first seen: %w", err)
	}
	return nil
}
