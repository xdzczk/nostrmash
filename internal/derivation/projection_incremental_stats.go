package derivation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Ledger projection names for applied_stat_deltas. Each event may claim at
// most one row per projection, which gates the whole multi-pubkey delta set
// for that projection so retries are exactly-once. The same row is also the
// reversal gate: retention purges delete it before decrementing, so an
// event can only ever be un-applied once, and only if it was ever applied.
const (
	statDeltaProfilePublicStats   = "profile_public_stats"
	statDeltaAuthorActivityDaily  = "author_activity_daily"
	statDeltaAuthorHashtagDaily   = "author_hashtag_daily"
	statDeltaAuthorMediaDaily     = "author_media_daily"
	statDeltaAuthorHourlyActivity = "author_hourly_activity"
	statDeltaFollowerCounts       = "profile_public_stats_followers"
)

// ApplyIncrementalAuthorStats applies O(1) counter deltas for the
// projections that used to be full-history recomputes. It is a no-op when
// both incremental profile-public-stats and author-activity-daily flags are
// disabled.
//
// Must run after the cheap projections that establish the facts this path
// reads (hashtags, note_discovery_stats media flags, reaction/repost/zap
// rows, contact lists). Idempotent via applied_stat_deltas.
func (h *Handlers) ApplyIncrementalAuthorStats(ctx context.Context, eventID string) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	if !h.incrementalProfilePublicStats && !h.incrementalAuthorActivityDaily {
		return nil
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	var pubkey string
	var kind int
	var createdAt int64
	if err := h.pool.QueryRow(ctx, `
		SELECT pubkey, kind, created_at
		FROM events
		WHERE id = $1
	`, eventID).Scan(&pubkey, &kind, &createdAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("load event for incremental stats: %w", err)
	}

	tags, err := h.loadEventTags(ctx, eventID)
	if err != nil {
		return err
	}
	isReply, replyTargetEventID := replyTargetFromTags(eventID, tags)

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin incremental stats tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if h.incrementalProfilePublicStats {
		if err := h.applyIncrementalProfilePublicStatsTx(ctx, tx, eventID, pubkey, kind, createdAt, isReply); err != nil {
			return err
		}
	}
	if h.incrementalAuthorActivityDaily {
		if err := h.applyIncrementalAuthorActivityDailyTx(ctx, tx, eventID, pubkey, kind, createdAt, isReply, replyTargetEventID, tags); err != nil {
			return err
		}
		if err := h.applyIncrementalAuthorHashtagDailyTx(ctx, tx, eventID, pubkey, kind, createdAt); err != nil {
			return err
		}
		if err := h.applyIncrementalAuthorMediaDailyTx(ctx, tx, eventID, pubkey, kind, createdAt); err != nil {
			return err
		}
		if err := h.applyIncrementalAuthorHourlyActivityTx(ctx, tx, eventID, pubkey, kind, createdAt, isReply, replyTargetEventID, tags); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit incremental stats tx: %w", err)
	}
	return nil
}

// ReverseIncrementalAuthorStatsTx undoes whatever O(1) deltas
// ApplyIncrementalAuthorStats previously applied for eventID, one
// projection at a time, gated by whether that projection actually claimed
// an applied_stat_deltas row for this event. It deliberately ignores the
// *current* incremental-stats feature flags: those only gate whether new
// deltas get applied, not whether previously-applied ones need to be
// undone (an event created while a flag was on can still be purged after
// the flag is later disabled).
//
// Callers MUST run this inside the same transaction that will delete the
// events row for eventID (and its cascaded children in event_hashtags,
// note_discovery_stats, event_tags, ...), and BEFORE issuing that delete:
// several sub-projections re-derive the facts they originally used from
// those still-intact rows. See internal/store/retention for the purge
// wrappers that wire this in.
//
// Deliberate simplification: this decrements note_count / reply_count and
// all daily/hourly counters, but does not roll back
// profile_public_stats.recent_activity_at, since that would require
// knowing the pubkey's second-most-recent activity timestamp. Retention
// only purges old events, so the purged event's created_at is essentially
// never the current recent_activity_at in practice — leaving it stale (but
// never wrong in a way that matters) is preferable to the cost of tracking
// a full timestamp history just to support an edge case that doesn't occur
// in the purge-by-age access pattern.
func (h *Handlers) ReverseIncrementalAuthorStatsTx(ctx context.Context, tx pgx.Tx, eventID string) error {
	if h == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil
	}

	var pubkey string
	var kind int
	var createdAt int64
	if err := tx.QueryRow(ctx, `
		SELECT pubkey, kind, created_at
		FROM events
		WHERE id = $1
	`, eventID).Scan(&pubkey, &kind, &createdAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Already gone (e.g. a retried purge batch); nothing to reverse.
			return nil
		}
		return fmt.Errorf("load event for incremental stats reversal: %w", err)
	}

	tags, err := loadEventTagsTx(ctx, tx, eventID)
	if err != nil {
		return err
	}
	isReply, replyTargetEventID := replyTargetFromTags(eventID, tags)

	if err := h.reverseIncrementalProfilePublicStatsTx(ctx, tx, eventID, pubkey, kind, isReply); err != nil {
		return err
	}
	if err := h.reverseIncrementalAuthorActivityDailyTx(ctx, tx, eventID, pubkey, kind, createdAt, isReply, replyTargetEventID, tags); err != nil {
		return err
	}
	if err := h.reverseIncrementalAuthorHashtagDailyTx(ctx, tx, eventID, pubkey, kind, createdAt); err != nil {
		return err
	}
	if err := h.reverseIncrementalAuthorMediaDailyTx(ctx, tx, eventID, pubkey, kind, createdAt); err != nil {
		return err
	}
	return h.reverseIncrementalAuthorHourlyActivityTx(ctx, tx, eventID, pubkey, kind, createdAt, isReply, replyTargetEventID, tags)
}

// replyTargetFromTags derives whether an event is a reply and, if so, the
// event id it replies to. Shared by the apply and reverse paths so both
// classify the same event identically.
func replyTargetFromTags(eventID string, tags [][]string) (isReply bool, replyTargetEventID string) {
	for _, ref := range deriveEventReferences(eventID, tags) {
		if ref.Relation == "reply" {
			return true, strings.TrimSpace(ref.Referenced)
		}
	}
	return false, ""
}

// loadEventTagsTx is the transaction-scoped counterpart to loadEventTags,
// used by the reversal path so the read happens on the same transaction
// that will (later, in the same tx) delete the row — never on a separate
// pool connection.
func loadEventTagsTx(ctx context.Context, tx pgx.Tx, eventID string) ([][]string, error) {
	var rawEvent string
	if err := tx.QueryRow(ctx, `SELECT raw_json::text FROM events WHERE id = $1`, eventID).Scan(&rawEvent); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("load raw event for incremental stats reversal: %w", err)
	}
	var payload struct {
		Tags [][]string `json:"tags"`
	}
	if err := json.Unmarshal([]byte(rawEvent), &payload); err != nil {
		return nil, fmt.Errorf("decode event tags for incremental stats reversal: %w", err)
	}
	return payload.Tags, nil
}

func claimStatDeltaTx(ctx context.Context, tx pgx.Tx, eventID, projection string) (bool, error) {
	tag, err := tx.Exec(ctx, `
		INSERT INTO applied_stat_deltas (event_id, projection)
		VALUES ($1, $2)
		ON CONFLICT (event_id, projection) DO NOTHING
	`, eventID, projection)
	if err != nil {
		return false, fmt.Errorf("claim stat delta %s/%s: %w", eventID, projection, err)
	}
	return tag.RowsAffected() == 1, nil
}

// unclaimStatDeltaTx removes a previously-claimed applied_stat_deltas row,
// returning whether one existed. This is the reversal-side mirror of
// claimStatDeltaTx: a projection is only decremented if it was previously
// incremented, and removing the ledger row makes a repeated reversal
// attempt (e.g. a retried purge batch) a safe no-op.
func unclaimStatDeltaTx(ctx context.Context, tx pgx.Tx, eventID, projection string) (bool, error) {
	tag, err := tx.Exec(ctx, `
		DELETE FROM applied_stat_deltas
		WHERE event_id = $1 AND projection = $2
	`, eventID, projection)
	if err != nil {
		return false, fmt.Errorf("unclaim stat delta %s/%s: %w", eventID, projection, err)
	}
	return tag.RowsAffected() == 1, nil
}

func (h *Handlers) applyIncrementalProfilePublicStatsTx(
	ctx context.Context,
	tx pgx.Tx,
	eventID, pubkey string,
	kind int,
	createdAt int64,
	isReply bool,
) error {
	if kind != 1 {
		return nil
	}
	claimed, err := claimStatDeltaTx(ctx, tx, eventID, statDeltaProfilePublicStats)
	if err != nil || !claimed {
		return err
	}
	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationProfilePublicStats,
		ProfilePublicStatsVersion,
		"Project public profile counters and recent activity",
		nil,
	)
	if err != nil {
		return err
	}
	noteDelta := int64(0)
	replyDelta := int64(0)
	if isReply {
		replyDelta = 1
	} else {
		noteDelta = 1
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO profile_public_stats (
			pubkey,
			follower_count,
			following_count,
			note_count,
			reply_count,
			recent_activity_at,
			derivation_version
		)
		VALUES ($1, 0, 0, $2, $3, $4, $5)
		ON CONFLICT (pubkey) DO UPDATE
		SET note_count = GREATEST(profile_public_stats.note_count + EXCLUDED.note_count, 0),
		    reply_count = GREATEST(profile_public_stats.reply_count + EXCLUDED.reply_count, 0),
		    recent_activity_at = GREATEST(
				COALESCE(profile_public_stats.recent_activity_at, 0),
				COALESCE(EXCLUDED.recent_activity_at, 0)
			),
		    derivation_version = EXCLUDED.derivation_version,
		    updated_at = now()
	`, pubkey, noteDelta, replyDelta, createdAt, writeVersion); err != nil {
		return fmt.Errorf("apply profile public stats delta: %w", err)
	}
	return nil
}

// reverseIncrementalProfilePublicStatsTx undoes the note_count/reply_count
// delta applyIncrementalProfilePublicStatsTx previously applied. It does
// not touch recent_activity_at — see the ReverseIncrementalAuthorStatsTx
// doc comment for why.
func (h *Handlers) reverseIncrementalProfilePublicStatsTx(
	ctx context.Context,
	tx pgx.Tx,
	eventID, pubkey string,
	kind int,
	isReply bool,
) error {
	if kind != 1 {
		return nil
	}
	unclaimed, err := unclaimStatDeltaTx(ctx, tx, eventID, statDeltaProfilePublicStats)
	if err != nil || !unclaimed {
		return err
	}
	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationProfilePublicStats,
		ProfilePublicStatsVersion,
		"Project public profile counters and recent activity",
		nil,
	)
	if err != nil {
		return err
	}
	noteDelta := int64(0)
	replyDelta := int64(0)
	if isReply {
		replyDelta = -1
	} else {
		noteDelta = -1
	}
	if _, err := tx.Exec(ctx, `
		UPDATE profile_public_stats
		SET note_count = GREATEST(note_count + $2, 0),
		    reply_count = GREATEST(reply_count + $3, 0),
		    derivation_version = $4,
		    updated_at = now()
		WHERE pubkey = $1
	`, pubkey, noteDelta, replyDelta, writeVersion); err != nil {
		return fmt.Errorf("reverse profile public stats delta: %w", err)
	}
	return nil
}

func (h *Handlers) applyFollowerCountDeltasTx(
	ctx context.Context,
	tx pgx.Tx,
	eventID, authorPubkey string,
	previousFollowed, contacts []string,
	versionOverride *int,
) error {
	if !h.incrementalProfilePublicStats {
		return nil
	}
	claimed, err := claimStatDeltaTx(ctx, tx, eventID, statDeltaFollowerCounts)
	if err != nil || !claimed {
		return err
	}
	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationProfilePublicStats,
		ProfilePublicStatsVersion,
		"Project public profile counters and recent activity",
		versionOverride,
	)
	if err != nil {
		return err
	}

	prev := make(map[string]struct{}, len(previousFollowed))
	for _, p := range previousFollowed {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		prev[p] = struct{}{}
	}
	next := make(map[string]struct{}, len(contacts))
	for _, p := range contacts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		next[p] = struct{}{}
	}

	type delta struct {
		pubkey         string
		followerDelta  int64
		followingDelta int64
	}
	deltas := map[string]*delta{}
	bump := func(pubkey string, followerDelta, followingDelta int64) {
		pubkey = strings.TrimSpace(pubkey)
		if pubkey == "" {
			return
		}
		d, ok := deltas[pubkey]
		if !ok {
			d = &delta{pubkey: pubkey}
			deltas[pubkey] = d
		}
		d.followerDelta += followerDelta
		d.followingDelta += followingDelta
	}

	for followed := range next {
		if _, ok := prev[followed]; ok {
			continue
		}
		bump(authorPubkey, 0, 1)
		bump(followed, 1, 0)
	}
	for followed := range prev {
		if _, ok := next[followed]; ok {
			continue
		}
		bump(authorPubkey, 0, -1)
		bump(followed, -1, 0)
	}

	for _, d := range deltas {
		if d.followerDelta == 0 && d.followingDelta == 0 {
			continue
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO profile_public_stats (
				pubkey,
				follower_count,
				following_count,
				note_count,
				reply_count,
				recent_activity_at,
				derivation_version
			)
			VALUES ($1, GREATEST($2, 0), GREATEST($3, 0), 0, 0, NULL, $4)
			ON CONFLICT (pubkey) DO UPDATE
			SET follower_count = GREATEST(profile_public_stats.follower_count + $2, 0),
			    following_count = GREATEST(profile_public_stats.following_count + $3, 0),
			    derivation_version = EXCLUDED.derivation_version,
			    updated_at = now()
		`, d.pubkey, d.followerDelta, d.followingDelta, writeVersion); err != nil {
			return fmt.Errorf("apply follower count delta for %s: %w", d.pubkey, err)
		}
	}
	return nil
}

type activityDailyDelta struct {
	pubkey             string
	activityDate       time.Time
	postCount          int64
	noteCount          int64
	replyCount         int64
	engagementReceived int64
	engagementGiven    int64
}

// computeActivityDailyDeltas derives the author_activity_daily row deltas
// a single event contributes. It is pure with respect to the ledger (no
// claim/unclaim, no writes) so both the forward-apply and reversal paths
// can call it and get an identical delta shape — reversal just negates the
// numeric fields before writing.
func computeActivityDailyDeltas(
	ctx context.Context,
	tx pgx.Tx,
	pubkey string,
	kind int,
	createdAt int64,
	isReply bool,
	replyTargetEventID string,
	tags [][]string,
) ([]activityDailyDelta, error) {
	activityDate := time.Unix(createdAt, 0).UTC().Truncate(24 * time.Hour)
	deltas := make([]activityDailyDelta, 0, 2)

	switch kind {
	case 1:
		d := activityDailyDelta{
			pubkey:       pubkey,
			activityDate: activityDate,
			postCount:    1,
		}
		if isReply {
			d.replyCount = 1
			d.engagementGiven = 1
		} else {
			d.noteCount = 1
		}
		deltas = append(deltas, d)
		if isReply && replyTargetEventID != "" {
			targetPubkey, ok, err := lookupEventPubkeyTx(ctx, tx, replyTargetEventID)
			if err != nil {
				return nil, err
			}
			if ok && targetPubkey != "" && targetPubkey != pubkey {
				deltas = append(deltas, activityDailyDelta{
					pubkey:             targetPubkey,
					activityDate:       activityDate,
					engagementReceived: 1,
				})
			} else if ok && targetPubkey == pubkey {
				// Self-reply: cancel the given increment to match full-rebuild
				// semantics (e.pubkey <> $1 / target.pubkey <> $1 filters).
				deltas[0].engagementGiven = 0
			}
		}
	case 6, 7:
		targetEventID := firstReferencedEventID(tags)
		if targetEventID == "" {
			return nil, nil
		}
		targetPubkey, ok, err := lookupEventPubkeyTx(ctx, tx, targetEventID)
		if err != nil {
			return nil, err
		}
		if !ok || targetPubkey == "" || targetPubkey == pubkey {
			return nil, nil
		}
		deltas = append(deltas,
			activityDailyDelta{pubkey: pubkey, activityDate: activityDate, engagementGiven: 1},
			activityDailyDelta{pubkey: targetPubkey, activityDate: activityDate, engagementReceived: 1},
		)
	case 9735:
		receiver := firstTagValue(tags, "p")
		if receiver == "" || receiver == pubkey {
			return nil, nil
		}
		deltas = append(deltas,
			activityDailyDelta{pubkey: pubkey, activityDate: activityDate, engagementGiven: 1},
			activityDailyDelta{pubkey: receiver, activityDate: activityDate, engagementReceived: 1},
		)
	default:
		return nil, nil
	}
	return deltas, nil
}

func negateActivityDailyDelta(d activityDailyDelta) activityDailyDelta {
	d.postCount = -d.postCount
	d.noteCount = -d.noteCount
	d.replyCount = -d.replyCount
	d.engagementReceived = -d.engagementReceived
	d.engagementGiven = -d.engagementGiven
	return d
}

func (h *Handlers) applyIncrementalAuthorActivityDailyTx(
	ctx context.Context,
	tx pgx.Tx,
	eventID, pubkey string,
	kind int,
	createdAt int64,
	isReply bool,
	replyTargetEventID string,
	tags [][]string,
) error {
	deltas, err := computeActivityDailyDeltas(ctx, tx, pubkey, kind, createdAt, isReply, replyTargetEventID, tags)
	if err != nil {
		return err
	}
	if len(deltas) == 0 {
		return nil
	}

	claimed, err := claimStatDeltaTx(ctx, tx, eventID, statDeltaAuthorActivityDaily)
	if err != nil || !claimed {
		return err
	}
	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationAuthorActivityDaily,
		AuthorActivityDailyVersion,
		"Project per-author daily post cadence and engagement aggregates",
		nil,
	)
	if err != nil {
		return err
	}
	for _, d := range deltas {
		if err := upsertAuthorActivityDailyDeltaTx(ctx, tx, d, writeVersion); err != nil {
			return err
		}
	}
	return nil
}

// reverseIncrementalAuthorActivityDailyTx undoes the author_activity_daily
// deltas applyIncrementalAuthorActivityDailyTx previously applied, by
// recomputing the identical delta set (events are immutable, so the
// classification can't have changed) and applying it negated.
func (h *Handlers) reverseIncrementalAuthorActivityDailyTx(
	ctx context.Context,
	tx pgx.Tx,
	eventID, pubkey string,
	kind int,
	createdAt int64,
	isReply bool,
	replyTargetEventID string,
	tags [][]string,
) error {
	unclaimed, err := unclaimStatDeltaTx(ctx, tx, eventID, statDeltaAuthorActivityDaily)
	if err != nil || !unclaimed {
		return err
	}
	deltas, err := computeActivityDailyDeltas(ctx, tx, pubkey, kind, createdAt, isReply, replyTargetEventID, tags)
	if err != nil {
		return err
	}
	if len(deltas) == 0 {
		return nil
	}
	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationAuthorActivityDaily,
		AuthorActivityDailyVersion,
		"Project per-author daily post cadence and engagement aggregates",
		nil,
	)
	if err != nil {
		return err
	}
	for _, d := range deltas {
		if err := upsertAuthorActivityDailyDeltaTx(ctx, tx, negateActivityDailyDelta(d), writeVersion); err != nil {
			return err
		}
	}
	return nil
}

func upsertAuthorActivityDailyDeltaTx(ctx context.Context, tx pgx.Tx, d activityDailyDelta, writeVersion int) error {
	if d.pubkey == "" {
		return nil
	}
	if d.postCount == 0 && d.noteCount == 0 && d.replyCount == 0 && d.engagementReceived == 0 && d.engagementGiven == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO author_activity_daily (
			pubkey,
			activity_date,
			post_count,
			note_count,
			reply_count,
			engagement_received,
			engagement_given,
			derivation_version
		)
		VALUES ($1, $2::date, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (pubkey, activity_date) DO UPDATE
		SET post_count = GREATEST(author_activity_daily.post_count + EXCLUDED.post_count, 0),
		    note_count = GREATEST(author_activity_daily.note_count + EXCLUDED.note_count, 0),
		    reply_count = GREATEST(author_activity_daily.reply_count + EXCLUDED.reply_count, 0),
		    engagement_received = GREATEST(author_activity_daily.engagement_received + EXCLUDED.engagement_received, 0),
		    engagement_given = GREATEST(author_activity_daily.engagement_given + EXCLUDED.engagement_given, 0),
		    derivation_version = EXCLUDED.derivation_version,
		    updated_at = now()
	`, d.pubkey, d.activityDate, d.postCount, d.noteCount, d.replyCount, d.engagementReceived, d.engagementGiven, writeVersion)
	if err != nil {
		return fmt.Errorf("upsert author_activity_daily delta for %s: %w", d.pubkey, err)
	}
	return nil
}

func (h *Handlers) applyIncrementalAuthorHashtagDailyTx(
	ctx context.Context,
	tx pgx.Tx,
	eventID, pubkey string,
	kind int,
	createdAt int64,
) error {
	if kind != 1 {
		return nil
	}
	hashtags, err := loadEventHashtagsTx(ctx, tx, eventID)
	if err != nil {
		return err
	}
	if len(hashtags) == 0 {
		return nil
	}
	claimed, err := claimStatDeltaTx(ctx, tx, eventID, statDeltaAuthorHashtagDaily)
	if err != nil || !claimed {
		return err
	}
	activityDate := time.Unix(createdAt, 0).UTC().Truncate(24 * time.Hour)
	for _, hashtag := range hashtags {
		if _, err := tx.Exec(ctx, `
			INSERT INTO author_hashtag_daily (
				pubkey, activity_date, hashtag, usage_count, derivation_version
			)
			VALUES ($1, $2::date, $3, 1, $4)
			ON CONFLICT (pubkey, activity_date, hashtag) DO UPDATE
			SET usage_count = GREATEST(author_hashtag_daily.usage_count + 1, 0),
			    derivation_version = EXCLUDED.derivation_version,
			    updated_at = now()
		`, pubkey, activityDate, hashtag, AuthorTopicStatsVersion); err != nil {
			return fmt.Errorf("upsert author_hashtag_daily: %w", err)
		}
	}
	return nil
}

// reverseIncrementalAuthorHashtagDailyTx undoes the author_hashtag_daily
// deltas applyIncrementalAuthorHashtagDailyTx previously applied. It reads
// event_hashtags for eventID, so it must run before that row's cascaded
// delete.
func (h *Handlers) reverseIncrementalAuthorHashtagDailyTx(
	ctx context.Context,
	tx pgx.Tx,
	eventID, pubkey string,
	kind int,
	createdAt int64,
) error {
	if kind != 1 {
		return nil
	}
	unclaimed, err := unclaimStatDeltaTx(ctx, tx, eventID, statDeltaAuthorHashtagDaily)
	if err != nil || !unclaimed {
		return err
	}
	hashtags, err := loadEventHashtagsTx(ctx, tx, eventID)
	if err != nil {
		return err
	}
	if len(hashtags) == 0 {
		return nil
	}
	activityDate := time.Unix(createdAt, 0).UTC().Truncate(24 * time.Hour)
	for _, hashtag := range hashtags {
		if _, err := tx.Exec(ctx, `
			UPDATE author_hashtag_daily
			SET usage_count = GREATEST(usage_count - 1, 0),
			    updated_at = now()
			WHERE pubkey = $1 AND activity_date = $2::date AND hashtag = $3
		`, pubkey, activityDate, hashtag); err != nil {
			return fmt.Errorf("reverse author_hashtag_daily: %w", err)
		}
	}
	return nil
}

func loadEventHashtagsTx(ctx context.Context, tx pgx.Tx, eventID string) ([]string, error) {
	rows, err := tx.Query(ctx, `
		SELECT hashtag
		FROM event_hashtags
		WHERE event_id = $1
	`, eventID)
	if err != nil {
		return nil, fmt.Errorf("load event hashtags for incremental daily: %w", err)
	}
	defer rows.Close()
	hashtags := make([]string, 0)
	for rows.Next() {
		var hashtag string
		if err := rows.Scan(&hashtag); err != nil {
			return nil, fmt.Errorf("scan hashtag for incremental daily: %w", err)
		}
		hashtags = append(hashtags, hashtag)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return hashtags, nil
}

func (h *Handlers) applyIncrementalAuthorMediaDailyTx(
	ctx context.Context,
	tx pgx.Tx,
	eventID, pubkey string,
	kind int,
	createdAt int64,
) error {
	if kind != 1 {
		return nil
	}
	hasImage, hasVideo, hasLink, hasArticle, attachmentCount, ok, err := loadNoteDiscoveryMediaFlagsTx(ctx, tx, eventID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	claimed, err := claimStatDeltaTx(ctx, tx, eventID, statDeltaAuthorMediaDaily)
	if err != nil || !claimed {
		return err
	}
	textOnly := !hasImage && !hasVideo && !hasLink && !hasArticle
	activityDate := time.Unix(createdAt, 0).UTC().Truncate(24 * time.Hour)
	_, err = tx.Exec(ctx, `
		INSERT INTO author_media_daily (
			pubkey,
			activity_date,
			total_posts,
			with_image_count,
			with_video_count,
			with_link_count,
			with_article_count,
			text_only_count,
			total_attachment_count,
			derivation_version
		)
		VALUES (
			$1, $2::date, 1,
			CASE WHEN $3 THEN 1 ELSE 0 END,
			CASE WHEN $4 THEN 1 ELSE 0 END,
			CASE WHEN $5 THEN 1 ELSE 0 END,
			CASE WHEN $6 THEN 1 ELSE 0 END,
			CASE WHEN $7 THEN 1 ELSE 0 END,
			$8,
			$9
		)
		ON CONFLICT (pubkey, activity_date) DO UPDATE
		SET total_posts = GREATEST(author_media_daily.total_posts + 1, 0),
		    with_image_count = GREATEST(author_media_daily.with_image_count + EXCLUDED.with_image_count, 0),
		    with_video_count = GREATEST(author_media_daily.with_video_count + EXCLUDED.with_video_count, 0),
		    with_link_count = GREATEST(author_media_daily.with_link_count + EXCLUDED.with_link_count, 0),
		    with_article_count = GREATEST(author_media_daily.with_article_count + EXCLUDED.with_article_count, 0),
		    text_only_count = GREATEST(author_media_daily.text_only_count + EXCLUDED.text_only_count, 0),
		    total_attachment_count = GREATEST(author_media_daily.total_attachment_count + EXCLUDED.total_attachment_count, 0),
		    derivation_version = EXCLUDED.derivation_version,
		    updated_at = now()
	`, pubkey, activityDate, hasImage, hasVideo, hasLink, hasArticle, textOnly, attachmentCount, AuthorMediaMixStatsVersion)
	if err != nil {
		return fmt.Errorf("upsert author_media_daily: %w", err)
	}
	return nil
}

// reverseIncrementalAuthorMediaDailyTx undoes the author_media_daily
// deltas applyIncrementalAuthorMediaDailyTx previously applied. It reads
// note_discovery_stats for eventID, so it must run before that row's
// cascaded delete.
func (h *Handlers) reverseIncrementalAuthorMediaDailyTx(
	ctx context.Context,
	tx pgx.Tx,
	eventID, pubkey string,
	kind int,
	createdAt int64,
) error {
	if kind != 1 {
		return nil
	}
	unclaimed, err := unclaimStatDeltaTx(ctx, tx, eventID, statDeltaAuthorMediaDaily)
	if err != nil || !unclaimed {
		return err
	}
	hasImage, hasVideo, hasLink, hasArticle, attachmentCount, ok, err := loadNoteDiscoveryMediaFlagsTx(ctx, tx, eventID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	textOnly := !hasImage && !hasVideo && !hasLink && !hasArticle
	activityDate := time.Unix(createdAt, 0).UTC().Truncate(24 * time.Hour)
	_, err = tx.Exec(ctx, `
		UPDATE author_media_daily
		SET total_posts = GREATEST(total_posts - 1, 0),
		    with_image_count = GREATEST(with_image_count - CASE WHEN $3 THEN 1 ELSE 0 END, 0),
		    with_video_count = GREATEST(with_video_count - CASE WHEN $4 THEN 1 ELSE 0 END, 0),
		    with_link_count = GREATEST(with_link_count - CASE WHEN $5 THEN 1 ELSE 0 END, 0),
		    with_article_count = GREATEST(with_article_count - CASE WHEN $6 THEN 1 ELSE 0 END, 0),
		    text_only_count = GREATEST(text_only_count - CASE WHEN $7 THEN 1 ELSE 0 END, 0),
		    total_attachment_count = GREATEST(total_attachment_count - $8, 0),
		    updated_at = now()
		WHERE pubkey = $1 AND activity_date = $2::date
	`, pubkey, activityDate, hasImage, hasVideo, hasLink, hasArticle, textOnly, attachmentCount)
	if err != nil {
		return fmt.Errorf("reverse author_media_daily: %w", err)
	}
	return nil
}

func loadNoteDiscoveryMediaFlagsTx(ctx context.Context, tx pgx.Tx, eventID string) (hasImage, hasVideo, hasLink, hasArticle bool, attachmentCount int, ok bool, err error) {
	err = tx.QueryRow(ctx, `
		SELECT has_image, has_video, has_link, has_article, attachment_count
		FROM note_discovery_stats
		WHERE event_id = $1
	`, eventID).Scan(&hasImage, &hasVideo, &hasLink, &hasArticle, &attachmentCount)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, false, false, false, 0, false, nil
		}
		return false, false, false, false, 0, false, fmt.Errorf("load note_discovery_stats for media daily: %w", err)
	}
	return hasImage, hasVideo, hasLink, hasArticle, attachmentCount, true, nil
}

type hourlyActivityDelta struct {
	pubkey             string
	postCount          int64
	noteCount          int64
	replyCount         int64
	engagementReceived int64
	replyReceived      int64
	reactionReceived   int64
	repostReceived     int64
	zapReceived        int64
}

// computeHourlyActivityDeltas is the author_hourly_activity counterpart to
// computeActivityDailyDeltas: a pure (no ledger, no writes) derivation of
// the per-pubkey deltas one event contributes, shared by apply and
// reversal.
func computeHourlyActivityDeltas(
	ctx context.Context,
	tx pgx.Tx,
	pubkey string,
	kind int,
	isReply bool,
	replyTargetEventID string,
	tags [][]string,
) ([]hourlyActivityDelta, error) {
	deltas := make([]hourlyActivityDelta, 0, 2)

	switch kind {
	case 1:
		d := hourlyActivityDelta{pubkey: pubkey, postCount: 1}
		if isReply {
			d.replyCount = 1
		} else {
			d.noteCount = 1
		}
		deltas = append(deltas, d)
		if isReply && replyTargetEventID != "" {
			targetPubkey, ok, err := lookupEventPubkeyTx(ctx, tx, replyTargetEventID)
			if err != nil {
				return nil, err
			}
			if ok && targetPubkey != "" && targetPubkey != pubkey {
				deltas = append(deltas, hourlyActivityDelta{
					pubkey:             targetPubkey,
					engagementReceived: 1,
					replyReceived:      1,
				})
			}
		}
	case 6, 7:
		targetEventID := firstReferencedEventID(tags)
		if targetEventID == "" {
			return nil, nil
		}
		targetPubkey, ok, err := lookupEventPubkeyTx(ctx, tx, targetEventID)
		if err != nil {
			return nil, err
		}
		if !ok || targetPubkey == "" || targetPubkey == pubkey {
			return nil, nil
		}
		d := hourlyActivityDelta{pubkey: targetPubkey, engagementReceived: 1}
		if kind == 7 {
			d.reactionReceived = 1
		} else {
			d.repostReceived = 1
		}
		deltas = append(deltas, d)
	case 9735:
		receiver := firstTagValue(tags, "p")
		if receiver == "" || receiver == pubkey {
			return nil, nil
		}
		deltas = append(deltas, hourlyActivityDelta{
			pubkey:             receiver,
			engagementReceived: 1,
			zapReceived:        1,
		})
	default:
		return nil, nil
	}
	return deltas, nil
}

func negateHourlyActivityDelta(d hourlyActivityDelta) hourlyActivityDelta {
	d.postCount = -d.postCount
	d.noteCount = -d.noteCount
	d.replyCount = -d.replyCount
	d.engagementReceived = -d.engagementReceived
	d.replyReceived = -d.replyReceived
	d.reactionReceived = -d.reactionReceived
	d.repostReceived = -d.repostReceived
	d.zapReceived = -d.zapReceived
	return d
}

func (h *Handlers) applyIncrementalAuthorHourlyActivityTx(
	ctx context.Context,
	tx pgx.Tx,
	eventID, pubkey string,
	kind int,
	createdAt int64,
	isReply bool,
	replyTargetEventID string,
	tags [][]string,
) error {
	deltas, err := computeHourlyActivityDeltas(ctx, tx, pubkey, kind, isReply, replyTargetEventID, tags)
	if err != nil {
		return err
	}
	if len(deltas) == 0 {
		return nil
	}

	claimed, err := claimStatDeltaTx(ctx, tx, eventID, statDeltaAuthorHourlyActivity)
	if err != nil || !claimed {
		return err
	}
	engagedAt := time.Unix(createdAt, 0).UTC()
	activityDate := engagedAt.Truncate(24 * time.Hour)
	dow := int16(engagedAt.Weekday()) // Sunday=0 matches EXTRACT(DOW ...)
	hour := int16(engagedAt.Hour())
	for _, d := range deltas {
		if err := upsertAuthorHourlyActivityDeltaTx(ctx, tx, d, activityDate, dow, hour, AuthorActivityWindowsVersion); err != nil {
			return err
		}
	}
	return nil
}

// reverseIncrementalAuthorHourlyActivityTx undoes the author_hourly_activity
// deltas applyIncrementalAuthorHourlyActivityTx previously applied.
func (h *Handlers) reverseIncrementalAuthorHourlyActivityTx(
	ctx context.Context,
	tx pgx.Tx,
	eventID, pubkey string,
	kind int,
	createdAt int64,
	isReply bool,
	replyTargetEventID string,
	tags [][]string,
) error {
	unclaimed, err := unclaimStatDeltaTx(ctx, tx, eventID, statDeltaAuthorHourlyActivity)
	if err != nil || !unclaimed {
		return err
	}
	deltas, err := computeHourlyActivityDeltas(ctx, tx, pubkey, kind, isReply, replyTargetEventID, tags)
	if err != nil {
		return err
	}
	if len(deltas) == 0 {
		return nil
	}
	engagedAt := time.Unix(createdAt, 0).UTC()
	activityDate := engagedAt.Truncate(24 * time.Hour)
	dow := int16(engagedAt.Weekday())
	hour := int16(engagedAt.Hour())
	for _, d := range deltas {
		if err := upsertAuthorHourlyActivityDeltaTx(ctx, tx, negateHourlyActivityDelta(d), activityDate, dow, hour, AuthorActivityWindowsVersion); err != nil {
			return err
		}
	}
	return nil
}

func upsertAuthorHourlyActivityDeltaTx(
	ctx context.Context,
	tx pgx.Tx,
	d hourlyActivityDelta,
	activityDate time.Time,
	dow, hour int16,
	writeVersion int,
) error {
	if d.pubkey == "" {
		return nil
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO author_hourly_activity (
			pubkey,
			activity_date,
			day_of_week,
			hour_of_day,
			post_count,
			note_count,
			reply_count,
			engagement_received,
			reply_received,
			reaction_received,
			repost_received,
			zap_received,
			derivation_version
		)
		VALUES ($1, $2::date, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (pubkey, activity_date, day_of_week, hour_of_day) DO UPDATE
		SET post_count = GREATEST(author_hourly_activity.post_count + EXCLUDED.post_count, 0),
		    note_count = GREATEST(author_hourly_activity.note_count + EXCLUDED.note_count, 0),
		    reply_count = GREATEST(author_hourly_activity.reply_count + EXCLUDED.reply_count, 0),
		    engagement_received = GREATEST(author_hourly_activity.engagement_received + EXCLUDED.engagement_received, 0),
		    reply_received = GREATEST(author_hourly_activity.reply_received + EXCLUDED.reply_received, 0),
		    reaction_received = GREATEST(author_hourly_activity.reaction_received + EXCLUDED.reaction_received, 0),
		    repost_received = GREATEST(author_hourly_activity.repost_received + EXCLUDED.repost_received, 0),
		    zap_received = GREATEST(author_hourly_activity.zap_received + EXCLUDED.zap_received, 0),
		    derivation_version = EXCLUDED.derivation_version,
		    updated_at = now()
	`, d.pubkey, activityDate, dow, hour,
		d.postCount, d.noteCount, d.replyCount,
		d.engagementReceived, d.replyReceived, d.reactionReceived, d.repostReceived, d.zapReceived,
		writeVersion); err != nil {
		return fmt.Errorf("upsert author_hourly_activity for %s: %w", d.pubkey, err)
	}
	return nil
}

func lookupEventPubkeyTx(ctx context.Context, tx pgx.Tx, eventID string) (string, bool, error) {
	var pubkey string
	err := tx.QueryRow(ctx, `SELECT pubkey FROM events WHERE id = $1`, eventID).Scan(&pubkey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("lookup event pubkey %s: %w", eventID, err)
	}
	return pubkey, true, nil
}

func firstReferencedEventID(tags [][]string) string {
	for _, tag := range tags {
		if len(tag) < 2 || tag[0] != "e" {
			continue
		}
		value := strings.TrimSpace(tag[1])
		if value != "" {
			return value
		}
	}
	return ""
}
