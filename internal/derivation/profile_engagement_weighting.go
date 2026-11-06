package derivation

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// profileWeightedScoreInputs is what profile trending and rising actually
// score. Raw recent_* counters stay on the analytics path.
//
// Trending engagement is one trust-weighted vote per (signal, engager) on
// root notes published in the window, after the note farm discount: a
// greeting pile is sqrt(people) times its strongest weight, any other copied
// phrase collapses to one vote, and reactions and reposts past the reply and
// zap allowance count at a floor. One person liking ten of those notes
// counts once per signal, and a like on an older note does not count.
// Rising engagement is one person whose event falls in the window, at any
// note age, classified by their strongest signal. motionAt is the newest
// in-window root note or the latest engagement on one, whichever is later.
type profileWeightedScoreInputs struct {
	trendingEngagement24h float64
	trendingEngagement7d  float64
	trendingEngagement30d float64
	risingEngagement24h   float64
	risingEngagement7d    float64
	risingEngagement30d   float64
	trendingZapMSats24h   float64
	trendingZapMSats7d    float64
	trendingZapMSats30d   float64
	newFollowers24h       float64
	newFollowers7d        float64
	newFollowers30d       float64
	motionAt24h           *int64
	motionAt7d            *int64
	motionAt30d           *int64
}

// loadProfileWeightedScoreInputsTx scans windowed engagement toward one
// profile and aggregates the trending and rising score inputs. Trust
// weighting scales each vote; with it off, every non-author engager counts
// 1.0 before the farm discount. Either way the population rules are the
// same, so a like on an old note cannot become "engagement per post".
//
// Engagement branches start from the denormalized target_pubkey indexes
// (migration 000082) and only then look up the target note by primary key.
// The previous plan joined events first to discover the author and timed
// out. Rows with NULL target_pubkey are skipped. The authored-note scan is
// bounded to the 30d window on (pubkey, kind, created_at). Reply rows bring
// their text so a copied phrase can collapse. Replies, reactions, reposts,
// and zaps are read a page at a time and then folded to one vote per
// engager, so one busy profile cannot hold a statement open until the timeout.
func loadProfileWeightedScoreInputsTx(
	ctx context.Context,
	tx pgx.Tx,
	pubkey string,
	nowUnix int64,
	weighting EngagementWeightingOptions,
) (profileWeightedScoreInputs, error) {
	weighting = weighting.normalized()
	cutoff24h := nowUnix - int64((24*time.Hour)/time.Second)
	cutoff7d := nowUnix - int64((7*24*time.Hour)/time.Second)
	cutoff30d := nowUnix - int64((30*24*time.Hour)/time.Second)
	// $4/$5/$6 stay the shared engager-weight binds. $2 is the 30d floor,
	// $3 is the 24h cutoff, $7 is the 7d cutoff, and $8 is the sane created_at
	// ceiling. Follower gains do not use $8.
	baseArgs := []any{pubkey, cutoff30d, cutoff24h, weighting.TrustWeighted, weighting.MaxHops, weighting.UntrustedWeight, cutoff7d}
	noteArgs := append(append([]any{}, baseArgs...), maxSaneUnixCreatedAt)

	var out profileWeightedScoreInputs

	replies, replyMotion24, replyMotion7, replyMotion30, err := loadProfileReplyVotesTx(ctx, tx, noteArgs)
	if err != nil {
		return out, err
	}
	reactions, reposts, zaps, signalMotion24, signalMotion7, signalMotion30, err := loadProfileSignalVotesTx(ctx, tx, noteArgs)
	if err != nil {
		return out, err
	}
	authoredMotion24, authoredMotion7, authoredMotion30, err := loadProfileAuthoredMotionTx(ctx, tx, pubkey, cutoff30d, cutoff24h, cutoff7d)
	if err != nil {
		return out, err
	}
	scores := discountProfileVotes(replies, reactions, reposts, zaps)
	out.trendingEngagement24h = scores.trending24h
	out.trendingEngagement7d = scores.trending7d
	out.trendingEngagement30d = scores.trending30d
	out.risingEngagement24h = scores.rising24h
	out.risingEngagement7d = scores.rising7d
	out.risingEngagement30d = scores.rising30d
	out.motionAt24h = unixPtr(laterUnix(authoredMotion24, replyMotion24, signalMotion24))
	out.motionAt7d = unixPtr(laterUnix(authoredMotion7, replyMotion7, signalMotion7))
	out.motionAt30d = unixPtr(laterUnix(authoredMotion30, replyMotion30, signalMotion30))

	// Zap volume counts only sats on root notes published in the window.
	// A zap on an old note must not raise the per-post score, and a profile
	// zap (no note) is not a post either. Each sat is still scaled by the
	// sender's trust weight.
	if err := tx.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(v) FILTER (WHERE note_created >= $3), 0),
			COALESCE(SUM(v) FILTER (WHERE note_created >= $7), 0),
			COALESCE(SUM(v), 0)
		FROM (
			SELECT e.created_at AS note_created, zr.amount_sats * 1000 * `+engagerWeightCaseSQL+` AS v
			FROM zap_receipts zr
			JOIN events e ON e.id = zr.event_id
			LEFT JOIN trust_pubkeys_latest t ON t.pubkey = zr.sender_pubkey
			WHERE zr.receiver_pubkey = $1
			  AND zr.sender_pubkey <> $1
			  AND zr.created_at >= $2
			  AND zr.created_at <= $8
			  AND e.kind = 1
			  AND e.pubkey = $1
			  AND e.created_at >= $2
			  AND e.created_at <= $8
			  AND NOT EXISTS (
				SELECT 1 FROM thread_edges te WHERE te.child_event_id = e.id
			  )
		) weighted
	`, noteArgs...).Scan(&out.trendingZapMSats24h, &out.trendingZapMSats7d, &out.trendingZapMSats30d); err != nil {
		return out, fmt.Errorf("load weighted profile zap volume: %w", err)
	}

	// New followers: one weighted vote per follower gained in the window.
	// follower_gain_events holds true kind=3 edge-diff gains (self-follows
	// excluded at write), so contact-list rewrites by existing followers buy
	// nothing, and a ring of fresh untrusted accounts following a bot buys
	// zero rising momentum.
	if err := tx.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(w) FILTER (WHERE ts >= $3), 0),
			COALESCE(SUM(w) FILTER (WHERE ts >= $7), 0),
			COALESCE(SUM(w), 0)
		FROM (
			SELECT fge.gained_at AS ts, `+engagerWeightCaseSQL+` AS w
			FROM follower_gain_events fge
			LEFT JOIN trust_pubkeys_latest t ON t.pubkey = fge.follower_pubkey
			WHERE fge.followed_pubkey = $1
			  AND fge.gained_at >= $2
		) weighted
	`, baseArgs...).Scan(&out.newFollowers24h, &out.newFollowers7d, &out.newFollowers30d); err != nil {
		return out, fmt.Errorf("load weighted profile new followers: %w", err)
	}
	return out, nil
}

// profileReplyVotePageSize is how many reply rows one statement may join to
// the target note. Event text is loaded afterwards, in smaller batches:
// a page of 1000 bodies still streamed past the 15s statement timeout.
// profileReplyContentBatch is that second statement's size.
const (
	profileReplyVotePageSize  = 200
	profileReplyContentBatch  = 40
	profileSignalVotePageSize = 200
)

func loadProfileReplyVotesTx(ctx context.Context, tx pgx.Tx, noteArgs []any) ([]profileVote, int64, int64, int64, error) {
	return loadProfileReplyVotesPageTx(ctx, tx, noteArgs, profileReplyVotePageSize)
}

func loadProfileReplyVotesPageTx(ctx context.Context, tx pgx.Tx, noteArgs []any, pageSize int) ([]profileVote, int64, int64, int64, error) {
	if pageSize <= 0 {
		pageSize = profileReplyVotePageSize
	}
	// Keyset starts before any real created_at. The contribution index is
	// (target_pubkey, source_created_at); source_event_id and target_event_id
	// break ties so two replies in the same second are both returned.
	cursorAt := int64(-1)
	cursorSrc := ""
	cursorTgt := ""

	var (
		votes                       []profileVote
		sourceIDs                   []string
		motion24, motion7, motion30 int64
	)
	for {
		rows, err := tx.Query(ctx, `
			WITH page AS MATERIALIZED (
				SELECT c.source_event_id, c.target_event_id, c.source_pubkey, c.source_created_at
				FROM reply_count_contributions c
				WHERE c.target_pubkey = $1
				  AND c.source_pubkey <> $1
				  AND c.source_created_at >= $2
				  AND c.source_created_at <= $8
				  AND (c.source_created_at, c.source_event_id, c.target_event_id) > ($9, $10, $11)
				ORDER BY c.source_created_at ASC, c.source_event_id ASC, c.target_event_id ASC
				LIMIT $12
			)
			SELECT
				p.source_pubkey,
				`+engagerWeightCaseSQL+`,
				p.source_created_at,
				COALESCE(n.created_at >= $3, false),
				COALESCE(n.created_at >= $7, false),
				(n.id IS NOT NULL),
				COALESCE(p.source_created_at >= $3, false),
				COALESCE(p.source_created_at >= $7, false),
				p.source_event_id,
				p.target_event_id
			FROM page p
			LEFT JOIN trust_pubkeys_latest t ON t.pubkey = p.source_pubkey
			LEFT JOIN events n ON n.id = p.target_event_id
				AND n.kind = 1
				AND n.pubkey = $1
				AND n.created_at >= $2
				AND n.created_at <= $8
				AND NOT EXISTS (
					SELECT 1 FROM thread_edges te WHERE te.child_event_id = n.id
				)
			ORDER BY p.source_created_at ASC, p.source_event_id ASC, p.target_event_id ASC
		`, append(append([]any{}, noteArgs...), cursorAt, cursorSrc, cursorTgt, pageSize)...)
		if err != nil {
			return nil, 0, 0, 0, fmt.Errorf("load profile reply votes: %w", err)
		}

		n := 0
		var pageAt int64
		var pageSrc, pageTgt string
		for rows.Next() {
			var vote profileVote
			if err := rows.Scan(
				&vote.Pubkey,
				&vote.Weight,
				&vote.EventAt,
				&vote.Trending24h,
				&vote.Trending7d,
				&vote.Trending30d,
				&vote.Rising24h,
				&vote.Rising7d,
				&pageSrc,
				&pageTgt,
			); err != nil {
				rows.Close()
				return nil, 0, 0, 0, fmt.Errorf("scan profile reply vote: %w", err)
			}
			pageAt = vote.EventAt
			n++
			votes = append(votes, vote)
			sourceIDs = append(sourceIDs, pageSrc)
			if vote.Trending24h && vote.EventAt > motion24 {
				motion24 = vote.EventAt
			}
			if vote.Trending7d && vote.EventAt > motion7 {
				motion7 = vote.EventAt
			}
			if vote.Trending30d && vote.EventAt > motion30 {
				motion30 = vote.EventAt
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, 0, 0, 0, fmt.Errorf("read profile reply votes: %w", err)
		}
		if n == 0 {
			break
		}
		if pageAt == cursorAt && pageSrc == cursorSrc && pageTgt == cursorTgt {
			return nil, 0, 0, 0, fmt.Errorf("read profile reply votes: page cursor did not advance")
		}
		cursorAt, cursorSrc, cursorTgt = pageAt, pageSrc, pageTgt
		if n < pageSize {
			break
		}
	}
	if err := attachReplyContent(ctx, tx, votes, sourceIDs); err != nil {
		return nil, 0, 0, 0, err
	}
	return votes, motion24, motion7, motion30, nil
}

// attachReplyContent fills reply text after the page query. Pulling content
// in the same statement as the note join made a page of large bodies exceed
// the statement timeout while the cursor was still walking.
func attachReplyContent(ctx context.Context, tx pgx.Tx, votes []profileVote, sourceIDs []string) error {
	if len(votes) == 0 {
		return nil
	}
	if len(votes) != len(sourceIDs) {
		return fmt.Errorf("read profile reply votes: content ids do not match votes")
	}
	content := make(map[string]string, len(sourceIDs))
	for start := 0; start < len(sourceIDs); start += profileReplyContentBatch {
		end := start + profileReplyContentBatch
		if end > len(sourceIDs) {
			end = len(sourceIDs)
		}
		rows, err := tx.Query(ctx, `
			SELECT id, COALESCE(content, '')
			FROM events
			WHERE id = ANY($1::text[])
		`, sourceIDs[start:end])
		if err != nil {
			return fmt.Errorf("load profile reply content: %w", err)
		}
		for rows.Next() {
			var id, body string
			if err := rows.Scan(&id, &body); err != nil {
				rows.Close()
				return fmt.Errorf("scan profile reply content: %w", err)
			}
			content[id] = body
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return fmt.Errorf("read profile reply content: %w", err)
		}
	}
	for i := range votes {
		votes[i].Content = content[sourceIDs[i]]
	}
	return nil
}

func loadProfileSignalVotesTx(ctx context.Context, tx pgx.Tx, noteArgs []any) ([]profileVote, []profileVote, []profileVote, int64, int64, int64, error) {
	return loadProfileSignalVotesPageTx(ctx, tx, noteArgs, profileSignalVotePageSize)
}

// loadProfileSignalVotesPageTx aggregates reactions, reposts, and zaps one
// page at a time. One statement used to join every 30-day row to its target
// note, and that scan exceeded the statement timeout for busy profiles.
// Weight depends only on the engager, so folding each page into one vote per
// pubkey matches the old GROUP BY.
func loadProfileSignalVotesPageTx(ctx context.Context, tx pgx.Tx, noteArgs []any, pageSize int) ([]profileVote, []profileVote, []profileVote, int64, int64, int64, error) {
	if pageSize <= 0 {
		pageSize = profileSignalVotePageSize
	}
	reactions, react24, react7, react30, err := pageProfileSignal(ctx, tx, noteArgs, pageSize, profileReactionSignalPageSQL)
	if err != nil {
		return nil, nil, nil, 0, 0, 0, err
	}
	reposts, repost24, repost7, repost30, err := pageProfileSignal(ctx, tx, noteArgs, pageSize, profileRepostSignalPageSQL)
	if err != nil {
		return nil, nil, nil, 0, 0, 0, err
	}
	zaps, zap24, zap7, zap30, err := pageProfileSignal(ctx, tx, noteArgs, pageSize, profileZapSignalPageSQL)
	if err != nil {
		return nil, nil, nil, 0, 0, 0, err
	}
	return reactions, reposts, zaps, laterUnix(react24, repost24, zap24), laterUnix(react7, repost7, zap7), laterUnix(react30, repost30, zap30), nil
}

const profileSignalPageSelectSQL = `
			SELECT
				p.pubkey,
				` + engagerWeightCaseSQL + `,
				p.created_at,
				COALESCE(n.created_at >= $3, false),
				COALESCE(n.created_at >= $7, false),
				(n.id IS NOT NULL),
				COALESCE(p.created_at >= $3, false),
				COALESCE(p.created_at >= $7, false),
				p.event_id
			FROM page p
			LEFT JOIN trust_pubkeys_latest t ON t.pubkey = p.pubkey
			LEFT JOIN events n ON n.id = p.target_event_id
				AND n.kind = 1
				AND n.pubkey = $1
				AND n.created_at >= $2
				AND n.created_at <= $8
				AND NOT EXISTS (
					SELECT 1 FROM thread_edges te WHERE te.child_event_id = n.id
				)
			ORDER BY p.created_at ASC, p.event_id ASC`

const profileReactionSignalPageSQL = `
			WITH page AS MATERIALIZED (
				SELECT event_id, reactor_pubkey AS pubkey, target_event_id, created_at
				FROM reaction_events
				WHERE target_pubkey = $1
				  AND reactor_pubkey <> $1
				  AND created_at >= $2
				  AND created_at <= $8
				  AND (created_at, event_id) > ($9, $10)
				ORDER BY created_at ASC, event_id ASC
				LIMIT $11
			)` + profileSignalPageSelectSQL

const profileRepostSignalPageSQL = `
			WITH page AS MATERIALIZED (
				SELECT event_id, reposter_pubkey AS pubkey, target_event_id, created_at
				FROM repost_events
				WHERE target_pubkey = $1
				  AND reposter_pubkey <> $1
				  AND created_at >= $2
				  AND created_at <= $8
				  AND (created_at, event_id) > ($9, $10)
				ORDER BY created_at ASC, event_id ASC
				LIMIT $11
			)` + profileSignalPageSelectSQL

const profileZapSignalPageSQL = `
			WITH page AS MATERIALIZED (
				SELECT zap_receipt_id AS event_id, sender_pubkey AS pubkey, event_id AS target_event_id, created_at
				FROM zap_receipts
				WHERE receiver_pubkey = $1
				  AND sender_pubkey <> $1
				  AND created_at >= $2
				  AND created_at <= $8
				  AND (created_at, zap_receipt_id) > ($9, $10)
				ORDER BY created_at ASC, zap_receipt_id ASC
				LIMIT $11
			)` + profileSignalPageSelectSQL

type profileSignalState struct {
	weight     float64
	rising24   bool
	rising7    bool
	trending24 bool
	trending7  bool
	trending30 bool
}

func pageProfileSignal(ctx context.Context, tx pgx.Tx, noteArgs []any, pageSize int, query string) ([]profileVote, int64, int64, int64, error) {
	cursorAt := int64(-1)
	cursorID := ""
	byPubkey := map[string]*profileSignalState{}
	var order []string
	var motion24, motion7, motion30 int64
	for {
		rows, err := tx.Query(ctx, query, append(append([]any{}, noteArgs...), cursorAt, cursorID, pageSize)...)
		if err != nil {
			return nil, 0, 0, 0, fmt.Errorf("load profile signal votes: %w", err)
		}
		n := 0
		var pageAt int64
		var pageID string
		for rows.Next() {
			var pubkey, eventID string
			var weight float64
			var created int64
			var trending24, trending7, trending30, rising24, rising7 bool
			if err := rows.Scan(
				&pubkey,
				&weight,
				&created,
				&trending24,
				&trending7,
				&trending30,
				&rising24,
				&rising7,
				&eventID,
			); err != nil {
				rows.Close()
				return nil, 0, 0, 0, fmt.Errorf("scan profile signal vote: %w", err)
			}
			pageAt = created
			pageID = eventID
			n++
			state := byPubkey[pubkey]
			if state == nil {
				state = &profileSignalState{}
				byPubkey[pubkey] = state
				order = append(order, pubkey)
			}
			state.weight = weight
			state.rising24 = state.rising24 || rising24
			state.rising7 = state.rising7 || rising7
			state.trending24 = state.trending24 || trending24
			state.trending7 = state.trending7 || trending7
			state.trending30 = state.trending30 || trending30
			if trending24 && created > motion24 {
				motion24 = created
			}
			if trending7 && created > motion7 {
				motion7 = created
			}
			if trending30 && created > motion30 {
				motion30 = created
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, 0, 0, 0, fmt.Errorf("read profile signal votes: %w", err)
		}
		if n == 0 {
			break
		}
		if pageAt == cursorAt && pageID == cursorID {
			return nil, 0, 0, 0, fmt.Errorf("read profile signal votes: page cursor did not advance")
		}
		cursorAt, cursorID = pageAt, pageID
		if n < pageSize {
			break
		}
	}
	votes := make([]profileVote, 0, len(order))
	for _, pubkey := range order {
		state := byPubkey[pubkey]
		votes = append(votes, profileVote{
			Pubkey:      pubkey,
			Weight:      state.weight,
			Rising24h:   state.rising24,
			Rising7d:    state.rising7,
			Trending24h: state.trending24,
			Trending7d:  state.trending7,
			Trending30d: state.trending30,
		})
	}
	return votes, motion24, motion7, motion30, nil
}

func loadProfileAuthoredMotionTx(
	ctx context.Context,
	tx pgx.Tx,
	pubkey string,
	cutoff30d int64,
	cutoff24h int64,
	cutoff7d int64,
) (int64, int64, int64, error) {
	var motion24, motion7, motion30 int64
	if err := tx.QueryRow(ctx, `
		SELECT
			COALESCE(MAX(e.created_at) FILTER (WHERE e.created_at >= $3), 0),
			COALESCE(MAX(e.created_at) FILTER (WHERE e.created_at >= $4), 0),
			COALESCE(MAX(e.created_at), 0)
		FROM events e
		WHERE e.pubkey = $1
		  AND e.kind = 1
		  AND e.created_at >= $2
		  AND e.created_at <= $5
		  AND NOT EXISTS (
			SELECT 1 FROM thread_edges te WHERE te.child_event_id = e.id
		  )
	`, pubkey, cutoff30d, cutoff24h, cutoff7d, maxSaneUnixCreatedAt).Scan(&motion24, &motion7, &motion30); err != nil {
		return 0, 0, 0, fmt.Errorf("load profile authored motion: %w", err)
	}
	return motion24, motion7, motion30, nil
}

func laterUnix(values ...int64) int64 {
	var best int64
	for _, value := range values {
		if value > best {
			best = value
		}
	}
	return best
}

func unixPtr(value int64) *int64 {
	if value <= 0 {
		return nil
	}
	out := value
	return &out
}
