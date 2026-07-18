package replay

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// StateSnapshot captures deterministic Layer 1/2/3 state for replay validation.
type StateSnapshot struct {
	Layer1 Layer1Snapshot `json:"layer1"`
	Layer2 Layer2Snapshot `json:"layer2"`
	Layer3 Layer3Snapshot `json:"layer3"`
}

type Layer1Snapshot struct {
	Events         []Layer1Event        `json:"events"`
	EventTags      []Layer1EventTag     `json:"event_tags"`
	EventRelays    []Layer1EventRelay   `json:"event_relays"`
	InvalidEvents  []Layer1InvalidEvent `json:"invalid_events"`
	PendingJobs    []LayerJobStatus     `json:"pending_jobs"`
	SucceededJobs  []LayerJobStatus     `json:"succeeded_jobs"`
	DeadLetterJobs []LayerJobStatus     `json:"dead_letter_jobs"`
}

type Layer2Snapshot struct {
	EventReferences  []EventReferenceRow   `json:"event_references"`
	ReplaceableState []ReplaceableStateRow `json:"replaceable_state"`
	ThreadEdges      []ThreadEdgeRow       `json:"thread_edges"`
	UnresolvedThread []UnresolvedThreadRow `json:"unresolved_thread_references"`
}

type Layer3Snapshot struct {
	ProfilesLatest   []ProfileLatestRow   `json:"profiles_latest"`
	AuthorRecent     []AuthorRecentRow    `json:"author_recent_events"`
	ReplyCounts      []CountRow           `json:"reply_counts"`
	ReactionCounts   []CountRow           `json:"reaction_counts"`
	RepostCounts     []CountRow           `json:"repost_counts"`
	FollowerEdges    []FollowerEdgeRow    `json:"follower_edges"`
	DMUnreadCounts   []DMUnreadCountRow   `json:"dm_unread_counts"`
	ZapReceipts      []ZapReceiptRow      `json:"zap_receipts"`
	CuratedReads     []CuratedReadRow     `json:"curated_recommended_reads"`
	CuratedTopics    []CuratedTopicRow    `json:"curated_reads_topics"`
	CuratedAuthors   []CuratedAuthorRow   `json:"curated_featured_authors"`
	CuratedPaidTiers []CuratedPaidTierRow `json:"curated_creator_paid_tiers"`
}

type Layer1Event struct {
	ID        string `json:"id"`
	Pubkey    string `json:"pubkey"`
	CreatedAt int64  `json:"created_at"`
	Kind      int    `json:"kind"`
}

type Layer1EventTag struct {
	EventID    string `json:"event_id"`
	TagName    string `json:"tag_name"`
	TagIndex   int    `json:"tag_index"`
	ValueIndex int    `json:"value_index"`
	Value      string `json:"value"`
}

type Layer1EventRelay struct {
	EventID  string `json:"event_id"`
	RelayURL string `json:"relay_url"`
}

type Layer1InvalidEvent struct {
	SourceRelay string `json:"source_relay"`
	ErrorCode   string `json:"error_code"`
}

type LayerJobStatus struct {
	JobType  string `json:"job_type"`
	Attempts int    `json:"attempts"`
}

type EventReferenceRow struct {
	SourceEventID     string `json:"source_event_id"`
	ReferencedEventID string `json:"referenced_event_id"`
	Relation          string `json:"relation"`
	TagIndex          int    `json:"tag_index"`
}

type ReplaceableStateRow struct {
	Pubkey    string `json:"pubkey"`
	Kind      int    `json:"kind"`
	DTag      string `json:"d_tag"`
	EventID   string `json:"event_id"`
	CreatedAt int64  `json:"created_at"`
}

type ThreadEdgeRow struct {
	ChildEventID  string `json:"child_event_id"`
	ParentEventID string `json:"parent_event_id"`
	ParentMissing bool   `json:"parent_missing"`
}

type UnresolvedThreadRow struct {
	SourceEventID  string `json:"source_event_id"`
	MissingEventID string `json:"missing_event_id"`
}

type ProfileLatestRow struct {
	Pubkey          string `json:"pubkey"`
	MetadataEventID string `json:"metadata_event_id"`
}

type AuthorRecentRow struct {
	AuthorPubkey string `json:"author_pubkey"`
	EventID      string `json:"event_id"`
	CreatedAt    int64  `json:"created_at"`
}

type CountRow struct {
	EventID string `json:"event_id"`
	Count   int64  `json:"count"`
}

type FollowerEdgeRow struct {
	FollowedPubkey string `json:"followed_pubkey"`
	FollowerPubkey string `json:"follower_pubkey"`
	SourceEventID  string `json:"source_event_id"`
}

type DMUnreadCountRow struct {
	ReceiverPubkey string `json:"receiver_pubkey"`
	SenderPubkey   string `json:"sender_pubkey"`
	Count          int64  `json:"cnt"`
	LatestAt       int64  `json:"latest_at"`
	LatestEventID  string `json:"latest_event_id"`
}

type ZapReceiptRow struct {
	ZapReceiptID   string `json:"zap_receipt_id"`
	EventID        string `json:"event_id"`
	SenderPubkey   string `json:"sender_pubkey"`
	ReceiverPubkey string `json:"receiver_pubkey"`
	AmountSats     int64  `json:"amount_sats"`
}

type CuratedReadRow struct {
	EventID string  `json:"event_id"`
	Rank    float64 `json:"rank"`
}

type CuratedTopicRow struct {
	Topic string  `json:"topic"`
	Rank  float64 `json:"rank"`
}

type CuratedAuthorRow struct {
	Pubkey string  `json:"pubkey"`
	Rank   float64 `json:"rank"`
}

type CuratedPaidTierRow struct {
	Pubkey    string `json:"pubkey"`
	TierID    string `json:"tier_id"`
	Title     string `json:"title"`
	PriceSats int64  `json:"price_sats"`
}

func CaptureStateSnapshot(ctx context.Context, pool *pgxpool.Pool) (StateSnapshot, error) {
	if pool == nil {
		return StateSnapshot{}, fmt.Errorf("pool is required")
	}
	out := StateSnapshot{}

	if err := queryRows(ctx, pool, &out.Layer1.Events, `
		SELECT id, pubkey, created_at, kind
		FROM events
		ORDER BY id ASC
	`); err != nil {
		return out, err
	}
	if err := queryRows(ctx, pool, &out.Layer1.EventTags, `
		SELECT event_id, tag_name, tag_index, value_index, value
		FROM event_tags
		ORDER BY event_id ASC, tag_index ASC, value_index ASC
	`); err != nil {
		return out, err
	}
	if err := queryRows(ctx, pool, &out.Layer1.EventRelays, `
		SELECT event_id, relay_url
		FROM event_relays
		ORDER BY event_id ASC, relay_url ASC
	`); err != nil {
		return out, err
	}
	if err := queryRows(ctx, pool, &out.Layer1.InvalidEvents, `
		SELECT source_relay, error_code
		FROM invalid_events
		ORDER BY source_relay ASC, error_code ASC, id ASC
	`); err != nil {
		return out, err
	}
	if err := queryRows(ctx, pool, &out.Layer1.PendingJobs, `
		SELECT job_type, attempts
		FROM jobs
		WHERE status = 'pending'
		ORDER BY job_type ASC, id ASC
	`); err != nil {
		return out, err
	}
	if err := queryRows(ctx, pool, &out.Layer1.SucceededJobs, `
		SELECT job_type, attempts
		FROM jobs
		WHERE status = 'succeeded'
		ORDER BY job_type ASC, id ASC
	`); err != nil {
		return out, err
	}
	if err := queryRows(ctx, pool, &out.Layer1.DeadLetterJobs, `
		SELECT job_type, attempts
		FROM jobs
		WHERE status = 'dead'
		ORDER BY job_type ASC, id ASC
	`); err != nil {
		return out, err
	}

	if err := queryRows(ctx, pool, &out.Layer2.EventReferences, `
		SELECT source_event_id, referenced_event_id, relation, tag_index
		FROM event_references
		ORDER BY source_event_id ASC, tag_index ASC, referenced_event_id ASC, relation ASC
	`); err != nil {
		return out, err
	}
	if err := queryRows(ctx, pool, &out.Layer2.ReplaceableState, `
		SELECT pubkey, kind, d_tag, event_id, created_at
		FROM replaceable_state
		ORDER BY pubkey ASC, kind ASC, d_tag ASC
	`); err != nil {
		return out, err
	}
	if err := queryRows(ctx, pool, &out.Layer2.ThreadEdges, `
		SELECT child_event_id, parent_event_id, parent_missing
		FROM thread_edges
		ORDER BY child_event_id ASC
	`); err != nil {
		return out, err
	}
	if err := queryRows(ctx, pool, &out.Layer2.UnresolvedThread, `
		SELECT source_event_id, missing_event_id
		FROM unresolved_thread_references
		ORDER BY source_event_id ASC, missing_event_id ASC
	`); err != nil {
		return out, err
	}

	if err := queryRows(ctx, pool, &out.Layer3.ProfilesLatest, `
		SELECT pubkey, metadata_event_id
		FROM profiles_latest
		ORDER BY pubkey ASC
	`); err != nil {
		return out, err
	}
	if err := queryRows(ctx, pool, &out.Layer3.AuthorRecent, `
		SELECT author_pubkey, event_id, created_at
		FROM author_recent_events
		ORDER BY author_pubkey ASC, created_at DESC, event_id DESC
	`); err != nil {
		return out, err
	}
	if err := queryRows(ctx, pool, &out.Layer3.ReplyCounts, `
		SELECT event_id, count
		FROM reply_counts
		ORDER BY event_id ASC
	`); err != nil {
		return out, err
	}
	if err := queryRows(ctx, pool, &out.Layer3.ReactionCounts, `
		SELECT event_id, count
		FROM reaction_counts
		ORDER BY event_id ASC
	`); err != nil {
		return out, err
	}
	if err := queryRows(ctx, pool, &out.Layer3.RepostCounts, `
		SELECT event_id, count
		FROM repost_counts
		ORDER BY event_id ASC
	`); err != nil {
		return out, err
	}
	if err := queryRows(ctx, pool, &out.Layer3.FollowerEdges, `
		SELECT followed_pubkey, follower_pubkey, source_event_id
		FROM follower_edges
		ORDER BY followed_pubkey ASC, follower_pubkey ASC
	`); err != nil {
		return out, err
	}
	if err := queryRows(ctx, pool, &out.Layer3.DMUnreadCounts, `
		SELECT receiver_pubkey, sender_pubkey, cnt, latest_at, latest_event_id
		FROM dm_unread_counts
		ORDER BY receiver_pubkey ASC, sender_pubkey ASC
	`); err != nil {
		return out, err
	}
	if err := queryRows(ctx, pool, &out.Layer3.ZapReceipts, `
		SELECT zap_receipt_id, COALESCE(event_id, ''), sender_pubkey, COALESCE(receiver_pubkey, ''), amount_sats
		FROM zap_receipts
		ORDER BY zap_receipt_id ASC
	`); err != nil {
		return out, err
	}
	if err := queryRows(ctx, pool, &out.Layer3.CuratedReads, `
		SELECT event_id, rank
		FROM curated_recommended_reads
		ORDER BY rank DESC, event_id ASC
	`); err != nil {
		return out, err
	}
	if err := queryRows(ctx, pool, &out.Layer3.CuratedTopics, `
		SELECT topic, rank
		FROM curated_reads_topics
		ORDER BY rank DESC, topic ASC
	`); err != nil {
		return out, err
	}
	if err := queryRows(ctx, pool, &out.Layer3.CuratedAuthors, `
		SELECT pubkey, rank
		FROM curated_featured_authors
		ORDER BY rank DESC, pubkey ASC
	`); err != nil {
		return out, err
	}
	if err := queryRows(ctx, pool, &out.Layer3.CuratedPaidTiers, `
		SELECT pubkey, tier_id, title, price_sats
		FROM curated_creator_paid_tiers
		ORDER BY pubkey ASC, price_sats ASC, tier_id ASC
	`); err != nil {
		return out, err
	}

	return out, nil
}

func queryRows[T any](ctx context.Context, pool *pgxpool.Pool, out *[]T, sql string) error {
	rows, err := pool.Query(ctx, sql)
	if err != nil {
		return fmt.Errorf("query snapshot rows: %w", err)
	}
	defer rows.Close()

	items := make([]T, 0)
	for rows.Next() {
		var row T
		switch v := any(&row).(type) {
		case *Layer1Event:
			if err := rows.Scan(&v.ID, &v.Pubkey, &v.CreatedAt, &v.Kind); err != nil {
				return fmt.Errorf("scan layer1 event row: %w", err)
			}
		case *Layer1EventTag:
			if err := rows.Scan(&v.EventID, &v.TagName, &v.TagIndex, &v.ValueIndex, &v.Value); err != nil {
				return fmt.Errorf("scan layer1 event tag row: %w", err)
			}
		case *Layer1EventRelay:
			if err := rows.Scan(&v.EventID, &v.RelayURL); err != nil {
				return fmt.Errorf("scan layer1 relay row: %w", err)
			}
		case *Layer1InvalidEvent:
			if err := rows.Scan(&v.SourceRelay, &v.ErrorCode); err != nil {
				return fmt.Errorf("scan invalid event row: %w", err)
			}
		case *LayerJobStatus:
			if err := rows.Scan(&v.JobType, &v.Attempts); err != nil {
				return fmt.Errorf("scan job status row: %w", err)
			}
		case *EventReferenceRow:
			if err := rows.Scan(&v.SourceEventID, &v.ReferencedEventID, &v.Relation, &v.TagIndex); err != nil {
				return fmt.Errorf("scan event reference row: %w", err)
			}
		case *ReplaceableStateRow:
			if err := rows.Scan(&v.Pubkey, &v.Kind, &v.DTag, &v.EventID, &v.CreatedAt); err != nil {
				return fmt.Errorf("scan replaceable state row: %w", err)
			}
		case *ThreadEdgeRow:
			if err := rows.Scan(&v.ChildEventID, &v.ParentEventID, &v.ParentMissing); err != nil {
				return fmt.Errorf("scan thread edge row: %w", err)
			}
		case *UnresolvedThreadRow:
			if err := rows.Scan(&v.SourceEventID, &v.MissingEventID); err != nil {
				return fmt.Errorf("scan unresolved row: %w", err)
			}
		case *ProfileLatestRow:
			if err := rows.Scan(&v.Pubkey, &v.MetadataEventID); err != nil {
				return fmt.Errorf("scan profile latest row: %w", err)
			}
		case *AuthorRecentRow:
			if err := rows.Scan(&v.AuthorPubkey, &v.EventID, &v.CreatedAt); err != nil {
				return fmt.Errorf("scan author recent row: %w", err)
			}
		case *CountRow:
			if err := rows.Scan(&v.EventID, &v.Count); err != nil {
				return fmt.Errorf("scan count row: %w", err)
			}
		default:
			return fmt.Errorf("unsupported snapshot row type")
		}
		items = append(items, row)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate snapshot rows: %w", err)
	}
	*out = items
	return nil
}
