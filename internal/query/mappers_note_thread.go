package query

import (
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
)

func eventWithProvenanceFromStore(row store.EventWithProvenance) EventWithProvenance {
	relays := make([]model.EventRelay, 0, len(row.Relays))
	for _, relay := range row.Relays {
		relays = append(relays, model.EventRelay{
			EventID:  relay.EventID,
			RelayURL: relay.RelayURL,
			SeenAt:   relay.SeenAt.UTC(),
		})
	}
	return EventWithProvenance{
		Event:  row.Event,
		Relays: relays,
	}
}

func eventCountsFromStore(row store.EventCounts) EventCounts {
	return EventCounts{
		EventID:       row.EventID,
		ReplyCount:    row.ReplyCount,
		ReactionCount: row.ReactionCount,
		RepostCount:   row.RepostCount,
		ZapCount:      row.ZapCount,
		ZapMSats:      row.ZapMSats,
		Consistency:   row.Consistency,
	}
}

func threadSummaryFromStore(row store.ThreadSummaryProjection) ThreadSummary {
	return ThreadSummary{
		RootEventID:      row.RootEventID,
		ReplyCount:       row.ReplyCount,
		ParticipantCount: row.ParticipantCount,
		MaxDepth:         row.MaxDepth,
		LastActivityAt:   row.LastActivityAt,
		Velocity: ThreadVelocityHints{
			Replies24h: row.Replies24h,
			Replies7d:  row.Replies7d,
		},
		Consistency: row.Consistency,
	}
}

func noteStatsFromStore(row store.NoteStats) (NoteEngagementCounts, NoteMediaFlags) {
	return NoteEngagementCounts{
			ReplyCount:    row.ReplyCount,
			ReactionCount: row.ReactionCount,
			RepostCount:   row.RepostCount,
			ZapCount:      row.ZapCount,
			ZapMSats:      row.ZapMSats,
		}, NoteMediaFlags{
			HasImage:        row.HasImage,
			HasVideo:        row.HasVideo,
			HasLink:         row.HasLink,
			HasArticle:      row.HasArticle,
			AttachmentCount: row.AttachmentCount,
		}
}

func noteConversationActivityFromStore(row store.NoteConversationVelocity) NoteConversationActivity {
	return NoteConversationActivity{
		Replies24h: row.Replies24h,
		Replies7d:  row.Replies7d,
	}
}

func relatedNoteFromStore(row store.RelatedNote) RelatedNote {
	return RelatedNote{
		EventID:      row.EventID,
		AuthorPubkey: row.AuthorPubkey,
		CreatedAt:    row.CreatedAt,
		Content:      row.Content,
		Event:        row.Event,
		Counts: NoteEngagementCounts{
			ReplyCount:    row.ReplyCount,
			ReactionCount: row.ReactionCount,
			RepostCount:   row.RepostCount,
			ZapCount:      row.ZapCount,
			ZapMSats:      row.ZapMSats,
		},
		Reasons: row.Reasons,
		Score:   row.RankScore,
	}
}
