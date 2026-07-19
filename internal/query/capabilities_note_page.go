package query

import (
	"context"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

// Note-page capability interfaces are readmodel-shaped; the Service maps to
// query DTOs at the response edge via mappers_note_thread.go.

type noteStatsCapability interface {
	GetNoteStats(ctx context.Context, eventID string) (readmodel.NoteStats, error)
}

type noteConversationVelocityCapability interface {
	GetNoteConversationVelocity(ctx context.Context, eventID string) (readmodel.NoteConversationVelocity, error)
}

type relatedNotesCapability interface {
	GetRelatedNotes(ctx context.Context, eventID string, limit int) ([]readmodel.RelatedNote, error)
}

type noteQuoteRepostLinkageCapability interface {
	GetNoteQuoteRepostLinkage(
		ctx context.Context,
		eventID string,
		recentLimit int,
	) (readmodel.NoteQuoteRepostLinkageProjection, error)
}

func adaptNotePageCapabilities(reader any, caps *serviceCapabilities) {
	if r, ok := reader.(noteStatsCapability); ok {
		caps.notePage.noteStats = r
	}
	if r, ok := reader.(noteConversationVelocityCapability); ok {
		caps.notePage.conversationVelocity = r
	}
	if r, ok := reader.(relatedNotesCapability); ok {
		caps.notePage.relatedNotes = r
	}
	if r, ok := reader.(noteQuoteRepostLinkageCapability); ok {
		caps.notePage.quoteRepostLinkage = r
	}
}
