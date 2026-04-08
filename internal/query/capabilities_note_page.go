package query

import (
	"context"

	"github.com/xdzczk/nostrmash/internal/store"
)

type noteStatsCapability interface {
	GetNoteStats(ctx context.Context, eventID string) (NoteEngagementCounts, NoteMediaFlags, error)
}

type noteConversationVelocityCapability interface {
	GetNoteConversationVelocity(ctx context.Context, eventID string) (NoteConversationActivity, error)
}

type relatedNotesCapability interface {
	GetRelatedNotes(ctx context.Context, eventID string, limit int) ([]RelatedNote, error)
}

type noteQuoteRepostLinkageCapability interface {
	GetNoteQuoteRepostLinkage(ctx context.Context, eventID string, recentLimit int) (NoteQuoteRepostLinkageSummary, error)
}

func adaptNotePageCapabilities(reader any, caps *serviceCapabilities) {
	if r, ok := reader.(noteStatsCapability); ok {
		caps.notePage.noteStats = r
	} else if legacy, ok := reader.(legacyNoteStatsCapability); ok {
		caps.notePage.noteStats = legacyNoteStatsAdapter{legacy: legacy}
	}
	if r, ok := reader.(noteConversationVelocityCapability); ok {
		caps.notePage.conversationVelocity = r
	} else if legacy, ok := reader.(legacyNoteConversationVelocityCapability); ok {
		caps.notePage.conversationVelocity = legacyNoteConversationVelocityAdapter{legacy: legacy}
	}
	if r, ok := reader.(relatedNotesCapability); ok {
		caps.notePage.relatedNotes = r
	} else if legacy, ok := reader.(legacyRelatedNotesCapability); ok {
		caps.notePage.relatedNotes = legacyRelatedNotesAdapter{legacy: legacy}
	}
	if r, ok := reader.(noteQuoteRepostLinkageCapability); ok {
		caps.notePage.quoteRepostLinkage = r
	} else if legacy, ok := reader.(legacyNoteQuoteRepostLinkageCapability); ok {
		caps.notePage.quoteRepostLinkage = legacyNoteQuoteRepostLinkageAdapter{legacy: legacy}
	}
}

type legacyNoteStatsCapability interface {
	GetNoteStats(ctx context.Context, eventID string) (store.NoteStats, error)
}

type legacyNoteStatsAdapter struct {
	legacy legacyNoteStatsCapability
}

func (a legacyNoteStatsAdapter) GetNoteStats(ctx context.Context, eventID string) (NoteEngagementCounts, NoteMediaFlags, error) {
	row, err := a.legacy.GetNoteStats(ctx, eventID)
	if err != nil {
		return NoteEngagementCounts{}, NoteMediaFlags{}, err
	}
	counts, media := noteStatsFromStore(row)
	return counts, media, nil
}

type legacyNoteConversationVelocityCapability interface {
	GetNoteConversationVelocity(ctx context.Context, eventID string) (store.NoteConversationVelocity, error)
}

type legacyNoteConversationVelocityAdapter struct {
	legacy legacyNoteConversationVelocityCapability
}

func (a legacyNoteConversationVelocityAdapter) GetNoteConversationVelocity(ctx context.Context, eventID string) (NoteConversationActivity, error) {
	row, err := a.legacy.GetNoteConversationVelocity(ctx, eventID)
	if err != nil {
		return NoteConversationActivity{}, err
	}
	return noteConversationActivityFromStore(row), nil
}

type legacyRelatedNotesCapability interface {
	GetRelatedNotes(ctx context.Context, eventID string, limit int) ([]store.RelatedNote, error)
}

type legacyRelatedNotesAdapter struct {
	legacy legacyRelatedNotesCapability
}

func (a legacyRelatedNotesAdapter) GetRelatedNotes(ctx context.Context, eventID string, limit int) ([]RelatedNote, error) {
	rows, err := a.legacy.GetRelatedNotes(ctx, eventID, limit)
	if err != nil {
		return nil, err
	}
	out := make([]RelatedNote, 0, len(rows))
	for _, row := range rows {
		out = append(out, relatedNoteFromStore(row))
	}
	return out, nil
}

type legacyNoteQuoteRepostLinkageCapability interface {
	GetNoteQuoteRepostLinkage(
		ctx context.Context,
		eventID string,
		recentLimit int,
	) (store.NoteQuoteRepostLinkageProjection, error)
}

type legacyNoteQuoteRepostLinkageAdapter struct {
	legacy legacyNoteQuoteRepostLinkageCapability
}

func (a legacyNoteQuoteRepostLinkageAdapter) GetNoteQuoteRepostLinkage(
	ctx context.Context,
	eventID string,
	recentLimit int,
) (NoteQuoteRepostLinkageSummary, error) {
	row, err := a.legacy.GetNoteQuoteRepostLinkage(ctx, eventID, recentLimit)
	if err != nil {
		return NoteQuoteRepostLinkageSummary{}, err
	}
	return noteQuoteRepostLinkageFromStore(row), nil
}
