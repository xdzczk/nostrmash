package query

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/traceutil"
)

type notePageEvent struct {
	ID        string     `json:"id"`
	Pubkey    string     `json:"pubkey"`
	CreatedAt int64      `json:"created_at"`
	Content   string     `json:"content"`
	Kind      int        `json:"kind"`
	Tags      [][]string `json:"tags"`
}

func (s Service) GetNotePageSummary(ctx context.Context, eventID string, includeConversation bool) (out NoteSummary, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.get_note_page_summary")
	defer func() { span.End(err) }()

	normalizedEventID := strings.TrimSpace(eventID)
	if normalizedEventID == "" {
		return NoteSummary{}, fmt.Errorf("event id is required")
	}
	raw, err := s.GetEventByID(ctx, normalizedEventID)
	if err != nil {
		return NoteSummary{}, err
	}
	parsed, err := parseNotePageEvent(raw)
	if err != nil {
		return NoteSummary{}, fmt.Errorf("parse note event: %w", err)
	}
	if strings.TrimSpace(parsed.ID) == "" {
		parsed.ID = normalizedEventID
	}

	counts, err := s.GetEventActionCounts(ctx, normalizedEventID)
	if err != nil {
		return NoteSummary{}, err
	}
	engagement := NoteEngagementCounts{
		ReplyCount:    counts.ReplyCount,
		ReactionCount: counts.ReactionCount,
		RepostCount:   counts.RepostCount,
	}
	media := deriveNoteMediaFlags(parsed)
	if capability := s.capabilities.notePage.noteStats; capability != nil {
		stats, mediaStats, statsErr := capability.GetNoteStats(ctx, normalizedEventID)
		if statsErr == nil {
			engagement = stats
			media = mediaStats
		}
	}

	author := ProfilePublicSummary{
		Profile: Profile{
			Pubkey: strings.TrimSpace(parsed.Pubkey),
		},
		Stats: ProfilePublicStats{
			Pubkey: strings.TrimSpace(parsed.Pubkey),
		},
	}
	if parsed.Pubkey != "" {
		summary, profileErr := s.GetProfilePublicSummary(ctx, parsed.Pubkey)
		if profileErr == nil {
			author = summary
		} else if !IsNotFound(profileErr) {
			return NoteSummary{}, profileErr
		}
	}

	var rootEventID *string
	var parentEventID *string
	missingAncestorIDs := []string{}
	ancestors, ancestorsErr := s.GetEventAncestors(ctx, normalizedEventID, 100)
	if ancestorsErr == nil {
		ancestorIDs := make([]string, 0, len(ancestors.Ancestors))
		for _, ancestor := range ancestors.Ancestors {
			if parsedAncestor, ok := parseAncestorID(ancestor); ok {
				ancestorIDs = append(ancestorIDs, parsedAncestor)
			}
		}
		if len(ancestorIDs) > 0 {
			rootEventID = stringPtr(ancestorIDs[0])
			parentEventID = stringPtr(ancestorIDs[len(ancestorIDs)-1])
		}
		missingAncestorIDs = ancestors.MissingAncestorIDs
	} else if !IsNotFound(ancestorsErr) {
		return NoteSummary{}, ancestorsErr
	}

	var referenceEventID *string
	referenceID := extractReferenceEventID(parsed)
	if referenceID != "" {
		referenceEventID = stringPtr(referenceID)
	}
	var referenceEvent json.RawMessage
	if referenceEventID != nil {
		var refErr error
		referenceEvent, refErr = s.GetEventByID(ctx, *referenceEventID)
		if refErr != nil && !IsNotFound(refErr) {
			// The referenced event is best-effort enrichment; do not fail the
			// whole note page, but surface the degradation instead of hiding it.
			span.SetAttr("reference_event.error", refErr.Error())
			metrics.IncAPIPartialResponse("note_page_summary", "reference_event")
		}
	}

	var conversation *NoteConversationActivity
	if includeConversation {
		if capability := s.capabilities.notePage.conversationVelocity; capability != nil {
			activity, activityErr := capability.GetNoteConversationVelocity(ctx, normalizedEventID)
			if activityErr == nil {
				conversation = &activity
			} else if !IsNotFound(activityErr) {
				return NoteSummary{}, activityErr
			}
		}
	}
	var quoteRepostLinkage *NoteQuoteRepostLinkageSummary
	if capability := s.capabilities.notePage.quoteRepostLinkage; capability != nil {
		linkage, linkageErr := capability.GetNoteQuoteRepostLinkage(ctx, normalizedEventID, 6)
		if linkageErr == nil {
			quoteRepostLinkage = &linkage
		} else if !IsNotFound(linkageErr) {
			return NoteSummary{}, linkageErr
		}
	}

	return NoteSummary{
		EventID:              parsed.ID,
		Event:                raw,
		Author:               author,
		Counts:               engagement,
		Media:                media,
		RootEventID:          rootEventID,
		ParentEventID:        parentEventID,
		MissingAncestorIDs:   missingAncestorIDs,
		ReferenceEventID:     referenceEventID,
		ReferenceEvent:       referenceEvent,
		QuoteRepostLinkage:   quoteRepostLinkage,
		ConversationActivity: conversation,
		Consistency:          "eventual",
	}, nil
}

func (s Service) GetNoteRelated(ctx context.Context, eventID string, limit int) (out []RelatedNote, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.get_note_related")
	defer func() { span.End(err) }()

	normalizedEventID := strings.TrimSpace(eventID)
	if normalizedEventID == "" {
		return nil, fmt.Errorf("event id is required")
	}
	if limit <= 0 {
		limit = 10
	}
	if limit > 50 {
		limit = 50
	}
	if cap := s.capabilities.notePage.relatedNotes; cap != nil {
		return cap.GetRelatedNotes(ctx, normalizedEventID, limit)
	}
	return nil, unsupportedCapabilityError("related notes")
}

func parseNotePageEvent(raw json.RawMessage) (notePageEvent, error) {
	var event notePageEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return notePageEvent{}, err
	}
	return event, nil
}

func parseAncestorID(raw json.RawMessage) (string, bool) {
	var payload struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return "", false
	}
	id := strings.TrimSpace(payload.ID)
	return id, id != ""
}

func extractReferenceEventID(event notePageEvent) string {
	type scoredReference struct {
		id    string
		score int
	}
	best := scoredReference{}
	for _, tag := range event.Tags {
		if len(tag) < 2 || strings.TrimSpace(tag[0]) != "e" {
			continue
		}
		id := strings.TrimSpace(tag[1])
		if id == "" {
			continue
		}
		score := 1
		if len(tag) > 3 {
			switch strings.ToLower(strings.TrimSpace(tag[3])) {
			case "quote":
				score = 4
			case "reply":
				score = 3
			case "root":
				score = 2
			}
		}
		if event.Kind == 6 && score < 3 {
			score = 3
		}
		if score > best.score {
			best = scoredReference{id: id, score: score}
		}
	}
	return best.id
}

func stringPtr(value string) *string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	v := value
	return &v
}

func deriveNoteMediaFlags(event notePageEvent) NoteMediaFlags {
	out := NoteMediaFlags{
		HasArticle: event.Kind == 30023 || len(strings.TrimSpace(event.Content)) >= 1200,
	}
	contentLower := strings.ToLower(event.Content)
	out.HasLink = strings.Contains(contentLower, "http://") || strings.Contains(contentLower, "https://")
	imageHints := []string{".png", ".jpg", ".jpeg", ".gif", ".webp"}
	videoHints := []string{".mp4", ".mov", ".webm", ".m4v"}
	for _, hint := range imageHints {
		if strings.Contains(contentLower, hint) {
			out.HasImage = true
			break
		}
	}
	for _, hint := range videoHints {
		if strings.Contains(contentLower, hint) {
			out.HasVideo = true
			break
		}
	}
	for _, tag := range event.Tags {
		if len(tag) == 0 {
			continue
		}
		tagKey := strings.ToLower(strings.TrimSpace(tag[0]))
		switch tagKey {
		case "image", "thumb":
			out.HasImage = true
			out.AttachmentCount++
		case "video":
			out.HasVideo = true
			out.AttachmentCount++
		case "imeta":
			out.AttachmentCount++
		case "r":
			if len(tag) > 1 && strings.TrimSpace(tag[1]) != "" {
				out.HasLink = true
			}
		}
	}
	return out
}
