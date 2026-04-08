package api

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/xdzczk/nostrmash/internal/query"
)

func (h Handlers) GetNoteSummary(w http.ResponseWriter, r *http.Request) {
	eventID := strings.TrimSpace(r.PathValue("event_id"))
	if eventID == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "event id is required")
		return
	}
	includeActivity, err := parseBoolQuery(r, "include_activity", true)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	summary, err := h.service.GetNotePageSummary(r.Context(), eventID, includeActivity)
	if err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "event not found")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	payload := map[string]any{
		"event_id": summary.EventID,
		"event":    summary.Event,
		"author": map[string]any{
			"pubkey":              summary.Author.Profile.Pubkey,
			"metadata_event_id":   summary.Author.Profile.MetadataEventID,
			"metadata_created_at": summary.Author.Profile.MetadataCreatedAt,
			"profile":             summary.Author.Profile.ProfileJSON,
			"stats": map[string]any{
				"follower_count":     summary.Author.Stats.FollowerCount,
				"following_count":    summary.Author.Stats.FollowingCount,
				"note_count":         summary.Author.Stats.NoteCount,
				"reply_count":        summary.Author.Stats.ReplyCount,
				"recent_activity_at": summary.Author.Stats.RecentActivityAt,
			},
		},
		"counts": map[string]any{
			"reply_count":    summary.Counts.ReplyCount,
			"reaction_count": summary.Counts.ReactionCount,
			"repost_count":   summary.Counts.RepostCount,
			"zap_count":      summary.Counts.ZapCount,
			"zap_msats":      summary.Counts.ZapMSats,
		},
		"media": map[string]any{
			"has_image":        summary.Media.HasImage,
			"has_video":        summary.Media.HasVideo,
			"has_link":         summary.Media.HasLink,
			"has_article":      summary.Media.HasArticle,
			"attachment_count": summary.Media.AttachmentCount,
		},
		"thread": map[string]any{
			"root_event_id":        summary.RootEventID,
			"parent_event_id":      summary.ParentEventID,
			"missing_ancestor_ids": summary.MissingAncestorIDs,
		},
		"quote_repost_context": map[string]any{
			"reference_event_id": summary.ReferenceEventID,
			"reference_event":    summary.ReferenceEvent,
		},
		"consistency": summary.Consistency,
	}
	if summary.QuoteRepostLinkage != nil {
		recent := make([]map[string]any, 0, len(summary.QuoteRepostLinkage.RecentActivity))
		for _, activity := range summary.QuoteRepostLinkage.RecentActivity {
			item := map[string]any{
				"event_id":     activity.EventID,
				"actor_pubkey": activity.ActorPubkey,
				"created_at":   activity.CreatedAt,
				"action":       activity.Action,
				"linked_note": map[string]any{
					"event_id":      activity.LinkedNote.EventID,
					"author_pubkey": activity.LinkedNote.AuthorPubkey,
					"created_at":    activity.LinkedNote.CreatedAt,
					"content":       activity.LinkedNote.Content,
				},
			}
			if strings.TrimSpace(activity.Quote) != "" {
				item["quote"] = activity.Quote
			}
			recent = append(recent, item)
		}
		payload["quote_repost_context"].(map[string]any)["linkage"] = map[string]any{
			"event_id":        summary.QuoteRepostLinkage.EventID,
			"quote_count":     summary.QuoteRepostLinkage.QuoteCount,
			"repost_count":    summary.QuoteRepostLinkage.RepostCount,
			"recent_activity": recent,
		}
	}
	if summary.ConversationActivity != nil {
		payload["conversation_activity"] = map[string]any{
			"replies_24h": summary.ConversationActivity.Replies24h,
			"replies_7d":  summary.ConversationActivity.Replies7d,
		}
	}
	writeJSON(w, http.StatusOK, payload)
}

func (h Handlers) GetNoteRelated(w http.ResponseWriter, r *http.Request) {
	eventID := strings.TrimSpace(r.PathValue("event_id"))
	if eventID == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "event id is required")
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 10, 50)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	related, err := h.service.GetNoteRelated(r.Context(), eventID, limit)
	if err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "event not found")
			return
		}
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "related notes are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	items := make([]map[string]any, 0, len(related))
	for _, note := range related {
		items = append(items, map[string]any{
			"event_id":      note.EventID,
			"author_pubkey": note.AuthorPubkey,
			"created_at":    note.CreatedAt,
			"content":       note.Content,
			"event":         note.Event,
			"counts": map[string]any{
				"reply_count":    note.Counts.ReplyCount,
				"reaction_count": note.Counts.ReactionCount,
				"repost_count":   note.Counts.RepostCount,
				"zap_count":      note.Counts.ZapCount,
				"zap_msats":      note.Counts.ZapMSats,
			},
			"reasons": note.Reasons,
			"score":   note.Score,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"event_id":    eventID,
		"related":     items,
		"consistency": "eventual",
	})
}

func parseBoolQuery(r *http.Request, key string, defaultValue bool) (bool, error) {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return defaultValue, nil
	}
	parsed, err := strconv.ParseBool(raw)
	if err != nil {
		return false, errors.New(key + " must be a boolean")
	}
	return parsed, nil
}
