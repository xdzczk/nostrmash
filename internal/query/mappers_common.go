package query

import "github.com/xdzczk/nostrmash/internal/readmodel"

func eventCursorFromStore(cursor *readmodel.EventOrderCursor) *EventCursor {
	if cursor == nil {
		return nil
	}
	return &EventCursor{
		CreatedAt: cursor.CreatedAt,
		ID:        cursor.ID,
	}
}

func eventCursorToStore(cursor *EventCursor) *readmodel.EventOrderCursor {
	if cursor == nil {
		return nil
	}
	return &readmodel.EventOrderCursor{
		CreatedAt: cursor.CreatedAt,
		ID:        cursor.ID,
	}
}

func profileFromStore(row readmodel.ProfileProjection) Profile {
	return Profile{
		Pubkey:            row.Pubkey,
		MetadataEventID:   row.MetadataEventID,
		MetadataCreatedAt: row.MetadataCreatedAt,
		ProfileJSON:       row.ProfileJSON,
	}
}

func profilePublicStatsFromStore(row readmodel.ProfilePublicStatsProjection) ProfilePublicStats {
	return ProfilePublicStats{
		Pubkey:           row.Pubkey,
		FollowerCount:    row.FollowerCount,
		FollowingCount:   row.FollowingCount,
		NoteCount:        row.NoteCount,
		ReplyCount:       row.ReplyCount,
		RecentActivityAt: row.RecentActivityAt,
	}
}

func contactListFromStore(row readmodel.ContactListProjection) ContactList {
	return ContactList{
		Pubkey:          row.Pubkey,
		EventID:         row.EventID,
		CreatedAt:       row.CreatedAt,
		DerivationVer:   row.DerivationVer,
		ContactsJSONRaw: row.ContactsJSONRaw,
	}
}

func relayListFromStore(row readmodel.RelayListProjection) RelayList {
	return RelayList{
		Pubkey:        row.Pubkey,
		EventID:       row.EventID,
		CreatedAt:     row.CreatedAt,
		DerivationVer: row.DerivationVer,
		RelaysJSONRaw: row.RelaysJSONRaw,
	}
}

func languageSummaryFromStore(row readmodel.LanguageSummary) LanguageSummary {
	return LanguageSummary{
		Language: row.Language,
		Count:    row.Count,
	}
}

func relayUsageFromStore(row readmodel.RelayUsageSummary) RelayUsageSummary {
	return RelayUsageSummary{
		RelayURL:      row.RelayURL,
		EventCount:    row.EventCount,
		UniqueAuthors: row.UniqueAuthors,
	}
}

func searchDocumentFromStore(row readmodel.SearchDocumentProjection) SearchDocument {
	return SearchDocument{
		EntityType:     row.EntityType,
		EntityID:       row.EntityID,
		Title:          row.Title,
		Body:           row.Body,
		Aliases:        row.Aliases,
		IdentityTokens: row.IdentityTokens,
		Freshness:      row.Freshness,
		Popularity:     row.Popularity,
		TrustScore:     row.TrustScore,
		Score:          row.Score,
	}
}
