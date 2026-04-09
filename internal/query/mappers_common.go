package query

import "github.com/xdzczk/nostrmash/internal/store"

func eventCursorFromStore(cursor *store.EventOrderCursor) *EventCursor {
	if cursor == nil {
		return nil
	}
	return &EventCursor{
		CreatedAt: cursor.CreatedAt,
		ID:        cursor.ID,
	}
}

func eventCursorToStore(cursor *EventCursor) *store.EventOrderCursor {
	if cursor == nil {
		return nil
	}
	return &store.EventOrderCursor{
		CreatedAt: cursor.CreatedAt,
		ID:        cursor.ID,
	}
}

func profileFromStore(row store.ProfileProjection) Profile {
	return Profile{
		Pubkey:            row.Pubkey,
		MetadataEventID:   row.MetadataEventID,
		MetadataCreatedAt: row.MetadataCreatedAt,
		ProfileJSON:       row.ProfileJSON,
	}
}

func profilePublicStatsFromStore(row store.ProfilePublicStatsProjection) ProfilePublicStats {
	return ProfilePublicStats{
		Pubkey:           row.Pubkey,
		FollowerCount:    row.FollowerCount,
		FollowingCount:   row.FollowingCount,
		NoteCount:        row.NoteCount,
		ReplyCount:       row.ReplyCount,
		RecentActivityAt: row.RecentActivityAt,
	}
}

func contactListFromStore(row store.ContactListProjection) ContactList {
	return ContactList{
		Pubkey:          row.Pubkey,
		EventID:         row.EventID,
		CreatedAt:       row.CreatedAt,
		DerivationVer:   row.DerivationVer,
		ContactsJSONRaw: row.ContactsJSONRaw,
	}
}

func relayListFromStore(row store.RelayListProjection) RelayList {
	return RelayList{
		Pubkey:        row.Pubkey,
		EventID:       row.EventID,
		CreatedAt:     row.CreatedAt,
		DerivationVer: row.DerivationVer,
		RelaysJSONRaw: row.RelaysJSONRaw,
	}
}

func languageSummaryFromStore(row store.LanguageSummary) LanguageSummary {
	return LanguageSummary{
		Language: row.Language,
		Count:    row.Count,
	}
}

func relayUsageFromStore(row store.RelayUsageSummary) RelayUsageSummary {
	return RelayUsageSummary{
		RelayURL:      row.RelayURL,
		EventCount:    row.EventCount,
		UniqueAuthors: row.UniqueAuthors,
	}
}
