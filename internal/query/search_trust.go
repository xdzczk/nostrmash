package query

import (
	"context"
	"encoding/json"
	"fmt"
)

type trustedSearchNoteCandidate struct {
	note    json.RawMessage
	trusted bool
}

type trustedSearchProfileCandidate struct {
	profile Profile
	trusted bool
}

type noteTrustEnvelope struct {
	Pubkey string `json:"pubkey"`
}

func (s Service) searchNotesTrustAware(ctx context.Context, params NotesSearchParams) ([]json.RawMessage, error) {
	if s.searchTrustMode == trustModeOpen || params.Sort != "relevant" {
		return s.searchNotesPage(ctx, params)
	}
	if s.capabilities.trust.qualification == nil {
		if s.searchTrustMode == trustModeTrustedOnly {
			return nil, unsupportedCapabilityError("trust qualification")
		}
		return s.searchNotesPage(ctx, params)
	}

	targetRows := params.Limit + params.Offset
	if targetRows <= 0 {
		targetRows = params.Limit
	}
	if targetRows <= 0 {
		targetRows = 20
	}
	scanBudget := targetRows * 4
	if scanBudget > s.searchTrustScanSize {
		scanBudget = s.searchTrustScanSize
	}
	if scanBudget < targetRows {
		scanBudget = targetRows
	}

	candidates := make([]trustedSearchNoteCandidate, 0, targetRows)
	for fetched := 0; fetched < scanBudget; {
		batchSize := 100
		if remaining := scanBudget - fetched; remaining < batchSize {
			batchSize = remaining
		}
		batchParams := params
		batchParams.Limit = batchSize
		batchParams.Offset = fetched
		batch, err := s.searchNotesPage(ctx, batchParams)
		if err != nil {
			return nil, err
		}
		if len(batch) == 0 {
			break
		}
		qualified, err := s.qualifySearchNoteBatch(ctx, batch)
		if err != nil {
			return nil, err
		}
		if s.searchTrustMode == trustModeTrustedOnly {
			for _, row := range qualified {
				if row.trusted {
					candidates = append(candidates, row)
				}
			}
		} else {
			candidates = append(candidates, qualified...)
		}
		if len(candidates) >= targetRows {
			break
		}
		if len(batch) < batchSize {
			break
		}
		fetched += batchSize
	}
	return paginateSearchNotes(trustedSearchNoteRowsByMode(candidates, s.searchTrustMode), params.Limit, params.Offset), nil
}

func (s Service) searchProfilesTrustAware(ctx context.Context, params ProfileSearchParams) ([]Profile, error) {
	if s.searchTrustMode == trustModeOpen {
		return s.searchProfilesPage(ctx, params)
	}
	if s.capabilities.trust.qualification == nil {
		if s.searchTrustMode == trustModeTrustedOnly {
			return nil, unsupportedCapabilityError("trust qualification")
		}
		return s.searchProfilesPage(ctx, params)
	}

	targetRows := params.Limit + params.Offset
	if targetRows <= 0 {
		targetRows = params.Limit
	}
	if targetRows <= 0 {
		targetRows = 20
	}
	scanBudget := targetRows * 4
	if scanBudget > s.searchTrustScanSize {
		scanBudget = s.searchTrustScanSize
	}
	if scanBudget < targetRows {
		scanBudget = targetRows
	}

	candidates := make([]trustedSearchProfileCandidate, 0, targetRows)
	for fetched := 0; fetched < scanBudget; {
		batchSize := 100
		if remaining := scanBudget - fetched; remaining < batchSize {
			batchSize = remaining
		}
		batchParams := params
		batchParams.Limit = batchSize
		batchParams.Offset = fetched
		batch, err := s.searchProfilesPage(ctx, batchParams)
		if err != nil {
			return nil, err
		}
		if len(batch) == 0 {
			break
		}
		qualified, err := s.qualifySearchProfileBatch(ctx, batch)
		if err != nil {
			return nil, err
		}
		if s.searchTrustMode == trustModeTrustedOnly {
			for _, row := range qualified {
				if row.trusted {
					candidates = append(candidates, row)
				}
			}
		} else {
			candidates = append(candidates, qualified...)
		}
		if len(candidates) >= targetRows {
			break
		}
		if len(batch) < batchSize {
			break
		}
		fetched += batchSize
	}
	return paginateSearchProfiles(trustedSearchProfileRowsByMode(candidates, s.searchTrustMode), params.Limit, params.Offset), nil
}

func (s Service) searchNotesPage(ctx context.Context, params NotesSearchParams) ([]json.RawMessage, error) {
	if s.meilisearch != nil {
		rows, err := s.meilisearch.SearchNotes(
			ctx,
			params.Query,
			params.Sort,
			params.Window,
			params.Language,
			params.Limit,
			params.Offset,
		)
		if err == nil {
			return rows, nil
		}
	}
	if advanced, ok := s.reader.(notesSearchReader); ok {
		return advanced.SearchNotes(ctx, params.Query, params.Sort, params.Window, params.Language, params.Limit, params.Offset)
	}
	if params.Sort == "relevant" && params.Window == nil && params.Offset == 0 && params.Language == "" {
		return s.reader.SearchEventsByContent(ctx, params.Query, params.Limit)
	}
	return nil, unsupportedCapabilityError("advanced notes search")
}

func (s Service) searchProfilesPage(ctx context.Context, params ProfileSearchParams) ([]Profile, error) {
	if s.meilisearch != nil {
		rows, err := s.meilisearch.SearchProfiles(
			ctx,
			params.Query,
			params.Sort,
			params.Limit,
			params.Offset,
		)
		if err == nil {
			return rows, nil
		}
	}
	if advanced, ok := s.reader.(profilesSearchReader); ok {
		return advanced.SearchProfilesWithOptions(ctx, params.Query, params.Sort, params.Limit, params.Offset)
	}
	if params.Sort == "relevant" && params.Offset == 0 {
		return s.reader.SearchProfiles(ctx, params.Query, params.Limit)
	}
	return nil, unsupportedCapabilityError("advanced profile search")
}

func (s Service) qualifySearchNoteBatch(ctx context.Context, rows []json.RawMessage) ([]trustedSearchNoteCandidate, error) {
	pubkeys := make([]string, 0, len(rows))
	authors := make([]string, 0, len(rows))
	for _, row := range rows {
		var envelope noteTrustEnvelope
		_ = json.Unmarshal(row, &envelope)
		authors = append(authors, envelope.Pubkey)
		pubkeys = append(pubkeys, envelope.Pubkey)
	}
	trustRows, err := s.GetTrustQualification(ctx, pubkeys, s.searchTrustPolicy)
	if err != nil {
		return nil, fmt.Errorf("qualify note search candidates: %w", err)
	}
	out := make([]trustedSearchNoteCandidate, 0, len(rows))
	for i, row := range rows {
		trusted := false
		if author := authors[i]; author != "" {
			trusted = trustRows[author].Trusted
		}
		out = append(out, trustedSearchNoteCandidate{
			note:    row,
			trusted: trusted,
		})
	}
	return out, nil
}

func (s Service) qualifySearchProfileBatch(ctx context.Context, rows []Profile) ([]trustedSearchProfileCandidate, error) {
	pubkeys := make([]string, 0, len(rows))
	for _, row := range rows {
		pubkeys = append(pubkeys, row.Pubkey)
	}
	trustRows, err := s.GetTrustQualification(ctx, pubkeys, s.searchTrustPolicy)
	if err != nil {
		return nil, fmt.Errorf("qualify profile search candidates: %w", err)
	}
	out := make([]trustedSearchProfileCandidate, 0, len(rows))
	for _, row := range rows {
		out = append(out, trustedSearchProfileCandidate{
			profile: row,
			trusted: trustRows[row.Pubkey].Trusted,
		})
	}
	return out, nil
}

func trustedSearchNoteRowsByMode(rows []trustedSearchNoteCandidate, mode string) []json.RawMessage {
	if mode != trustModePreferTrusted {
		out := make([]json.RawMessage, 0, len(rows))
		for _, row := range rows {
			out = append(out, row.note)
		}
		return out
	}
	out := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		if row.trusted {
			out = append(out, row.note)
		}
	}
	for _, row := range rows {
		if !row.trusted {
			out = append(out, row.note)
		}
	}
	return out
}

func trustedSearchProfileRowsByMode(rows []trustedSearchProfileCandidate, mode string) []Profile {
	if mode != trustModePreferTrusted {
		out := make([]Profile, 0, len(rows))
		for _, row := range rows {
			out = append(out, row.profile)
		}
		return out
	}
	out := make([]Profile, 0, len(rows))
	for _, row := range rows {
		if row.trusted {
			out = append(out, row.profile)
		}
	}
	for _, row := range rows {
		if !row.trusted {
			out = append(out, row.profile)
		}
	}
	return out
}

func paginateSearchNotes(rows []json.RawMessage, limit int, offset int) []json.RawMessage {
	if offset >= len(rows) {
		return []json.RawMessage{}
	}
	end := offset + limit
	if end > len(rows) {
		end = len(rows)
	}
	return rows[offset:end]
}

func paginateSearchProfiles(rows []Profile, limit int, offset int) []Profile {
	if offset >= len(rows) {
		return []Profile{}
	}
	end := offset + limit
	if end > len(rows) {
		end = len(rows)
	}
	return rows[offset:end]
}
