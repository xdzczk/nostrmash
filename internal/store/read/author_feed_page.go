package read

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/xdzczk/nostrmash/internal/nostr"
)

// authorFeedTwoPhaseMin is the follow-list size where fetching one newest
// note per author, then expanding only the authors that can appear on the
// page, is cheaper than a full innerLimit lateral over every author.
const authorFeedTwoPhaseMin = 32

type authorHead struct {
	pubkey  string
	id      string
	created int64
}

func feedUsesTopLevelIndex(kinds []int, includeReplies, onlyReplies, hasImage bool) bool {
	if includeReplies || onlyReplies || hasImage {
		return false
	}
	kinds = feedKindLiterals(kinds)
	if len(kinds) == 0 {
		return false
	}
	for _, kind := range kinds {
		if kind != 1 && kind != 6 {
			return false
		}
	}
	return true
}

func (s *Read) authorFeedIndexReady(ctx context.Context) bool {
	if s == nil || s.pool == nil {
		return false
	}
	var complete bool
	err := s.pool.QueryRow(ctx, `SELECT complete FROM author_feed_index_state WHERE id`).Scan(&complete)
	return err == nil && complete
}

// queryAuthorFeedTwoPhase keeps the follows page correct without reading
// innerLimit candidates from every followed author. The newest top-level
// note per author is enough to know who can contribute to the global top
// window: anyone whose newest note is older than that cutoff cannot.
func (s *Read) queryAuthorFeedTwoPhase(
	ctx context.Context,
	authors []string,
	kinds []int,
	since int64,
	until int64,
	limit int,
	offset int,
	fromIndex bool,
) ([]json.RawMessage, error) {
	window := limit + offset
	if window < limit {
		window = limit
	}
	if window > 100 {
		window = 100
	}
	if window <= 0 {
		return []json.RawMessage{}, nil
	}
	heads, err := s.authorFeedHeads(ctx, authors, kinds, since, until, fromIndex)
	if err != nil {
		return nil, err
	}
	picked := authorsAtFeedCutoff(heads, window)
	if len(picked) == 0 {
		return []json.RawMessage{}, nil
	}
	chunks := chunkStrings(picked, authorFeedChunkSize)
	pages := make([][]json.RawMessage, len(chunks))
	if len(chunks) == 1 {
		rows, err := s.queryTopLevelAuthorPage(ctx, chunks[0], kinds, since, until, window, 0, fromIndex)
		if err != nil {
			return nil, err
		}
		return mergeAuthorFeedPages([][]json.RawMessage{rows}, limit, offset), nil
	}
	parent := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	sem := make(chan struct{}, 3)
	for i, chunk := range chunks {
		wg.Add(1)
		go func(i int, chunk []string) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			rows, err := s.queryTopLevelAuthorPage(ctx, chunk, kinds, since, until, window, 0, fromIndex)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = err
					cancel()
				}
				return
			}
			pages[i] = rows
		}(i, chunk)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	if err := parent.Err(); err != nil {
		return nil, err
	}
	return mergeAuthorFeedPages(pages, limit, offset), nil
}

func (s *Read) queryTopLevelAuthorPage(
	ctx context.Context,
	authors []string,
	kinds []int,
	since int64,
	until int64,
	limit int,
	offset int,
	fromIndex bool,
) ([]json.RawMessage, error) {
	if fromIndex {
		return s.queryAuthorFeedIndex(ctx, authors, kinds, since, until, limit, offset)
	}
	return s.queryAuthorFeed(ctx, authors, kinds, since, until, limit, offset, false, false, false)
}

func (s *Read) queryAuthorFeedIndex(
	ctx context.Context,
	authors []string,
	kinds []int,
	since int64,
	until int64,
	limit int,
	offset int,
) ([]json.RawMessage, error) {
	innerLimit := limit + offset
	if innerLimit < limit {
		innerLimit = limit
	}
	if innerLimit > 100 {
		innerLimit = 100
	}
	rows, err := s.pool.Query(ctx, authorFeedIndexPageSQL(kinds), authors, since, until, limit, offset, innerLimit)
	if err != nil {
		return nil, fmt.Errorf("get feed events: %w", err)
	}
	return scanFeedRaw(rows)
}

func (s *Read) authorFeedHeads(
	ctx context.Context,
	authors []string,
	kinds []int,
	since int64,
	until int64,
	fromIndex bool,
) ([]authorHead, error) {
	chunks := chunkStrings(authors, authorFeedChunkSize)
	pages := make([][]authorHead, len(chunks))
	if len(chunks) == 1 {
		return s.queryAuthorFeedHeads(ctx, chunks[0], kinds, since, until, fromIndex)
	}
	parent := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	sem := make(chan struct{}, 3)
	for i, chunk := range chunks {
		wg.Add(1)
		go func(i int, chunk []string) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			rows, err := s.queryAuthorFeedHeads(ctx, chunk, kinds, since, until, fromIndex)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = err
					cancel()
				}
				return
			}
			pages[i] = rows
		}(i, chunk)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	if err := parent.Err(); err != nil {
		return nil, err
	}
	total := 0
	for _, page := range pages {
		total += len(page)
	}
	out := make([]authorHead, 0, total)
	for _, page := range pages {
		out = append(out, page...)
	}
	return out, nil
}

func (s *Read) queryAuthorFeedHeads(
	ctx context.Context,
	authors []string,
	kinds []int,
	since int64,
	until int64,
	fromIndex bool,
) ([]authorHead, error) {
	rows, err := s.pool.Query(ctx, authorHeadSQL(fromIndex, kinds), authors, since, until)
	if err != nil {
		return nil, fmt.Errorf("get feed heads: %w", err)
	}
	defer rows.Close()
	out := make([]authorHead, 0, len(authors))
	for rows.Next() {
		var head authorHead
		if err := rows.Scan(&head.pubkey, &head.id, &head.created); err != nil {
			return nil, fmt.Errorf("scan feed head: %w", err)
		}
		out = append(out, head)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read feed heads: %w", err)
	}
	return out, nil
}

// authorsAtFeedCutoff returns the pubkeys whose newest note can still fall
// inside a global page of window rows. heads is the newest top-level note
// per author.
func authorsAtFeedCutoff(heads []authorHead, window int) []string {
	if len(heads) == 0 || window <= 0 {
		return nil
	}
	sort.Slice(heads, func(i, j int) bool {
		if heads[i].created != heads[j].created {
			return heads[i].created > heads[j].created
		}
		return heads[i].id > heads[j].id
	})
	cutoff := heads[len(heads)-1].created
	if len(heads) > window {
		cutoff = heads[window-1].created
	}
	out := make([]string, 0, window)
	seen := make(map[string]struct{}, window)
	for _, head := range heads {
		if head.created < cutoff {
			break
		}
		if _, ok := seen[head.pubkey]; ok {
			continue
		}
		seen[head.pubkey] = struct{}{}
		out = append(out, head.pubkey)
	}
	return out
}

func authorHeadSQL(fromIndex bool, kinds []int) string {
	kinds = feedKindLiterals(kinds)
	var branches strings.Builder
	for i, kind := range kinds {
		if i > 0 {
			branches.WriteString("\n\t\t\t\tUNION ALL\n")
		}
		if fromIndex {
			fmt.Fprintf(&branches, `				(
					SELECT e.event_id AS id, e.created_at
					FROM author_feed_events e
					WHERE e.pubkey = author.pubkey
					  AND e.kind = %d
					  AND e.created_at >= $2
					  AND e.created_at <= $3
					ORDER BY e.created_at DESC, e.event_id DESC
					LIMIT 1
				)`, kind)
			continue
		}
		fmt.Fprintf(&branches, `				(
					SELECT e.id, e.created_at
					FROM events e
					WHERE e.pubkey = author.pubkey
					  AND e.kind = %d
					  AND e.created_at >= $2
					  AND e.created_at <= $3
					  AND NOT EXISTS (SELECT 1 FROM thread_edges te WHERE te.child_event_id = e.id)
					ORDER BY e.created_at DESC, e.id DESC
					LIMIT 1
				)`, kind)
	}
	return fmt.Sprintf(`
		SELECT author.pubkey, page.id, page.created_at
		FROM unnest($1::text[]) AS author(pubkey)
		JOIN LATERAL (
			SELECT id, created_at
			FROM (
%s
			) candidates
			ORDER BY created_at DESC, id DESC
			LIMIT 1
		) page ON true
	`, branches.String())
}

func authorFeedIndexPageSQL(kinds []int) string {
	kinds = feedKindLiterals(kinds)
	var branches strings.Builder
	for i, kind := range kinds {
		if i > 0 {
			branches.WriteString("\n\t\t\t\tUNION ALL\n")
		}
		fmt.Fprintf(&branches, `				(
					SELECT e.event_id AS id, e.created_at
					FROM author_feed_events e
					WHERE e.pubkey = author.pubkey
					  AND e.kind = %d
					  AND e.created_at >= $2
					  AND e.created_at <= $3
					ORDER BY e.created_at DESC, e.event_id DESC
					LIMIT $6
				)`, kind)
	}
	return fmt.Sprintf(`
		SELECT e.raw_json
		FROM (
			SELECT page.id, page.created_at
			FROM unnest($1::text[]) AS author(pubkey)
			JOIN LATERAL (
				SELECT id, created_at
				FROM (
%s
				) candidates
				ORDER BY created_at DESC, id DESC
				LIMIT $6
			) page ON true
			ORDER BY page.created_at DESC, page.id DESC
			LIMIT $4 OFFSET $5
		) chosen
		JOIN events e ON e.id = chosen.id
		WHERE %s
		ORDER BY chosen.created_at DESC, chosen.id DESC
	`, branches.String(), nostr.EventStillLiveSQL("e"))
}
