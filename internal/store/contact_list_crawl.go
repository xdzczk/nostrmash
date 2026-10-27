package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// ContactListCrawlEntry is one pubkey claimed for a contact-list fetch.
type ContactListCrawlEntry struct {
	Pubkey string
	Hop    int
}

type contactListCrawlCursor struct {
	RecentActivityAt *int64
	Pubkey           string
	InNullActivity   bool
	NextSeedAt       time.Time
}

type contactListSeedCandidate struct {
	Pubkey           string
	RecentActivityAt *int64
}

func (c contactListCrawlCursor) seedMode() string {
	if c.InNullActivity {
		return "nulls"
	}
	if c.RecentActivityAt != nil {
		return "row"
	}
	return "begin"
}

// SeedContactListCrawl queues up to batch hop-0 pubkeys. Active trust seeds
// come first. The rest are note authors in profile_public_stats who have no
// contact_lists_latest row, walked with a keyset cursor so a full pass does
// not rescan the stats index on every cycle. Authors already queued are left
// alone (ON CONFLICT DO NOTHING), including their hop and cooldown.
func (s *PostgresStore) SeedContactListCrawl(ctx context.Context, batch int, rescanInterval time.Duration) (int, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	if batch <= 0 {
		return 0, nil
	}
	if rescanInterval <= 0 {
		rescanInterval = 6 * time.Hour
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin contact list crawl seed: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	inserted, err := insertContactListCrawlSeeds(ctx, tx, batch)
	if err != nil {
		return 0, err
	}
	if remaining := batch - inserted; remaining > 0 {
		n, seedErr := seedContactListAuthors(ctx, tx, remaining, rescanInterval)
		if seedErr != nil {
			return 0, seedErr
		}
		inserted += n
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit contact list crawl seed: %w", err)
	}
	return inserted, nil
}

func insertContactListCrawlSeeds(ctx context.Context, tx pgx.Tx, limit int) (int, error) {
	tag, err := tx.Exec(ctx, `
		INSERT INTO contact_list_crawl (pubkey, hop, next_eligible_at)
		SELECT pubkey, 0, now()
		FROM trust_seeds
		WHERE is_active
		  AND pubkey <> ''
		  AND NOT EXISTS (
			SELECT 1 FROM contact_list_crawl q WHERE q.pubkey = trust_seeds.pubkey
		  )
		ORDER BY pubkey
		LIMIT $1
		ON CONFLICT (pubkey) DO NOTHING
	`, limit)
	if err != nil {
		return 0, fmt.Errorf("insert contact list crawl seeds: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

func seedContactListAuthors(ctx context.Context, tx pgx.Tx, limit int, rescanInterval time.Duration) (int, error) {
	if _, err := tx.Exec(ctx, `
		INSERT INTO contact_list_crawl_cursor (id) VALUES (1)
		ON CONFLICT DO NOTHING
	`); err != nil {
		return 0, fmt.Errorf("ensure contact list crawl cursor: %w", err)
	}

	var cursor contactListCrawlCursor
	err := tx.QueryRow(ctx, `
		SELECT recent_activity_at, pubkey, in_null_activity, next_seed_at
		FROM contact_list_crawl_cursor
		WHERE id = 1
		FOR UPDATE
	`).Scan(&cursor.RecentActivityAt, &cursor.Pubkey, &cursor.InNullActivity, &cursor.NextSeedAt)
	if err != nil {
		return 0, fmt.Errorf("lock contact list crawl cursor: %w", err)
	}
	if cursor.NextSeedAt.After(time.Now()) {
		return 0, nil
	}

	rows, err := queryContactListSeedCandidates(ctx, tx, cursor, limit)
	if err != nil {
		return 0, err
	}
	inserted := 0
	if len(rows) > 0 {
		pubkeys := make([]string, 0, len(rows))
		for _, row := range rows {
			if row.Pubkey == "" {
				continue
			}
			pubkeys = append(pubkeys, row.Pubkey)
		}
		if len(pubkeys) > 0 {
			tag, execErr := tx.Exec(ctx, `
				INSERT INTO contact_list_crawl (pubkey, hop, next_eligible_at)
				SELECT pubkey, 0, now()
				FROM unnest($1::text[]) AS pubkey
				ON CONFLICT (pubkey) DO NOTHING
			`, pubkeys)
			if execErr != nil {
				return 0, fmt.Errorf("insert contact list crawl authors: %w", execErr)
			}
			inserted = int(tag.RowsAffected())
		}
	}
	if err := advanceContactListSeedCursor(ctx, tx, rows, limit, rescanInterval); err != nil {
		return 0, err
	}
	return inserted, nil
}

// queryContactListSeedCandidates walks profile_public_stats in
// (recent_activity_at DESC NULLS LAST, pubkey ASC) order. Driving off that
// index, the same way author metadata discovery does, avoids scanning events.
func queryContactListSeedCandidates(ctx context.Context, tx pgx.Tx, cursor contactListCrawlCursor, limit int) ([]contactListSeedCandidate, error) {
	switch cursor.seedMode() {
	case "nulls":
		return queryContactListSeedNulls(ctx, tx, cursor.Pubkey, limit)
	case "row":
		if cursor.RecentActivityAt == nil {
			return queryContactListSeedBegin(ctx, tx, limit)
		}
		rows, err := queryContactListSeedRows(ctx, tx, *cursor.RecentActivityAt, cursor.Pubkey, limit)
		if err != nil || len(rows) >= limit {
			return rows, err
		}
		nulls, err := queryContactListSeedNulls(ctx, tx, "", limit-len(rows))
		if err != nil {
			return nil, err
		}
		return append(rows, nulls...), nil
	default:
		return queryContactListSeedBegin(ctx, tx, limit)
	}
}

const contactListSeedFromStats = `
	SELECT pps.pubkey, pps.recent_activity_at
	FROM profile_public_stats pps
	WHERE pps.note_count > 0
	  AND NOT EXISTS (
		SELECT 1 FROM contact_lists_latest c WHERE c.pubkey = pps.pubkey
	  )
	  AND NOT EXISTS (
		SELECT 1 FROM contact_list_crawl q WHERE q.pubkey = pps.pubkey
	  )
`

func queryContactListSeedBegin(ctx context.Context, tx pgx.Tx, limit int) ([]contactListSeedCandidate, error) {
	rows, err := tx.Query(ctx, contactListSeedFromStats+`
		ORDER BY pps.recent_activity_at DESC NULLS LAST, pps.pubkey ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("query contact list crawl seed page: %w", err)
	}
	return scanContactListSeedCandidates(rows)
}

func queryContactListSeedRows(ctx context.Context, tx pgx.Tx, activity int64, pubkey string, limit int) ([]contactListSeedCandidate, error) {
	rows, err := tx.Query(ctx, contactListSeedFromStats+`
		  AND pps.recent_activity_at IS NOT NULL
		  AND (
			pps.recent_activity_at < $2
			OR (pps.recent_activity_at = $2 AND pps.pubkey > $3)
		  )
		ORDER BY pps.recent_activity_at DESC, pps.pubkey ASC
		LIMIT $1
	`, limit, activity, pubkey)
	if err != nil {
		return nil, fmt.Errorf("query contact list crawl seed page: %w", err)
	}
	return scanContactListSeedCandidates(rows)
}

func queryContactListSeedNulls(ctx context.Context, tx pgx.Tx, pubkey string, limit int) ([]contactListSeedCandidate, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := tx.Query(ctx, contactListSeedFromStats+`
		  AND pps.recent_activity_at IS NULL
		  AND pps.pubkey > $2
		ORDER BY pps.pubkey ASC
		LIMIT $1
	`, limit, pubkey)
	if err != nil {
		return nil, fmt.Errorf("query contact list crawl seed page: %w", err)
	}
	return scanContactListSeedCandidates(rows)
}

func scanContactListSeedCandidates(rows pgx.Rows) ([]contactListSeedCandidate, error) {
	defer rows.Close()
	out := make([]contactListSeedCandidate, 0)
	for rows.Next() {
		var row contactListSeedCandidate
		if err := rows.Scan(&row.Pubkey, &row.RecentActivityAt); err != nil {
			return nil, fmt.Errorf("scan contact list crawl seed candidate: %w", err)
		}
		row.Pubkey = strings.TrimSpace(row.Pubkey)
		if row.Pubkey == "" {
			continue
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read contact list crawl seed candidates: %w", err)
	}
	return out, nil
}

func advanceContactListSeedCursor(ctx context.Context, tx pgx.Tx, rows []contactListSeedCandidate, limit int, rescanInterval time.Duration) error {
	if len(rows) < limit {
		_, err := tx.Exec(ctx, `
			UPDATE contact_list_crawl_cursor
			SET recent_activity_at = NULL,
			    pubkey = '',
			    in_null_activity = false,
			    next_seed_at = $1,
			    updated_at = now()
			WHERE id = 1
		`, time.Now().UTC().Add(rescanInterval))
		if err != nil {
			return fmt.Errorf("pause contact list crawl seed cursor: %w", err)
		}
		return nil
	}
	last := rows[len(rows)-1]
	_, err := tx.Exec(ctx, `
		UPDATE contact_list_crawl_cursor
		SET recent_activity_at = $1,
		    pubkey = $2,
		    in_null_activity = $3,
		    next_seed_at = now(),
		    updated_at = now()
		WHERE id = 1
	`, last.RecentActivityAt, last.Pubkey, last.RecentActivityAt == nil)
	if err != nil {
		return fmt.Errorf("advance contact list crawl seed cursor: %w", err)
	}
	return nil
}

// ClaimContactListCrawl locks up to limit due rows, lowest hop first, and
// leases them until now+lease so overlapping ingestors skip them.
func (s *PostgresStore) ClaimContactListCrawl(ctx context.Context, limit int, lease time.Duration) ([]ContactListCrawlEntry, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		return nil, nil
	}
	if lease < 0 {
		lease = 0
	}
	rows, err := s.pool.Query(ctx, `
		WITH claimable AS (
			SELECT pubkey
			FROM contact_list_crawl
			WHERE next_eligible_at <= now()
			ORDER BY hop ASC, next_eligible_at ASC, pubkey ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE contact_list_crawl q
		SET next_eligible_at = now() + make_interval(secs => $2),
		    attempts = q.attempts + 1,
		    updated_at = now()
		FROM claimable c
		WHERE q.pubkey = c.pubkey
		RETURNING q.pubkey, q.hop
	`, limit, lease.Seconds())
	if err != nil {
		return nil, fmt.Errorf("claim contact list crawl rows: %w", err)
	}
	defer rows.Close()
	out := make([]ContactListCrawlEntry, 0, limit)
	for rows.Next() {
		var entry ContactListCrawlEntry
		if err := rows.Scan(&entry.Pubkey, &entry.Hop); err != nil {
			return nil, fmt.Errorf("scan claimed contact list crawl row: %w", err)
		}
		out = append(out, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read claimed contact list crawl rows: %w", err)
	}
	return out, nil
}

// MarkContactListCrawlFetched records a finished attempt. foundList is false
// when every tried relay confirmed the author has no kind-3 list.
func (s *PostgresStore) MarkContactListCrawlFetched(ctx context.Context, pubkey string, foundList bool, cooldown time.Duration) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return fmt.Errorf("pubkey is required")
	}
	if cooldown < 0 {
		cooldown = 0
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE contact_list_crawl
		SET last_fetched_at = now(),
		    found_list = $2,
		    last_error = NULL,
		    next_eligible_at = now() + make_interval(secs => $3),
		    updated_at = now()
		WHERE pubkey = $1
	`, pubkey, foundList, cooldown.Seconds())
	if err != nil {
		return fmt.Errorf("mark contact list crawl fetched: %w", err)
	}
	return nil
}

// MarkContactListCrawlFailed schedules a retry and keeps the previous found_list.
func (s *PostgresStore) MarkContactListCrawlFailed(ctx context.Context, pubkey string, retryAfter time.Duration, cause error) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return fmt.Errorf("pubkey is required")
	}
	if retryAfter < 0 {
		retryAfter = 0
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE contact_list_crawl
		SET last_error = $3,
		    next_eligible_at = now() + make_interval(secs => $2),
		    updated_at = now()
		WHERE pubkey = $1
	`, pubkey, retryAfter.Seconds(), trimContactCrawlError(cause))
	if err != nil {
		return fmt.Errorf("mark contact list crawl failed: %w", err)
	}
	return nil
}

// EnqueueContactListCrawl inserts pubkeys at hop while the number of due rows
// is under maxPending. An existing row keeps its hop and schedule, and does
// not consume the remaining room.
func (s *PostgresStore) EnqueueContactListCrawl(ctx context.Context, pubkeys []string, hop int, maxPending int) (int, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	if hop < 0 || maxPending <= 0 {
		return 0, nil
	}
	pubkeys = uniqueCrawlPubkeys(pubkeys)
	if len(pubkeys) == 0 {
		return 0, nil
	}
	var pending int
	if err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM contact_list_crawl
		WHERE next_eligible_at <= now()
	`).Scan(&pending); err != nil {
		return 0, fmt.Errorf("count due contact list crawl rows: %w", err)
	}
	room := maxPending - pending
	if room <= 0 {
		return 0, nil
	}
	// Already-queued pubkeys do not consume the cap. A list whose first
	// p-tags are duplicates would otherwise fill the room and drop the rest.
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO contact_list_crawl (pubkey, hop, next_eligible_at)
		SELECT pubkey, $2, now()
		FROM (
			SELECT t.pubkey
			FROM unnest($1::text[]) WITH ORDINALITY AS t(pubkey, ord)
			WHERE NOT EXISTS (
				SELECT 1 FROM contact_list_crawl q WHERE q.pubkey = t.pubkey
			)
			ORDER BY t.ord
			LIMIT $3
		) fresh
		ON CONFLICT (pubkey) DO NOTHING
	`, pubkeys, hop, room)
	if err != nil {
		return 0, fmt.Errorf("enqueue contact list crawl pubkeys: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// ContactListCrawlDueByHop counts rows whose fetch is due, grouped by hop.
func (s *PostgresStore) ContactListCrawlDueByHop(ctx context.Context) (map[int]int, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT hop, COUNT(*)
		FROM contact_list_crawl
		WHERE next_eligible_at <= now()
		GROUP BY hop
	`)
	if err != nil {
		return nil, fmt.Errorf("count contact list crawl depth: %w", err)
	}
	defer rows.Close()
	out := make(map[int]int)
	for rows.Next() {
		var hop int
		var count int
		if err := rows.Scan(&hop, &count); err != nil {
			return nil, fmt.Errorf("scan contact list crawl depth: %w", err)
		}
		out[hop] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read contact list crawl depth: %w", err)
	}
	return out, nil
}

// LatestKind10002Raw returns the newest stored kind-10002 event for pubkey.
// A missing row is (nil, nil); callers fall back to the configured relays.
func (s *PostgresStore) LatestKind10002Raw(ctx context.Context, pubkey string) ([]byte, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, nil
	}
	var raw string
	err := s.pool.QueryRow(ctx, `
		SELECT raw_json::text
		FROM events
		WHERE pubkey = $1 AND kind = 10002
		ORDER BY created_at DESC, id DESC
		LIMIT 1
	`, pubkey).Scan(&raw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("load latest kind 10002 for %s: %w", pubkey, err)
	}
	return []byte(raw), nil
}

func uniqueCrawlPubkeys(pubkeys []string) []string {
	if len(pubkeys) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(pubkeys))
	out := make([]string, 0, len(pubkeys))
	for _, pubkey := range pubkeys {
		pubkey = strings.TrimSpace(pubkey)
		if pubkey == "" {
			continue
		}
		if _, ok := seen[pubkey]; ok {
			continue
		}
		seen[pubkey] = struct{}{}
		out = append(out, pubkey)
	}
	return out
}

func trimContactCrawlError(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	const maxLen = 500
	if len(msg) > maxLen {
		msg = msg[:maxLen]
	}
	return msg
}
