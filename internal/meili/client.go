package meili

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	ms "github.com/meilisearch/meilisearch-go"
)

// DefaultHTTPTimeout bounds every Meilisearch HTTP call (search + sync).
// Search paths also apply a tighter context timeout; this is the hard cap.
const DefaultHTTPTimeout = 3 * time.Second

type Config struct {
	Enabled      bool
	URL          string
	MasterKey    string
	SearchAPIKey string
	// HTTPTimeout overrides DefaultHTTPTimeout when > 0.
	HTTPTimeout time.Duration
}

type IndexStats struct {
	UID          string
	NumberOfDocs int64
}

type ServiceStats struct {
	Healthy bool
	Indexes []IndexStats
}

type Client struct {
	enabled bool
	service ms.ServiceManager
}

func NewClient(cfg Config) (*Client, error) {
	if !cfg.Enabled {
		return &Client{enabled: false}, nil
	}
	url := strings.TrimSpace(cfg.URL)
	if url == "" {
		return nil, fmt.Errorf("meilisearch url is required when enabled")
	}
	apiKey := strings.TrimSpace(cfg.MasterKey)
	if searchKey := strings.TrimSpace(cfg.SearchAPIKey); searchKey != "" {
		apiKey = searchKey
	}
	timeout := DefaultHTTPTimeout
	if cfg.HTTPTimeout > 0 {
		timeout = cfg.HTTPTimeout
	}
	options := []ms.Option{
		ms.WithCustomClient(&http.Client{Timeout: timeout}),
		ms.DisableRetries(),
	}
	if apiKey != "" {
		options = append(options, ms.WithAPIKey(apiKey))
	}
	// Use New, not Connect. Connect pings GET /health immediately and
	// fails if Meilisearch is not yet listening — the Coolify Compose
	// race that produced "1x restarts" on api after every redeploy
	// (meili and api start together; meili binds :7700 ~15-20s later).
	// The client is still fully usable; callers that need the process
	// up should use EnsureIndexesReady.
	return &Client{
		enabled: true,
		service: ms.New(url, options...),
	}, nil
}

// startupReadyTimeout is how long EnsureIndexesReady waits for Meilisearch
// to accept connections after a simultaneous compose start. Observed
// production: ~19s from container start to GET /health succeeding on a
// warm 12GB index; 90s leaves headroom for a cold reopen without making
// api/worker crash-loop via restart: unless-stopped.
const startupReadyTimeout = 90 * time.Second

// EnsureIndexesReady waits until Meilisearch accepts connections, then
// creates/updates indexes. Unlike NewClient this may block; unlike a
// hard Connect() it retries instead of failing the process on the first
// connection-refused.
func (c *Client) EnsureIndexesReady(ctx context.Context) error {
	if !c.Enabled() {
		return nil
	}
	return retryUntil(ctx, startupReadyTimeout, func(callCtx context.Context) error {
		return c.EnsureIndexes(callCtx)
	})
}

func retryUntil(ctx context.Context, timeout time.Duration, fn func(context.Context) error) error {
	if timeout <= 0 {
		return fn(ctx)
	}
	deadline := time.Now().Add(timeout)
	var last error
	delay := 200 * time.Millisecond
	for {
		if err := ctx.Err(); err != nil {
			if last != nil {
				return fmt.Errorf("%w: %w", err, last)
			}
			return err
		}
		callCtx, cancel := context.WithTimeout(ctx, DefaultHTTPTimeout)
		last = fn(callCtx)
		cancel()
		if last == nil {
			return nil
		}
		if !time.Now().Add(delay).Before(deadline) {
			return fmt.Errorf("meilisearch not ready after %s: %w", timeout, last)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", ctx.Err(), last)
		case <-time.After(delay):
		}
		if delay < 2*time.Second {
			delay *= 2
		}
	}
}

func (c *Client) Enabled() bool {
	return c != nil && c.enabled && c.service != nil
}

func (c *Client) EnsureIndexes(ctx context.Context) error {
	return c.ensureIndexes(ctx)
}

func (c *Client) Health(ctx context.Context) error {
	if !c.Enabled() {
		return nil
	}
	if _, err := c.service.HealthWithContext(ctx); err != nil {
		return fmt.Errorf("meilisearch health check: %w", err)
	}
	return nil
}

func (c *Client) Stats(ctx context.Context) (ServiceStats, error) {
	if !c.Enabled() {
		return ServiceStats{}, nil
	}
	health, err := c.service.HealthWithContext(ctx)
	if err != nil {
		return ServiceStats{}, fmt.Errorf("read meilisearch health: %w", err)
	}
	stats, err := c.service.GetStatsWithContext(ctx, nil)
	if err != nil {
		return ServiceStats{}, fmt.Errorf("read meilisearch stats: %w", err)
	}
	indexes := make([]IndexStats, 0, len(stats.Indexes))
	for uid, row := range stats.Indexes {
		indexes = append(indexes, IndexStats{
			UID:          uid,
			NumberOfDocs: row.NumberOfDocuments,
		})
	}
	return ServiceStats{
		Healthy: strings.EqualFold(strings.TrimSpace(health.Status), "available"),
		Indexes: indexes,
	}, nil
}

// NeedsSync compares document counts in Meilisearch against PostgreSQL and
// returns true when any index is significantly behind (below 80% of PG count)
// or completely empty. This catches both fresh indexes and interrupted syncs.
//
// The notes comparison MUST use the same indexedNotesMaxAge window that
// streamNotes actually syncs (the notes index deliberately only ever holds
// the last 14 days, per index_trim.go), not the all-time events count.
// Comparing against all-time count is a bug that previously made this ratio
// permanently sit far below the threshold once the corpus grew beyond a few
// weeks (observed in production: ~30% with a 2.3M-row all-time count vs a
// 700K-row 14-day window), so NeedsSync returned true on every single
// api/worker restart forever, triggering a full nuclear resync of the notes
// window on every deploy regardless of whether Meili was actually caught up.
func (c *Client) NeedsSync(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	if !c.Enabled() || pool == nil {
		return false, nil
	}
	type countQuery struct {
		index string
		sql   string
	}
	checks := []countQuery{
		{IndexProfiles, `SELECT count(*) FROM profiles_latest p WHERE ` + namedProfileSQL},
		{IndexNotes, `SELECT count(*) FROM events e WHERE ` + indexedNotesWhere + ` AND e.created_at >= $1::bigint`},
	}
	const syncThreshold = 0.80
	minNotesCreatedAt := indexedNotesMinCreatedAt(time.Now())
	for _, check := range checks {
		meiliStats, err := c.service.Index(check.index).GetStatsWithContext(ctx, nil)
		if err != nil {
			return false, fmt.Errorf("get stats for index %s: %w", check.index, err)
		}
		var (
			pgCount int64
			row     pgx.Row
		)
		if check.index == IndexNotes {
			row = pool.QueryRow(ctx, check.sql, minNotesCreatedAt)
		} else {
			row = pool.QueryRow(ctx, check.sql)
		}
		if err := row.Scan(&pgCount); err != nil {
			return false, fmt.Errorf("count rows for %s: %w", check.index, err)
		}
		if pgCount == 0 {
			continue
		}
		ratio := float64(meiliStats.NumberOfDocuments) / float64(pgCount)
		if ratio < syncThreshold {
			return true, nil
		}
	}
	return false, nil
}

func (c *Client) waitForTask(ctx context.Context, taskUID int64) error {
	if !c.Enabled() {
		return nil
	}
	task, err := c.service.WaitForTaskWithContext(ctx, taskUID, 250*time.Millisecond)
	if err != nil {
		return err
	}
	if task.Status != ms.TaskStatusSucceeded {
		if task.Error.Code != "" {
			return fmt.Errorf("task %d ended with status=%s: [%s] %s", taskUID, task.Status, task.Error.Code, task.Error.Message)
		}
		return fmt.Errorf("task %d ended with status=%s", taskUID, task.Status)
	}
	return nil
}

func (c *Client) UpsertNotes(ctx context.Context, docs []NoteDocument) error {
	taskUID, err := c.enqueueNotes(ctx, docs)
	if err != nil || taskUID == 0 {
		return err
	}
	return c.waitForTask(ctx, taskUID)
}

func (c *Client) UpsertProfiles(ctx context.Context, docs []ProfileDocument) error {
	taskUID, err := c.enqueueProfiles(ctx, docs)
	if err != nil || taskUID == 0 {
		return err
	}
	return c.waitForTask(ctx, taskUID)
}

func (c *Client) UpsertDocuments(ctx context.Context, docs []SearchDocument) error {
	taskUID, err := c.enqueueDocuments(ctx, docs)
	if err != nil || taskUID == 0 {
		return err
	}
	return c.waitForTask(ctx, taskUID)
}

func (c *Client) enqueueNotes(ctx context.Context, docs []NoteDocument) (int64, error) {
	if !c.Enabled() || len(docs) == 0 {
		return 0, nil
	}
	for i := range docs {
		trimNoteDocument(&docs[i])
	}
	task, err := c.service.Index(IndexNotes).UpdateDocumentsWithContext(ctx, docs, nil)
	if err != nil {
		return 0, fmt.Errorf("upsert notes in meilisearch: %w", err)
	}
	return task.TaskUID, nil
}

func (c *Client) enqueueProfiles(ctx context.Context, docs []ProfileDocument) (int64, error) {
	if !c.Enabled() || len(docs) == 0 {
		return 0, nil
	}
	for i := range docs {
		trimProfileDocument(&docs[i])
	}
	// Add replaces the document. Update would merge and leave a stale
	// profile_json key on profiles indexed before that field was dropped.
	task, err := c.service.Index(IndexProfiles).AddDocumentsWithContext(ctx, docs, nil)
	if err != nil {
		return 0, fmt.Errorf("upsert profiles in meilisearch: %w", err)
	}
	return task.TaskUID, nil
}

func (c *Client) enqueueDocuments(ctx context.Context, docs []SearchDocument) (int64, error) {
	if !c.Enabled() || len(docs) == 0 {
		return 0, nil
	}
	// Notes and profiles already live in dedicated Meili indexes; the global
	// search path only consumes hashtag/identity/relay hits from documents.
	// Rare hashtags are omitted here. Deleting each one as it is seen would
	// add a task to every sweeper cycle; PurgeRareHashtags removes the
	// documents already stored under the cutoff in one filter task.
	docs, _ = splitMeiliDocuments(docs)
	if len(docs) == 0 {
		return 0, nil
	}
	for i := range docs {
		trimSearchDocument(&docs[i])
	}
	task, err := c.service.Index(IndexDocuments).UpdateDocumentsWithContext(ctx, docs, nil)
	if err != nil {
		return 0, fmt.Errorf("upsert documents in meilisearch: %w", err)
	}
	return task.TaskUID, nil
}

// PurgeAgedNotes deletes note documents older than the indexed-notes age
// horizon (indexedNotesMaxAge). FullSync already refuses to index notes past
// that age, but incremental syncs index every fresh note and nothing removed
// them as they aged, so the live notes index grew without bound between full
// rebuilds — at production scale that standing document count is what kept
// Meilisearch task commits expensive (each commit cost scales with index
// size) and the instance pinned at 100% CPU. One delete-by-filter task per
// purge tick keeps the index at its designed working set.
//
// Returns the number of documents the delete task reports removing.
func (c *Client) PurgeAgedNotes(ctx context.Context) (int64, error) {
	cutoff := indexedNotesMinCreatedAt(time.Now())
	return c.deleteByFilter(ctx, IndexNotes, fmt.Sprintf("created_at < %d", cutoff), "aged notes")
}

// PurgeRareHashtags removes one-off hashtag documents already in the index.
// Incremental sync stops writing them; this deletes the ones written before
// that cutoff. popularity is the event count stored on the document.
func (c *Client) PurgeRareHashtags(ctx context.Context) (int64, error) {
	filter := fmt.Sprintf(`entity_type = "hashtag" AND popularity < %d`, minIndexedHashtagPopularity)
	return c.deleteByFilter(ctx, IndexDocuments, filter, "rare hashtags")
}

// PurgeNamelessProfiles deletes profiles index documents whose Postgres row
// has neither a name nor a display name. Incremental sync deletes a profile
// when its name is cleared; this walks the rows already in the index.
func (c *Client) PurgeNamelessProfiles(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	if !c.Enabled() || pool == nil {
		return 0, nil
	}
	const page = 500
	var (
		deleted int64
		cursor  string
	)
	for {
		rows, err := pool.Query(ctx, `
			SELECT pubkey
			FROM profiles_latest
			WHERE pubkey > $1
			  AND btrim(coalesce(name, '')) = ''
			  AND btrim(coalesce(display_name, '')) = ''
			ORDER BY pubkey
			LIMIT $2
		`, cursor, page)
		if err != nil {
			return deleted, fmt.Errorf("list nameless profiles: %w", err)
		}
		ids := make([]string, 0, page)
		for rows.Next() {
			var pubkey string
			if err := rows.Scan(&pubkey); err != nil {
				rows.Close()
				return deleted, fmt.Errorf("scan nameless profile: %w", err)
			}
			ids = append(ids, pubkey)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return deleted, fmt.Errorf("read nameless profiles: %w", err)
		}
		rows.Close()
		if len(ids) == 0 {
			return deleted, nil
		}
		n, err := c.deleteDocumentsCounted(ctx, IndexProfiles, ids)
		if err != nil {
			return deleted, err
		}
		deleted += n
		cursor = ids[len(ids)-1]
		if len(ids) < page {
			return deleted, nil
		}
	}
}

func (c *Client) deleteByFilter(ctx context.Context, indexUID, filter, what string) (int64, error) {
	if !c.Enabled() {
		return 0, nil
	}
	taskInfo, err := c.service.Index(indexUID).DeleteDocumentsByFilterWithContext(ctx, filter, nil)
	if err != nil {
		return 0, fmt.Errorf("delete %s from meilisearch: %w", what, err)
	}
	task, err := c.service.WaitForTaskWithContext(ctx, taskInfo.TaskUID, 250*time.Millisecond)
	if err != nil {
		return 0, fmt.Errorf("wait for %s delete task: %w", what, err)
	}
	if task.Status != ms.TaskStatusSucceeded {
		if task.Error.Code != "" {
			return 0, fmt.Errorf("%s delete task %d ended with status=%s: [%s] %s", what, taskInfo.TaskUID, task.Status, task.Error.Code, task.Error.Message)
		}
		return 0, fmt.Errorf("%s delete task %d ended with status=%s", what, taskInfo.TaskUID, task.Status)
	}
	return task.Details.DeletedDocuments, nil
}

func (c *Client) deleteDocuments(ctx context.Context, indexUID string, ids []string) error {
	_, err := c.deleteDocumentsCounted(ctx, indexUID, ids)
	return err
}

func (c *Client) deleteDocumentsCounted(ctx context.Context, indexUID string, ids []string) (int64, error) {
	if !c.Enabled() || len(ids) == 0 {
		return 0, nil
	}
	taskInfo, err := c.service.Index(indexUID).DeleteDocumentsWithContext(ctx, ids, nil)
	if err != nil {
		return 0, fmt.Errorf("delete %s documents from meilisearch: %w", indexUID, err)
	}
	task, err := c.service.WaitForTaskWithContext(ctx, taskInfo.TaskUID, 250*time.Millisecond)
	if err != nil {
		return 0, fmt.Errorf("wait for %s document delete task: %w", indexUID, err)
	}
	if task.Status != ms.TaskStatusSucceeded {
		if task.Error.Code != "" {
			return 0, fmt.Errorf("%s document delete task %d ended with status=%s: [%s] %s", indexUID, taskInfo.TaskUID, task.Status, task.Error.Code, task.Error.Message)
		}
		return 0, fmt.Errorf("%s document delete task %d ended with status=%s", indexUID, taskInfo.TaskUID, task.Status)
	}
	return task.Details.DeletedDocuments, nil
}

// ResetIndexes deletes the NostrMash Meilisearch indexes so the next
// EnsureIndexes + FullSync rebuilds them with the current document shape.
// Meilisearch only reclaims disk after index deletion/recreation.
func (c *Client) ResetIndexes(ctx context.Context) error {
	if !c.Enabled() {
		return nil
	}
	for _, uid := range []string{IndexNotes, IndexProfiles, IndexDocuments} {
		task, err := c.service.DeleteIndexWithContext(ctx, uid)
		if err != nil {
			// Missing index is fine during first-time setup / repeated resets.
			continue
		}
		if task == nil {
			continue
		}
		if waitErr := c.waitForTask(ctx, task.TaskUID); waitErr != nil {
			// Concurrent cancels / already-deleted indexes surface as task
			// failures rather than DeleteIndex errors.
			if isIgnorableIndexDeleteFailure(waitErr) {
				continue
			}
			return fmt.Errorf("wait delete index %s: %w", uid, waitErr)
		}
	}
	return c.EnsureIndexes(ctx)
}

func isIgnorableIndexDeleteFailure(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "index_not_found") ||
		strings.Contains(msg, "index not found") ||
		strings.Contains(msg, "status=canceled")
}
