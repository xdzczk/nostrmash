package meili

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

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
	client, err := ms.Connect(url, options...)
	if err != nil {
		return nil, fmt.Errorf("connect meilisearch: %w", err)
	}
	return &Client{
		enabled: true,
		service: client,
	}, nil
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
	stats, err := c.service.GetStatsWithContext(ctx)
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
func (c *Client) NeedsSync(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	if !c.Enabled() || pool == nil {
		return false, nil
	}
	type countQuery struct {
		index string
		sql   string
	}
	checks := []countQuery{
		{IndexProfiles, `SELECT count(*) FROM profiles_latest`},
		{IndexNotes, `SELECT count(*) FROM events WHERE kind IN (1, 30023)`},
	}
	const syncThreshold = 0.80
	for _, check := range checks {
		meiliStats, err := c.service.Index(check.index).GetStatsWithContext(ctx)
		if err != nil {
			return false, fmt.Errorf("get stats for index %s: %w", check.index, err)
		}
		var pgCount int64
		if err := pool.QueryRow(ctx, check.sql).Scan(&pgCount); err != nil {
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
	task, err := c.service.Index(IndexProfiles).UpdateDocumentsWithContext(ctx, docs, nil)
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
	docs = filterMeiliDocumentsIndexRows(docs)
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
