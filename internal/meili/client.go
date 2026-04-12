package meili

import (
	"context"
	"fmt"
	"strings"
	"time"

	ms "github.com/meilisearch/meilisearch-go"
)

type Config struct {
	Enabled      bool
	URL          string
	MasterKey    string
	SearchAPIKey string
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
	options := make([]ms.Option, 0, 1)
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

func (c *Client) waitForTask(ctx context.Context, taskUID int64) error {
	if !c.Enabled() {
		return nil
	}
	task, err := c.service.WaitForTaskWithContext(ctx, taskUID, 250*time.Millisecond)
	if err != nil {
		return err
	}
	if task.Status != ms.TaskStatusSucceeded {
		return fmt.Errorf("task %d ended with status=%s", taskUID, task.Status)
	}
	return nil
}

func (c *Client) UpsertNotes(ctx context.Context, docs []NoteDocument) error {
	if !c.Enabled() || len(docs) == 0 {
		return nil
	}
	task, err := c.service.Index(IndexNotes).UpdateDocumentsWithContext(ctx, docs, nil)
	if err != nil {
		return fmt.Errorf("upsert notes in meilisearch: %w", err)
	}
	return c.waitForTask(ctx, task.TaskUID)
}

func (c *Client) UpsertProfiles(ctx context.Context, docs []ProfileDocument) error {
	if !c.Enabled() || len(docs) == 0 {
		return nil
	}
	task, err := c.service.Index(IndexProfiles).UpdateDocumentsWithContext(ctx, docs, nil)
	if err != nil {
		return fmt.Errorf("upsert profiles in meilisearch: %w", err)
	}
	return c.waitForTask(ctx, task.TaskUID)
}

func (c *Client) UpsertDocuments(ctx context.Context, docs []SearchDocument) error {
	if !c.Enabled() || len(docs) == 0 {
		return nil
	}
	task, err := c.service.Index(IndexDocuments).UpdateDocumentsWithContext(ctx, docs, nil)
	if err != nil {
		return fmt.Errorf("upsert documents in meilisearch: %w", err)
	}
	return c.waitForTask(ctx, task.TaskUID)
}
