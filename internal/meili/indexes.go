package meili

import (
	"context"
	"fmt"

	ms "github.com/meilisearch/meilisearch-go"
)

const (
	IndexNotes     = "notes"
	IndexProfiles  = "profiles"
	IndexDocuments = "documents"
)

type indexSpec struct {
	UID        string
	PrimaryKey string
	Settings   ms.Settings
}

func meiliIndexSpecs() []indexSpec {
	return []indexSpec{
		{
			UID:        IndexNotes,
			PrimaryKey: "id",
			Settings: ms.Settings{
				RankingRules: []string{"words", "typo", "proximity", "attribute", "sort", "exactness"},
				SearchableAttributes: []string{
					"id",
					"content",
					"pubkey",
				},
				FilterableAttributes: []string{
					"id",
					"pubkey",
					"created_at",
					"language",
				},
				SortableAttributes: []string{
					"created_at",
				},
				DisplayedAttributes: []string{
					"id",
					"pubkey",
					"created_at",
					"language",
				},
			},
		},
		{
			UID:        IndexProfiles,
			PrimaryKey: "pubkey",
			Settings: ms.Settings{
				RankingRules: []string{"words", "typo", "proximity", "attribute", "sort", "exactness"},
				SearchableAttributes: []string{
					"pubkey",
					"name",
					"display_name",
					"about",
					"nip05",
				},
				FilterableAttributes: []string{
					"pubkey",
					"nip05",
					"popularity",
				},
				SortableAttributes: []string{
					"popularity",
					"metadata_created_at",
				},
				DisplayedAttributes: []string{
					"pubkey",
					"metadata_event_id",
					"metadata_created_at",
					"name",
					"display_name",
					"about",
					"nip05",
					"popularity",
				},
			},
		},
		{
			UID:        IndexDocuments,
			PrimaryKey: "id",
			Settings: ms.Settings{
				RankingRules: []string{"words", "typo", "proximity", "attribute", "sort", "exactness"},
				SearchableAttributes: []string{
					"entity_id",
					"title",
					"body",
					"aliases",
					"identity_tokens",
				},
				FilterableAttributes: []string{
					"entity_type",
					"entity_id",
					"popularity",
					"freshness",
				},
				SortableAttributes: []string{
					"popularity",
					"freshness",
				},
				DisplayedAttributes: []string{
					"id",
					"entity_type",
					"entity_id",
					"title",
					"aliases",
					"identity_tokens",
					"freshness",
					"popularity",
					"trust_score",
				},
			},
		},
	}
}

func (c *Client) ensureIndexes(ctx context.Context) error {
	if !c.Enabled() {
		return nil
	}
	for _, spec := range meiliIndexSpecs() {
		if err := c.ensureIndex(ctx, spec); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) ensureIndex(ctx context.Context, spec indexSpec) error {
	if _, err := c.service.GetIndexWithContext(ctx, spec.UID); err != nil {
		task, createErr := c.service.CreateIndexWithContext(ctx, &ms.IndexConfig{
			Uid:        spec.UID,
			PrimaryKey: spec.PrimaryKey,
		})
		if createErr != nil {
			return fmt.Errorf("create meilisearch index %s: %w", spec.UID, createErr)
		}
		if err := c.waitForTask(ctx, task.TaskUID); err != nil {
			return fmt.Errorf("wait index create task (%s): %w", spec.UID, err)
		}
	}
	task, err := c.service.Index(spec.UID).UpdateSettingsWithContext(ctx, &spec.Settings)
	if err != nil {
		return fmt.Errorf("update meilisearch index settings (%s): %w", spec.UID, err)
	}
	if err := c.waitForTask(ctx, task.TaskUID); err != nil {
		return fmt.Errorf("wait settings update task (%s): %w", spec.UID, err)
	}
	return nil
}
