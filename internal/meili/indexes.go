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
				// byAttribute stores proximity per field instead of per word
				// pair. Production's notes/profiles/documents indexes were
				// ~16GB on disk with ~1GB of raw documents; the gap is this
				// proximity database. Search still ranks within a field.
				ProximityPrecision: ms.ByAttribute,
				// Hash-shaped fields (64-hex id/pubkey) must not be searchable:
				// every new note adds brand-new unique "words" to the index, and
				// Meilisearch rebuilds its word/prefix databases on each task, so
				// production saw a one-document update to `notes` take ~27s at
				// ~300% CPU. Hex identifiers are resolved via the `pubkey`
				// filter (SearchNotes) and the query layer's direct event lookup.
				// `id` is the primary key and is returned on every hit; it is
				// not filterable, because a filterable 64-hex id is a second
				// high-cardinality index search never queries.
				SearchableAttributes: []string{
					"content",
				},
				FilterableAttributes: []string{
					"pubkey",
					"created_at",
					"language",
				},
				SortableAttributes: []string{
					"created_at",
				},
				// event_id is the Nostr id when id is the live-activity address
				// key. attributesToRetrieve can only return displayed fields.
				DisplayedAttributes: []string{
					"id",
					"event_id",
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
				RankingRules:       []string{"words", "typo", "proximity", "attribute", "sort", "exactness"},
				ProximityPrecision: ms.ByAttribute,
				// `pubkey` stays filterable only; see the notes index comment.
				SearchableAttributes: []string{
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
				RankingRules:       []string{"words", "typo", "proximity", "attribute", "sort", "exactness"},
				ProximityPrecision: ms.ByAttribute,
				// `entity_id` stays filterable and displayed, not searchable.
				// Hashtag and relay ids repeat `title`, and a searchable id
				// puts every distinct tag or URL into the word dictionary.
				SearchableAttributes: []string{
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
