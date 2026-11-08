package derivation

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type archiveBaseline struct {
	Metric       string  `json:"metric"`
	Window       string  `json:"window"`
	Value        float64 `json:"value"`
	Typical      float64 `json:"typical"`
	TypicalBasis string  `json:"typical_basis"`
	Ratio        float64 `json:"ratio"`
}

type archiveHashtag struct {
	Hashtag       string           `json:"hashtag"`
	EventCount    int64            `json:"event_count"`
	UniqueAuthors int64            `json:"unique_authors"`
	Baseline      *archiveBaseline `json:"baseline,omitempty"`
}

type archiveDomain struct {
	Domain        string           `json:"domain"`
	LinkCount     int64            `json:"link_count"`
	NoteCount     int64            `json:"note_count"`
	UniqueAuthors int64            `json:"unique_authors"`
	Baseline      *archiveBaseline `json:"baseline,omitempty"`
}

type archiveNetwork struct {
	NoteVolume            int64            `json:"note_volume"`
	ActiveAuthors         int64            `json:"active_authors"`
	NoteVolumeBaseline    *archiveBaseline `json:"note_volume_baseline,omitempty"`
	ActiveAuthorsBaseline *archiveBaseline `json:"active_authors_baseline,omitempty"`
}

type archiveDocument struct {
	Day        string           `json:"day"`
	Language   string           `json:"language"`
	Window     string           `json:"window"`
	ComputedAt time.Time        `json:"computed_at"`
	Hashtags   []archiveHashtag `json:"hashtags"`
	Domains    []archiveDomain  `json:"domains"`
	Network    archiveNetwork   `json:"network"`
}

// WriteDiscoveryArchive stores today's 24-hour overview once. Later ticks
// leave the row alone. The payload is the snapshots the homepage already
// serves, plus baselines when those rows exist. It does not recompute the
// rankings.
func (h *Handlers) WriteDiscoveryArchive(ctx context.Context) error {
	if h == nil || h.pool == nil {
		return nil
	}
	today := utcDay(time.Now())
	day := today.Format("2006-01-02")
	var exists bool
	err := h.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM discovery_archive
			WHERE day = $1::date AND language = '' AND window_label = '24h'
		)
	`, day).Scan(&exists)
	if err != nil {
		if isUndefinedTable(err) {
			return nil
		}
		return fmt.Errorf("check discovery archive: %w", err)
	}
	if exists {
		return nil
	}

	doc, err := h.buildDiscoveryArchive(ctx, day)
	if err != nil {
		if isUndefinedTable(err) {
			return nil
		}
		return err
	}
	payload, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("encode discovery archive: %w", err)
	}
	if _, err := h.pool.Exec(ctx, `
		INSERT INTO discovery_archive (day, language, window_label, payload, computed_at)
		VALUES ($1::date, '', '24h', $2, $3)
		ON CONFLICT (day, language, window_label) DO NOTHING
	`, day, payload, doc.ComputedAt); err != nil {
		if isUndefinedTable(err) {
			return nil
		}
		return fmt.Errorf("insert discovery archive: %w", err)
	}
	return nil
}

func (h *Handlers) buildDiscoveryArchive(ctx context.Context, day string) (archiveDocument, error) {
	doc := archiveDocument{
		Day:        day,
		Language:   "",
		Window:     "24h",
		ComputedAt: time.Now().UTC(),
		Hashtags:   []archiveHashtag{},
		Domains:    []archiveDomain{},
	}
	rows, err := h.pool.Query(ctx, `
		SELECT snapshot_label, payload, computed_at
		FROM relay_window_snapshots
		WHERE snapshot_label IN ('top_hashtags_24h', 'top_domains_24h', 'home_window_24h')
	`)
	if err != nil {
		return doc, fmt.Errorf("read archive snapshots: %w", err)
	}
	defer rows.Close()

	var hashtags []hashtagRow
	var domains []domainRow
	var sawComputedAt bool
	for rows.Next() {
		var label string
		var payload []byte
		var computedAt time.Time
		if err := rows.Scan(&label, &payload, &computedAt); err != nil {
			return doc, fmt.Errorf("scan archive snapshot: %w", err)
		}
		if !computedAt.IsZero() && (!sawComputedAt || computedAt.After(doc.ComputedAt)) {
			doc.ComputedAt = computedAt.UTC()
			sawComputedAt = true
		}
		switch label {
		case relaySnapshotLabelTopHashtags24h:
			if err := json.Unmarshal(payload, &hashtags); err != nil {
				return doc, fmt.Errorf("decode hashtag snapshot: %w", err)
			}
		case relaySnapshotLabelTopDomains24h:
			if err := json.Unmarshal(payload, &domains); err != nil {
				return doc, fmt.Errorf("decode domain snapshot: %w", err)
			}
		case relaySnapshotLabelHomeWindow24h:
			var home homeWindowSnapshotPayload
			if err := json.Unmarshal(payload, &home); err != nil {
				return doc, fmt.Errorf("decode home snapshot: %w", err)
			}
			doc.Network.NoteVolume = home.NoteVolume
			doc.Network.ActiveAuthors = home.ActiveAuthors
		}
	}
	if err := rows.Err(); err != nil {
		return doc, fmt.Errorf("read archive snapshot rows: %w", err)
	}

	if len(hashtags) > 20 {
		hashtags = hashtags[:20]
	}
	if len(domains) > 20 {
		domains = domains[:20]
	}
	hashtagKeys := make([]string, 0, len(hashtags))
	for _, row := range hashtags {
		hashtagKeys = append(hashtagKeys, row.Hashtag)
	}
	domainKeys := make([]string, 0, len(domains))
	for _, row := range domains {
		domainKeys = append(domainKeys, row.Domain)
	}
	hashtagBaselines, err := loadArchiveBaselines(ctx, h.pool, "hashtag", hashtagKeys)
	if err != nil {
		return doc, err
	}
	domainBaselines, err := loadArchiveBaselines(ctx, h.pool, "domain", domainKeys)
	if err != nil {
		return doc, err
	}
	for _, row := range hashtags {
		doc.Hashtags = append(doc.Hashtags, archiveHashtag{
			Hashtag:       row.Hashtag,
			EventCount:    row.EventCount,
			UniqueAuthors: row.UniqueAuthors,
			Baseline:      entityBaseline(hashtagBaselines[row.Hashtag], float64(row.UniqueAuthors)),
		})
	}
	for _, row := range domains {
		doc.Domains = append(doc.Domains, archiveDomain{
			Domain:        row.Domain,
			LinkCount:     row.Activity24h.LinkCount,
			NoteCount:     row.Activity24h.NoteCount,
			UniqueAuthors: row.Activity24h.UniqueAuthors,
			Baseline:      entityBaseline(domainBaselines[row.Domain], float64(row.Activity24h.UniqueAuthors)),
		})
	}
	if typical, samples, err := SameWeekdayMedian(ctx, h.pool, "note_volume"); err != nil {
		return doc, err
	} else if baseline := weekdayBaseline("note_volume", typical, samples, float64(doc.Network.NoteVolume)); baseline != nil {
		doc.Network.NoteVolumeBaseline = baseline
	}
	if typical, samples, err := SameWeekdayMedian(ctx, h.pool, "active_authors"); err != nil {
		return doc, err
	} else if baseline := weekdayBaseline("active_authors", typical, samples, float64(doc.Network.ActiveAuthors)); baseline != nil {
		doc.Network.ActiveAuthorsBaseline = baseline
	}
	return doc, nil
}

type storedBaseline struct {
	typical    float64
	sampleDays int
}

func loadArchiveBaselines(ctx context.Context, pool *pgxpool.Pool, entityType string, keys []string) (map[string]storedBaseline, error) {
	out := map[string]storedBaseline{}
	if len(keys) == 0 || pool == nil {
		return out, nil
	}
	rows, err := pool.Query(ctx, `
		SELECT entity_key, typical, sample_days
		FROM entity_baseline
		WHERE entity_type = $1 AND metric = 'unique_authors' AND entity_key = ANY($2::text[])
	`, entityType, keys)
	if err != nil {
		if isUndefinedTable(err) {
			return out, nil
		}
		return nil, fmt.Errorf("read %s baselines: %w", entityType, err)
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		var row storedBaseline
		if err := rows.Scan(&key, &row.typical, &row.sampleDays); err != nil {
			return nil, fmt.Errorf("scan %s baseline: %w", entityType, err)
		}
		out[key] = row
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read %s baseline rows: %w", entityType, err)
	}
	return out, nil
}

func entityBaseline(row storedBaseline, value float64) *archiveBaseline {
	if row.sampleDays < 7 || row.typical <= 0 || value < 0 {
		return nil
	}
	return &archiveBaseline{
		Metric:       "unique_authors",
		Window:       "24h",
		Value:        value,
		Typical:      row.typical,
		TypicalBasis: "entity_30d_median",
		Ratio:        value / row.typical,
	}
}

func weekdayBaseline(metric string, typical float64, samples int, value float64) *archiveBaseline {
	if samples < 4 || typical <= 0 || value < 0 {
		return nil
	}
	return &archiveBaseline{
		Metric:       metric,
		Window:       "24h",
		Value:        value,
		Typical:      typical,
		TypicalBasis: "same_weekday_median",
		Ratio:        value / typical,
	}
}
