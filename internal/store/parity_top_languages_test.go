package store

import (
	"context"
	"encoding/json"
	"regexp"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/readmodel"
)

// Counts are one note per unit so the chart order is the count order.
// Junk codes and unmarked buckets outrank several real languages, which is
// the shape that used to hide es, fr, and ru behind an 8-row cap.
var topLanguageFixture = []struct {
	language string
	count    int
}{
	{"english", 22},
	{"zh-cn", 21},
	{"e", 20},
	{"und", 19},
	{"en", 18},
	{"ja", 17},
	{"pt", 16},
	{"de", 15},
	{"zh", 14},
	{"fr", 13},
	{"es", 12},
	{"ru", 11},
	{"it", 10},
	{"nl", 9},
	{"fil", 8},
	{"pl", 7},
	{"ko", 6},
	{"ar", 5},
	{"sv", 4},
	{"und-uncertain", 3},
	{"tr", 2},
}

func TestGetPublicDiscoveryNetworkStats_TopLanguagesIncludeFifteenRealCodes(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")
	pgStore := NewPostgresStore(pool)

	createdAt := time.Now().UTC().Unix()
	if _, err := pool.Exec(ctx, `
		WITH spec(language, n) AS (
			VALUES
				('english', 22),
				('zh-cn', 21),
				('e', 20),
				('und', 19),
				('en', 18),
				('ja', 17),
				('pt', 16),
				('de', 15),
				('zh', 14),
				('fr', 13),
				('es', 12),
				('ru', 11),
				('it', 10),
				('nl', 9),
				('fil', 8),
				('pl', 7),
				('ko', 6),
				('ar', 5),
				('sv', 4),
				('und-uncertain', 3),
				('tr', 2)
		),
		inserted AS (
			INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
			SELECT
				'tl/' || spec.language || '/' || gs.i::text,
				'author_tl',
				$1,
				1,
				'sig',
				'',
				'{}'::jsonb
			FROM spec
			CROSS JOIN LATERAL generate_series(1, spec.n) AS gs(i)
			RETURNING id
		)
		INSERT INTO note_discovery_stats (
			event_id, author_pubkey, created_at, derivation_version, primary_language, language_source
		)
		SELECT
			id,
			'author_tl',
			$1,
			1,
			CASE
				WHEN split_part(id, '/', 2) IN ('und', 'und-uncertain') THEN NULL
				ELSE split_part(id, '/', 2)
			END,
			CASE split_part(id, '/', 2)
				WHEN 'und' THEN 'short'
				WHEN 'und-uncertain' THEN 'uncertain'
				ELSE 'content'
			END
		FROM inserted
	`, createdAt); err != nil {
		t.Fatalf("seed indexed notes: %v", err)
	}

	if err := derivation.NewHandlers(pool).RefreshRelayWindowSnapshots(ctx); err != nil {
		t.Fatalf("RefreshRelayWindowSnapshots: %v", err)
	}

	stats, err := pgStore.GetPublicDiscoveryNetworkStats(ctx, 5, "")
	if err != nil {
		t.Fatalf("GetPublicDiscoveryNetworkStats: %v", err)
	}
	noteCount := 0
	for _, spec := range topLanguageFixture {
		noteCount += spec.count
	}
	if stats.NoteVolume.Last24h != int64(noteCount) || stats.NoteVolume.Last7d != int64(noteCount) || stats.NoteVolume.Last30d != int64(noteCount) {
		t.Fatalf("note volume = 24h:%d 7d:%d 30d:%d, want %d in every window", stats.NoteVolume.Last24h, stats.NoteVolume.Last7d, stats.NoteVolume.Last30d, noteCount)
	}

	windows := map[string][]langCount{
		"top_languages_24h": toLangCounts(stats.TopLanguages24h),
		"top_languages_7d":  toLangCounts(stats.TopLanguages7d),
		"top_languages_30d": toLangCounts(stats.TopLanguages30d),
	}
	for label, got := range windows {
		assertTopLanguageChart(t, label, got)
		snap := readLanguageSnapshot(t, ctx, pool, label)
		if len(snap) != len(got) {
			t.Fatalf("%s api rows = %d, snapshot rows = %d", label, len(got), len(snap))
		}
		for i := range snap {
			if snap[i] != got[i] {
				t.Fatalf("%s row %d api %#v snapshot %#v", label, i, got[i], snap[i])
			}
		}
	}
	if !languageChartsEqual(windows["top_languages_24h"], windows["top_languages_7d"]) || !languageChartsEqual(windows["top_languages_24h"], windows["top_languages_30d"]) {
		t.Fatalf("windows diverged: 24h %#v 7d %#v 30d %#v", windows["top_languages_24h"], windows["top_languages_7d"], windows["top_languages_30d"])
	}
}

type langCount struct {
	Language string `json:"language"`
	Count    int64  `json:"count"`
}

func toLangCounts(rows []readmodel.LanguageSummary) []langCount {
	out := make([]langCount, 0, len(rows))
	for _, row := range rows {
		out = append(out, langCount{Language: row.Language, Count: row.Count})
	}
	return out
}

func assertTopLanguageChart(t *testing.T, label string, rows []langCount) {
	t.Helper()
	realCode := regexp.MustCompile(`^[a-z]{2,3}$`)
	real := 0
	seen := map[string]int64{}
	for _, row := range rows {
		seen[row.Language] = row.Count
		if row.Language == "und" || row.Language == "und-uncertain" {
			continue
		}
		if realCode.MatchString(row.Language) {
			real++
		}
	}
	if real < 15 {
		t.Fatalf("%s real language codes = %d, want at least 15: %#v", label, real, rows)
	}
	for _, code := range []string{"und", "und-uncertain", "es", "fr", "ru", "sv", "fil"} {
		if _, ok := seen[code]; !ok {
			t.Fatalf("%s missing %s: %#v", label, code, rows)
		}
	}
	for _, code := range []string{"tr", "english", "zh-cn", "e"} {
		if _, ok := seen[code]; ok {
			t.Fatalf("%s should omit %s: %#v", label, code, rows)
		}
	}
	if seen["en"] != 18 || seen["ru"] != 11 || seen["sv"] != 4 || seen["und"] != 19 || seen["und-uncertain"] != 3 {
		t.Fatalf("%s counts = %#v", label, seen)
	}
}

func languageChartsEqual(a, b []langCount) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func readLanguageSnapshot(t *testing.T, ctx context.Context, pool *pgxpool.Pool, label string) []langCount {
	t.Helper()
	var raw []byte
	if err := pool.QueryRow(ctx, `SELECT payload FROM relay_window_snapshots WHERE snapshot_label = $1`, label).Scan(&raw); err != nil {
		t.Fatalf("read snapshot %s: %v", label, err)
	}
	var rows []langCount
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatalf("decode snapshot %s: %v", label, err)
	}
	return rows
}
