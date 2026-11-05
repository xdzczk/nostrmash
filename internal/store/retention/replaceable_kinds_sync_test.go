package retention

import (
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func TestSupersededReplaceableKindsMatchMigration(t *testing.T) {
	data, err := os.ReadFile("../../../migrations/000120_group_and_list_surfaces.sql")
	if err != nil {
		t.Fatal(err)
	}
	code := stripKindSyncComments(string(data))
	m := regexp.MustCompile(`p_kind NOT IN \(([^)]*)\)`).FindStringSubmatch(code)
	if m == nil {
		t.Fatal("migration is missing p_kind NOT IN (...)")
	}
	got := make([]int, 0)
	for _, tok := range strings.Split(m[1], ",") {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		n, err := strconv.Atoi(tok)
		if err != nil {
			t.Fatalf("kind %q: %v", tok, err)
		}
		got = append(got, n)
	}
	want := slices.Clone(supersededReplaceableKinds)
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("migration kinds %v, Go kinds %v", got, want)
	}
}

func stripKindSyncComments(sql string) string {
	lines := strings.Split(sql, "\n")
	for i, line := range lines {
		if idx := strings.Index(line, "--"); idx >= 0 {
			lines[i] = line[:idx]
		}
	}
	return strings.Join(lines, "\n")
}
