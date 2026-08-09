package retention_test

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/xdzczk/nostrmash/internal/eventtags"
)

// The disallowed-tag-name prune query and its supporting partial index
// cannot use a bind parameter for the allowlist (see the comments on
// PruneFilteredEventTagsDisallowedNames and migrations/000071); both hold a
// literal copy of internal/eventtags.AllowedTagNames instead. This test
// fails loudly if either literal list drifts from the Go source of truth,
// since a silent mismatch would make the query systematically wrong
// (deleting allowlisted tags, or leaving disallowed ones undeleted) without
// any test coverage catching it — TestPruneFilteredEventTags only exercises
// a couple of tag names, not the full allowlist boundary.
var notInListPattern = regexp.MustCompile(`(?s)NOT IN \(([^)]*)\)`)

func TestAllowedTagNamesMatchesDisallowedNameQuery(t *testing.T) {
	want := slices.Clone(eventtags.AllowedTagNames)
	slices.Sort(want)

	for _, path := range []string{
		"queries/retention.sql",
		"../../../migrations/000071_event_tags_disallowed_name_index.sql",
	} {
		got := extractQuotedList(t, path)
		if !slices.Equal(got, want) {
			t.Fatalf("%s NOT IN (...) list = %v, want %v (must match eventtags.AllowedTagNames)", path, got, want)
		}
	}
}

func extractQuotedList(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	code := stripSQLComments(string(data))
	m := notInListPattern.FindStringSubmatch(code)
	if m == nil {
		t.Fatalf("%s: no \"NOT IN (...)\" clause found", path)
	}
	var names []string
	for _, tok := range strings.Split(m[1], ",") {
		tok = strings.TrimSpace(tok)
		tok = strings.Trim(tok, "'")
		if tok == "" {
			continue
		}
		names = append(names, tok)
	}
	slices.Sort(names)
	return names
}

// stripSQLComments removes "-- ..." line comments so prose that happens to
// mention "NOT IN (...)" (as this very file's own migration/query comments
// do) doesn't get mistaken for the real predicate.
func stripSQLComments(sql string) string {
	lines := strings.Split(sql, "\n")
	for i, line := range lines {
		if idx := strings.Index(line, "--"); idx >= 0 {
			lines[i] = line[:idx]
		}
	}
	return strings.Join(lines, "\n")
}
