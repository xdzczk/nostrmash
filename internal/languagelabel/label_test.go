package languagelabel

import (
	"strings"
	"testing"
)

func TestValidFilter(t *testing.T) {
	t.Parallel()
	ok := []string{"en", "DE", "ja", "und", "UND-UNCERTAIN", "english"}
	for _, raw := range ok {
		if !ValidFilter(raw) {
			t.Fatalf("ValidFilter(%q) = false", raw)
		}
	}
	bad := []string{"", "e", "de-DE", "123", "too_longx"}
	for _, raw := range bad {
		if ValidFilter(raw) {
			t.Fatalf("ValidFilter(%q) = true", raw)
		}
	}
}

func TestFilterSQLUsesAlias(t *testing.T) {
	t.Parallel()
	got := FilterSQL("s", "$4::text")
	for _, part := range []string{"s.primary_language", "s.language_source", "$4::text", Und, Uncertain} {
		if !strings.Contains(got, part) {
			t.Fatalf("FilterSQL = %s, missing %s", got, part)
		}
	}
}
