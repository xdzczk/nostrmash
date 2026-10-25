// Package languagelabel is the shared spelling of note language labels.
// Charts, search filters, and the search index must agree on these codes.
package languagelabel

import (
	"fmt"
	"strings"
)

const (
	// Und is a note with too little text and no thread or author language.
	Und = "und"
	// Uncertain is a note with enough text that the detector still refused.
	Uncertain = "und-uncertain"
)

// Canonical returns the chart code when raw is one of the unmarked buckets.
func Canonical(raw string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case Und:
		return Und, true
	case Uncertain:
		return Uncertain, true
	default:
		return "", false
	}
}

// ValidFilter reports whether raw is a discovery or search language parameter:
// an unmarked bucket, or a 2-8 letter code.
func ValidFilter(raw string) bool {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return false
	}
	if _, ok := Canonical(raw); ok {
		return true
	}
	if len(raw) < 2 || len(raw) > 8 {
		return false
	}
	for _, r := range raw {
		if r < 'a' || r > 'z' {
			return false
		}
	}
	return true
}

// ChartSQL labels a note for charts and the search index.
// languageExpr and sourceExpr are SQL expressions (column names or qualified columns).
func ChartSQL(languageExpr, sourceExpr string) string {
	return fmt.Sprintf(`CASE WHEN %[1]s IS NOT NULL AND btrim(%[1]s) <> '' THEN %[1]s WHEN %[2]s = 'uncertain' THEN '%[3]s' ELSE '%[4]s' END`,
		languageExpr, sourceExpr, Uncertain, Und)
}

// FilterSQL matches a language parameter against one note_discovery_stats alias.
// alias is a SQL identifier such as "nds". param is a SQL expression such as "$4::text".
func FilterSQL(alias, param string) string {
	alias = strings.TrimSpace(alias)
	if alias == "" {
		alias = "nds"
	}
	languageCol := alias + ".primary_language"
	sourceCol := alias + ".language_source"
	return fmt.Sprintf(`CASE WHEN %[1]s = '%[2]s' THEN %[3]s IS NULL AND COALESCE(%[4]s, '') <> 'uncertain' WHEN %[1]s = '%[5]s' THEN %[4]s = 'uncertain' ELSE %[3]s = %[1]s END`,
		param, Und, languageCol, sourceCol, Uncertain)
}
