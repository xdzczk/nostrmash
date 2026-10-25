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

// ChartSQL labels a note for charts and the search index.
// languageExpr and sourceExpr are SQL expressions (column names or qualified columns).
func ChartSQL(languageExpr, sourceExpr string) string {
	return fmt.Sprintf(`CASE WHEN %[1]s IS NOT NULL AND btrim(%[1]s) <> '' THEN %[1]s WHEN %[2]s = 'uncertain' THEN '%[3]s' ELSE '%[4]s' END`,
		languageExpr, sourceExpr, Uncertain, Und)
}

// FilterSQL matches a search language parameter against note_discovery_stats (alias nds).
// param is a SQL expression such as "$4::text".
func FilterSQL(param string) string {
	return fmt.Sprintf(`CASE WHEN %[1]s = '%[2]s' THEN nds.primary_language IS NULL AND COALESCE(nds.language_source, '') <> 'uncertain' WHEN %[1]s = '%[3]s' THEN nds.language_source = 'uncertain' ELSE nds.primary_language = %[1]s END`,
		param, Und, Uncertain)
}
