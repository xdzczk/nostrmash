package derivation

import (
	"reflect"
	"testing"

	"github.com/xdzczk/nostrmash/internal/languagelabel"
)

func TestSelectTopLanguagesKeepsFifteenRealCodes(t *testing.T) {
	t.Parallel()
	reals := []string{
		"en", "ja", "pt", "de", "zh", "fr", "es", "ru", "it", "nl", "fil", "pl", "ko", "ar", "sv", "tr",
	}
	rows := []languageRow{
		{Language: "english", Count: 500},
		{Language: "zh-cn", Count: 400},
		{Language: "e", Count: 300},
		{Language: languagelabel.Und, Count: 200},
		{Language: languagelabel.Uncertain, Count: 3},
	}
	for i, code := range reals {
		rows = append(rows, languageRow{Language: code, Count: int64(100 - i)})
	}

	got := selectTopLanguages(rows, 0)
	want := []languageRow{
		{Language: languagelabel.Und, Count: 200},
		{Language: "en", Count: 100},
		{Language: "ja", Count: 99},
		{Language: "pt", Count: 98},
		{Language: "de", Count: 97},
		{Language: "zh", Count: 96},
		{Language: "fr", Count: 95},
		{Language: "es", Count: 94},
		{Language: "ru", Count: 93},
		{Language: "it", Count: 92},
		{Language: "nl", Count: 91},
		{Language: "fil", Count: 90},
		{Language: "pl", Count: 89},
		{Language: "ko", Count: 88},
		{Language: "ar", Count: 87},
		{Language: "sv", Count: 86},
		{Language: languagelabel.Uncertain, Count: 3},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("selectTopLanguages = %#v, want %#v", got, want)
	}
	if real := countRealChartLanguages(got); real != topLanguagesRealLimit {
		t.Fatalf("real language codes = %d, want %d", real, topLanguagesRealLimit)
	}
}

func TestSelectTopLanguagesTieBreaksByLanguage(t *testing.T) {
	t.Parallel()
	rows := make([]languageRow, 0, 17)
	for i, code := range []string{"en", "ja", "pt", "de", "zh", "fr", "es", "ru", "it", "nl", "fil", "pl", "ko", "ar"} {
		rows = append(rows, languageRow{Language: code, Count: int64(100 - i)})
	}
	rows = append(rows,
		languageRow{Language: "tr", Count: 10},
		languageRow{Language: "sv", Count: 10},
		languageRow{Language: languagelabel.Und, Count: 1},
		languageRow{Language: languagelabel.Uncertain, Count: 1},
	)
	got := selectTopLanguages(rows, topLanguagesRealLimit)
	if !hasLanguage(got, "sv") || hasLanguage(got, "tr") {
		t.Fatalf("tie at the cutoff should keep sv and drop tr, got %#v", got)
	}
	if !hasLanguage(got, languagelabel.Und) || !hasLanguage(got, languagelabel.Uncertain) {
		t.Fatalf("low-count buckets should stay on the chart, got %#v", got)
	}
}

func TestSelectTopLanguagesReturnsFewerWhenTheWindowIsSmall(t *testing.T) {
	t.Parallel()
	got := selectTopLanguages([]languageRow{
		{Language: "en", Count: 4},
		{Language: languagelabel.Und, Count: 9},
		{Language: "es", Count: 2},
	}, topLanguagesRealLimit)
	want := []languageRow{
		{Language: languagelabel.Und, Count: 9},
		{Language: "en", Count: 4},
		{Language: "es", Count: 2},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("selectTopLanguages = %#v, want %#v", got, want)
	}
}

func countRealChartLanguages(rows []languageRow) int {
	n := 0
	for _, row := range rows {
		if row.Language == languagelabel.Und || row.Language == languagelabel.Uncertain {
			continue
		}
		if realChartLanguageCode.MatchString(row.Language) {
			n++
		}
	}
	return n
}

func hasLanguage(rows []languageRow, language string) bool {
	for _, row := range rows {
		if row.Language == language {
			return true
		}
	}
	return false
}
