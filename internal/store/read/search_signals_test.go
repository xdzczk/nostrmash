package read

import "testing"

func TestSearchContentNeedles_StripsPhraseQuotes(t *testing.T) {
	tsQuery, likeNeedle := searchContentNeedles(`"not your keys"`)
	if tsQuery != `"not your keys"` || likeNeedle != "not your keys" {
		t.Fatalf("phrase needles = %q %q", tsQuery, likeNeedle)
	}
	tsQuery, likeNeedle = searchContentNeedles("bitcoin")
	if tsQuery != "bitcoin" || likeNeedle != "bitcoin" {
		t.Fatalf("plain needles = %q %q", tsQuery, likeNeedle)
	}
}
