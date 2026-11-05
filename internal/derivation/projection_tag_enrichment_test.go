package derivation

import "testing"

func TestParseSelfLabelsAndIdentities(t *testing.T) {
	labels := parseSelfLabels([][]string{
		{"L", "ISO-639-1"},
		{"l", "en", "ISO-639-1"},
		{"l", "topic"},
	})
	if len(labels) != 2 || labels[0].Label != "en" || labels[0].Namespace != "ISO-639-1" {
		t.Fatalf("labels: %+v", labels)
	}
	if labels[1].Namespace != "ISO-639-1" {
		t.Fatalf("single namespace should fill the bare label, got %+v", labels[1])
	}
	identities := parseExternalIdentities([][]string{
		{"i", "github:nostrmash", "proof"},
		{"i", "github:other"},
		{"i", "not-an-identity"},
	})
	if len(identities) != 1 || identities[0].Platform != "github" || identities[0].Identity != "nostrmash" || identities[0].Proof != "proof" {
		t.Fatalf("identities: %+v", identities)
	}
	words, mentions, hashtags, length, reading := contentShape(30023, "one two three", [][]string{{"p", "aa"}, {"t", "nostr"}})
	if words != 3 || mentions != 1 || hashtags != 1 || length != 13 || reading < 1 {
		t.Fatalf("shape words=%d mentions=%d hashtags=%d length=%d reading=%d", words, mentions, hashtags, length, reading)
	}
	hints := relayHints([][]string{{"e", "note", "wss://relay.example.com"}})
	if len(hints) != 1 || hints[0] == "" {
		t.Fatalf("hints: %+v", hints)
	}
}
