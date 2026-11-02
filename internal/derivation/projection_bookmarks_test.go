package derivation

import "testing"

func TestBookmarkTargetsFromTags(t *testing.T) {
	note := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	author := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	targets := bookmarkTargetsFromTags([][]string{
		{"e", note},
		{"e", note},
		{"e", "not-an-event"},
		{"t", "nostr"},
		{"a", "30023:" + author + ":Essay"},
		{"a", "1:" + author + ":nope"},
		{"a", "30023:" + author + ":Essay"},
		{"p", author},
	})
	if len(targets) != 2 {
		t.Fatalf("targets = %#v", targets)
	}
	if targets[0].eventID != note || targets[0].address != "" {
		t.Fatalf("note target = %#v", targets[0])
	}
	if targets[1].address != "30023:"+author+":Essay" || targets[1].dTag != "Essay" {
		t.Fatalf("article target = %#v", targets[1])
	}
}

func TestParseBookmarkAddressRejectsShortPubkey(t *testing.T) {
	if _, _, _, ok := parseBookmarkAddress("30023:abcd:essay"); ok {
		t.Fatal("expected short pubkey to be rejected")
	}
}
