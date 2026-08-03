package nostr

import "testing"

func TestParseRelationMarker(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in      string
		wantRel string
		wantOK  bool
	}{
		{"root", "root", true},
		{"reply", "reply", true},
		{"mention", "mention", true},
		{"  ROOT ", "root", true},
		{"Reply", "reply", true},
		{"", "", false},
		{"quote", "", false},
		{"unknown", "", false},
	}
	for _, tc := range cases {
		gotRel, gotOK := ParseRelationMarker(tc.in)
		if gotRel != tc.wantRel || gotOK != tc.wantOK {
			t.Fatalf("ParseRelationMarker(%q)=(%q,%t) want (%q,%t)", tc.in, gotRel, gotOK, tc.wantRel, tc.wantOK)
		}
	}
}

func TestReplyParentEventID(t *testing.T) {
	t.Parallel()
	t.Run("single unmarked e-tag", func(t *testing.T) {
		if got := ReplyParentEventID([][]string{{"e", "only_evt"}}); got != "only_evt" {
			t.Fatalf("got %q want only_evt", got)
		}
	})
	t.Run("prefers reply over root", func(t *testing.T) {
		tags := [][]string{
			{"e", "root_evt", "", "root"},
			{"e", "reply_evt", "", "reply"},
		}
		if got := ReplyParentEventID(tags); got != "reply_evt" {
			t.Fatalf("got %q want reply_evt", got)
		}
	})
	t.Run("mention-only has no parent", func(t *testing.T) {
		if got := ReplyParentEventID([][]string{{"e", "quoted", "", "mention"}}); got != "" {
			t.Fatalf("got %q want empty", got)
		}
	})
	t.Run("legacy positional last is reply parent", func(t *testing.T) {
		tags := [][]string{{"e", "first"}, {"e", "middle"}, {"e", "last"}}
		if got := ReplyParentEventID(tags); got != "last" {
			t.Fatalf("got %q want last", got)
		}
	})
}

func TestDeriveETagReferences(t *testing.T) {
	t.Parallel()
	tags := [][]string{
		{"e", "root_evt", "wss://relay.one", "root"},
		{"p", "somepubkey"},
		{"e", "reply_evt", "wss://relay.two", "reply"},
	}
	refs := DeriveETagReferences(tags)
	if len(refs) != 2 {
		t.Fatalf("expected 2 refs, got %d: %+v", len(refs), refs)
	}
	if FirstETagByRelation(refs, "root") != "root_evt" {
		t.Fatalf("root = %q", FirstETagByRelation(refs, "root"))
	}
	if FirstETagByRelation(refs, "reply") != "reply_evt" {
		t.Fatalf("reply = %q", FirstETagByRelation(refs, "reply"))
	}
	if refs[0].RelayHint != "wss://relay.one" {
		t.Fatalf("relay hint = %q", refs[0].RelayHint)
	}
}
