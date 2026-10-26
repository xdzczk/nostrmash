package derivation

import "testing"

func TestPrimalNotificationDraftsReplyMentionAndZap(t *testing.T) {
	reply := primalNotificationDrafts(1, "author", "reply-id", "", [][]string{
		{"e", "parent-id", "", "reply"},
		{"p", "parent-author"},
		{"p", "mentioned"},
		{"e", "quoted", "", "mention"},
	}, "parent-id", "parent-author", false)
	if len(reply) != 3 {
		t.Fatalf("drafts: %+v", reply)
	}
	if reply[0].Type != 6 || reply[0].Recipient != "parent-author" {
		t.Fatalf("reply draft: %+v", reply[0])
	}
	if reply[1].Type != 7 || reply[1].Recipient != "mentioned" {
		t.Fatalf("mention draft: %+v", reply[1])
	}
	if reply[2].Type != 8 {
		t.Fatalf("post mention draft: %+v", reply[2])
	}

	nested := primalNotificationDrafts(1, "author", "reply-id", "", nil, "parent-id", "parent-author", true)
	if len(nested) != 1 || nested[0].Type != 601 {
		t.Fatalf("nested reply: %+v", nested)
	}

	zap := primalNotificationDrafts(9735, "sender", "zap-id", "", [][]string{
		{"p", "receiver"},
		{"e", "note-id"},
		{"amount", "21000"},
	}, "", "", false)
	if len(zap) != 1 || zap[0].Type != 3 || zap[0].Recipient != "receiver" {
		t.Fatalf("zap draft: %+v", zap)
	}
	if zap[0].Args["your_post"] != "note-id" || zap[0].Args["who_zapped_it"] != "sender" {
		t.Fatalf("zap args: %+v", zap[0].Args)
	}
	if reply[0].Args["who_replied_to_it"] != "author" || reply[0].Args["your_post"] != "parent-id" {
		t.Fatalf("reply args: %+v", reply[0].Args)
	}
}
