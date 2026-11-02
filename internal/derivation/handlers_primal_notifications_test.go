package derivation

import (
	"strings"
	"testing"
)

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
	if notificationMentionedEventID(reply[2].Args) != "quoted" {
		t.Fatalf("quote mention target = %q", notificationMentionedEventID(reply[2].Args))
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
	payer := strings.Repeat("ab", 32)
	named := primalNotificationDrafts(9735, "wallet", "zap-id", "", [][]string{
		{"p", "receiver"},
		{"e", "note-id"},
		{"description", `{"pubkey":"` + strings.ToUpper(payer) + `","kind":9734,"content":"nice"}`},
	}, "", "", false)
	if len(named) != 1 || named[0].Args["who_zapped_it"] != payer {
		t.Fatalf("named zap: %+v", named)
	}
	self := primalNotificationDrafts(9735, "wallet", "zap-id", "", [][]string{
		{"p", payer},
		{"description", `{"pubkey":"` + payer + `"}`},
	}, "", "", false)
	if len(self) != 0 {
		t.Fatalf("self zap notice: %+v", self)
	}
	if reply[0].Args["who_replied_to_it"] != "author" || reply[0].Args["your_post"] != "parent-id" {
		t.Fatalf("reply args: %+v", reply[0].Args)
	}
}
