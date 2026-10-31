package read

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestBuildTopZapAdReadsCommentAndProfile(t *testing.T) {
	receipt := json.RawMessage(`{"tags":[["description","{\"content\":\"great\\n\\tpost\",\"tags\":[[\"amount\",\"21000\"]]}"]]}`)
	profile := json.RawMessage(`{"name":"bob","display_name":"Bob","picture":"https://cdn.example/a.png"}`)
	ad := buildTopZapAd("event", "sender", 21, receipt, profile)
	if ad.Comment != "great post" {
		t.Fatalf("comment = %q", ad.Comment)
	}
	if ad.Name != "bob" || ad.DisplayName != "Bob" || ad.Picture != "https://cdn.example/a.png" {
		t.Fatalf("profile = %#v", ad)
	}
	if ad.Sats != 21 || ad.SenderPubkey != "sender" {
		t.Fatalf("identity = %#v", ad)
	}
}

func TestBuildTopZapAdDropsUnsafePictureAndHugeComment(t *testing.T) {
	receipt := json.RawMessage(`{"tags":[["description","{\"content\":\"` + strings.Repeat("a", 200) + `\"}"]]}`)
	profile := json.RawMessage(`{"picture":"javascript:alert(1)"}`)
	ad := buildTopZapAd("event", "sender", 1, receipt, profile)
	if ad.Picture != "" {
		t.Fatalf("picture = %q", ad.Picture)
	}
	if len([]rune(ad.Comment)) > 160 {
		t.Fatalf("comment length = %d", len([]rune(ad.Comment)))
	}
	if !strings.HasSuffix(ad.Comment, "…") {
		t.Fatalf("comment = %q", ad.Comment)
	}
	longPicture := "https://cdn.example/" + strings.Repeat("a", 2048)
	if picture := buildTopZapAd("event", "sender", 1, receipt, json.RawMessage(`{"picture":"`+longPicture+`"}`)).Picture; picture != "" {
		t.Fatalf("long picture length = %d", len(picture))
	}
	exactPicture := "https://cdn.example/" + strings.Repeat("b", 2048-len("https://cdn.example/"))
	if picture := buildTopZapAd("event", "sender", 1, receipt, json.RawMessage(`{"picture":"`+exactPicture+`"}`)).Picture; picture != exactPicture {
		t.Fatalf("picture length = %d", len(picture))
	}
}

func TestLookupTopZapQueryIDsKeepCaseVariants(t *testing.T) {
	got := topZapLookupIDs([]string{"AbC", "abc", " "})
	if len(got) != 2 || got[0] != "AbC" || got[1] != "abc" {
		t.Fatalf("ids = %#v", got)
	}
}
