package relaynip11

import (
	"encoding/json"
	"testing"
)

func TestParse_KeepsWhitelist(t *testing.T) {
	body := []byte(`{
		"name": " Demo ",
		"description": "A relay\u0000 with text",
		"icon": "https://cdn.example/icon.png",
		"banner": "javascript:alert(1)",
		"pubkey": "ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789",
		"contact": "ops@example.com",
		"supported_nips": [11, 1, 1, -3, 11],
		"software": "strfry",
		"version": "1.0.0",
		"payments_url": "https://example.com/pay",
		"limitation": {"auth_required": false, "payment_required": true, "max_message_length": 16384, "extra": true},
		"fees": {"admission": [{"amount": 1000, "unit": "msats"}]},
		"retention": [{"kinds": [0, 1, [5, 7]], "time": 3600}]
	}`)
	info, err := Parse(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if info.Name != "Demo" || info.Description != "A relay with text" {
		t.Fatalf("text = %#v", info)
	}
	if info.Icon != "https://cdn.example/icon.png" || info.Banner != "" {
		t.Fatalf("urls icon=%q banner=%q", info.Icon, info.Banner)
	}
	if info.Pubkey != "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789" {
		t.Fatalf("pubkey = %s", info.Pubkey)
	}
	if len(info.SupportedNIPs) != 2 || info.SupportedNIPs[0] != 1 || info.SupportedNIPs[1] != 11 {
		t.Fatalf("nips = %#v", info.SupportedNIPs)
	}
	if info.Limitation == nil || info.Limitation.AuthRequired == nil || *info.Limitation.AuthRequired {
		t.Fatal("expected auth_required false")
	}
	if info.Limitation.PaymentRequired == nil || !*info.Limitation.PaymentRequired {
		t.Fatal("expected payment_required true")
	}
	raw, err := json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	if json.Unmarshal(raw, &map[string]any{}) != nil {
		t.Fatal("info is not json")
	}
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if _, ok := decoded["payments_url"]; ok {
		t.Fatal("payments_url should be dropped")
	}
	if info.Fees == nil || len(info.Fees.Admission) != 1 || info.Fees.Admission[0].Amount != 1000 {
		t.Fatalf("fees = %#v", info.Fees)
	}
	if len(info.Retention) != 1 || string(info.Retention[0].Kinds) != "[0,1,[5,7]]" {
		t.Fatalf("retention kinds = %s", info.Retention[0].Kinds)
	}
}

func TestParse_RejectsNonObject(t *testing.T) {
	for _, body := range []string{"", "[]", "<html></html>", "not json"} {
		if _, err := Parse([]byte(body)); err == nil || err.Error() != "not_json" {
			t.Fatalf("%q: %v", body, err)
		}
	}
}

func TestParse_KeepsSoftwareURL(t *testing.T) {
	info, err := Parse([]byte(`{"software":"https://github.com/hoytech/strfry","version":"not a version!"}`))
	if err != nil {
		t.Fatal(err)
	}
	if info.Software != "https://github.com/hoytech/strfry" || info.Version != "" {
		t.Fatalf("%#v", info)
	}
}

func TestParse_DropsPrivateIcon(t *testing.T) {
	info, err := Parse([]byte(`{"icon":"http://127.0.0.1/icon.png","name":"ok"}`))
	if err != nil {
		t.Fatal(err)
	}
	if info.Icon != "" || info.Name != "ok" {
		t.Fatalf("%#v", info)
	}
}
