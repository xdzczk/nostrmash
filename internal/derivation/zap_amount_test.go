package derivation

import "testing"

func TestParseBolt11AmountSats(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"lnbc210n1pw", 21},
		{"lnbc250n1pw", 25},
		{"lnbc10u1pw", 1000},
		{"lnbc2.5u1pw", 250},
		{"lnbc1pw", 0},
		{"LNBC210N1PW", 21},
		{"lnbc11pw", 100_000_000},
		{"lnbc1m1pw", 100_000},
		{"lntb1u1pw", 100},
		{"lnbcrt10u1pw", 1000},
		{"lnbc10000p1pw", 1},
		{"lnbc1p1pw", 0},
		{"lnbc10x1pw", 0},
		{"not-an-invoice", 0},
		{"", 0},
	}
	for _, tc := range cases {
		if got := parseBolt11AmountSats(tc.in); got != tc.want {
			t.Fatalf("parseBolt11AmountSats(%q)=%d want %d", tc.in, got, tc.want)
		}
	}
}

func TestZapReceiptAmountSats(t *testing.T) {
	if got := parseZapAmountSats("21000"); got != 21 {
		t.Fatalf("parseZapAmountSats(21000)=%d want 21", got)
	}

	amountTag := [][]string{
		{"amount", "21000"},
		{"bolt11", "lnbc250n1pw"},
		{"description", `{"content":"Good Vibes","tags":[["amount","999000"]]}`},
	}
	if got := zapReceiptAmountSats(amountTag); got != 21 {
		t.Fatalf("amount tag = %d want 21", got)
	}

	descriptionAmount := [][]string{
		{"bolt11", "lnbc210n1pw"},
		{"description", `{"content":"Good Vibes","tags":[["amount","25000"]]}`},
	}
	if got := zapReceiptAmountSats(descriptionAmount); got != 25 {
		t.Fatalf("description amount = %d want 25", got)
	}

	invoiceOnly := [][]string{
		{"p", "receiver"},
		{"e", "note"},
		{"bolt11", "lnbc210n1pwgood"},
		{"description", `{"content":"Good Vibes","tags":[]}`},
	}
	if got := zapReceiptAmountSats(invoiceOnly); got != 21 {
		t.Fatalf("invoice-only amount = %d want 21", got)
	}

	noAmount := [][]string{
		{"p", "receiver"},
		{"e", "note"},
		{"bolt11", "lnbc1pwzero"},
		{"description", `{"content":"Good Vibes","tags":[]}`},
	}
	if got := zapReceiptAmountSats(noAmount); got != 0 {
		t.Fatalf("no-amount invoice = %d want 0", got)
	}
}
