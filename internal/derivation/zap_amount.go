package derivation

import (
	"encoding/json"
	"math/big"
	"strings"
)

// zapReceiptAmountSats is the single sats value stored for a kind-9735 receipt.
// A positive receipt amount tag wins, then a positive amount tag inside the
// description zap request, then the BOLT11 human-readable amount. Zero and
// unparsable values are not amounts, so the next source is tried.
func zapReceiptAmountSats(tags [][]string) int64 {
	if sats := parseZapAmountSats(firstTagValue(tags, "amount")); sats > 0 {
		return sats
	}
	for _, tag := range tags {
		if len(tag) < 2 || tag[0] != "description" {
			continue
		}
		if sats := descriptionZapAmountSats(tag[1]); sats > 0 {
			return sats
		}
	}
	if sats := parseBolt11AmountSats(firstTagValue(tags, "bolt11")); sats > 0 {
		return sats
	}
	return 0
}

func descriptionZapAmountSats(raw string) int64 {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	var req struct {
		Tags [][]string `json:"tags"`
	}
	if err := json.Unmarshal([]byte(raw), &req); err != nil {
		return 0
	}
	return parseZapAmountSats(firstTagValue(req.Tags, "amount"))
}

// parseBolt11AmountSats reads only the BOLT11 human-readable amount.
// The HRP is the prefix before the bech32 separator '1'. After lnbc, lntb,
// or lnbcrt, an optional decimal and multiplier encode BTC. Multipliers are
// m milli, u micro, n nano, and p pico; no multiplier means whole BTC.
// A missing, zero, or unparsable amount returns 0. Sub-sat amounts floor to 0.
func parseBolt11AmountSats(invoice string) int64 {
	invoice = strings.TrimSpace(invoice)
	if invoice == "" {
		return 0
	}
	sep := strings.LastIndexByte(invoice, '1')
	if sep <= 0 {
		return 0
	}
	hrp := strings.ToLower(invoice[:sep])
	var amount string
	switch {
	case strings.HasPrefix(hrp, "lnbcrt"):
		amount = hrp[len("lnbcrt"):]
	case strings.HasPrefix(hrp, "lnbc"):
		amount = hrp[len("lnbc"):]
	case strings.HasPrefix(hrp, "lntb"):
		amount = hrp[len("lntb"):]
	default:
		return 0
	}
	if amount == "" {
		return 0
	}
	mult := byte(0)
	last := amount[len(amount)-1]
	if last == 'm' || last == 'u' || last == 'n' || last == 'p' {
		mult = last
		amount = amount[:len(amount)-1]
		if amount == "" {
			return 0
		}
	}
	num, den := bolt11SatsScale(mult)
	return bolt11DecimalToSats(amount, num, den)
}

// bolt11SatsScale returns sats per 1.0 of the HRP unit, as num/den.
func bolt11SatsScale(mult byte) (num, den int64) {
	switch mult {
	case 'm':
		return 100_000, 1
	case 'u':
		return 100, 1
	case 'n':
		return 1, 10
	case 'p':
		return 1, 10_000
	default:
		return 100_000_000, 1
	}
}

func bolt11DecimalToSats(raw string, num, den int64) int64 {
	if num <= 0 || den <= 0 {
		return 0
	}
	whole, frac, ok := splitBolt11Decimal(raw)
	if !ok {
		return 0
	}
	digits := strings.TrimLeft(whole+frac, "0")
	if digits == "" || len(digits) > 30 {
		return 0
	}
	value := new(big.Int)
	if _, ok := value.SetString(digits, 10); !ok {
		return 0
	}
	value.Mul(value, big.NewInt(num))
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(len(frac))), nil)
	scale.Mul(scale, big.NewInt(den))
	if scale.Sign() == 0 {
		return 0
	}
	value.Quo(value, scale)
	if !value.IsInt64() || value.Sign() <= 0 {
		return 0
	}
	return value.Int64()
}

func splitBolt11Decimal(raw string) (whole, frac string, ok bool) {
	if raw == "" || strings.Count(raw, ".") > 1 {
		return "", "", false
	}
	whole, frac, _ = strings.Cut(raw, ".")
	if whole == "" && frac == "" {
		return "", "", false
	}
	if whole == "" {
		whole = "0"
	}
	if !decimalDigits(whole) || !decimalDigits(frac) {
		return "", "", false
	}
	return whole, frac, true
}

func decimalDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
