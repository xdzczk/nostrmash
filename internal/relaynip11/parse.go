package relaynip11

import (
	"bytes"
	"encoding/json"
	"math"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/xdzczk/nostrmash/internal/relayurl"
)

const (
	maxNameRunes        = 80
	maxDescriptionRunes = 2000
	maxContactRunes     = 200
	maxHTTPURLRunes     = 500
	maxNIPs             = 64
	maxFeeEntries       = 8
	maxRetentionRules   = 16
	maxKindList         = 32
	maxLimitationValue  = int64(1 << 31)
)

var (
	softwareToken = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._+/-]{0,79}$`)
	versionToken  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._+-]{0,63}$`)
	feeUnit       = regexp.MustCompile(`^[A-Za-z]{1,16}$`)
)

// Parse keeps the NIP-11 whitelist and drops every other field.
// A document that is not a JSON object returns errNotJSON.
func Parse(body []byte) (Info, error) {
	body = bytes.TrimPrefix(bytes.TrimSpace(body), []byte{0xEF, 0xBB, 0xBF})
	if len(body) == 0 || body[0] != '{' {
		return Info{}, errNotJSON
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return Info{}, errNotJSON
	}
	info := Info{
		Name:          cleanText(rawString(raw["name"]), maxNameRunes, false),
		Description:   cleanText(rawString(raw["description"]), maxDescriptionRunes, true),
		Icon:          cleanHTTPURL(rawString(raw["icon"])),
		Banner:        cleanHTTPURL(rawString(raw["banner"])),
		Pubkey:        cleanPubkey(rawString(raw["pubkey"])),
		Contact:       cleanText(rawString(raw["contact"]), maxContactRunes, false),
		SupportedNIPs: parseNIPs(raw["supported_nips"]),
		Software:      cleanSoftware(rawString(raw["software"])),
		Version:       cleanVersion(rawString(raw["version"])),
		Limitation:    parseLimitation(raw["limitation"]),
		Fees:          parseFees(raw["fees"]),
		Retention:     parseRetention(raw["retention"]),
	}
	return info, nil
}

func rawString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return ""
	}
	return text
}

func cleanText(value string, maxRunes int, allowNewlines bool) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range value {
		if r == '\n' && allowNewlines {
			b.WriteByte('\n')
			continue
		}
		if r < 0x20 || r == 0x7f {
			continue
		}
		b.WriteRune(r)
	}
	return truncateRunes(strings.TrimSpace(b.String()), maxRunes)
}

func truncateRunes(value string, maxRunes int) string {
	if maxRunes <= 0 || utf8.RuneCountInString(value) <= maxRunes {
		return value
	}
	index := 0
	for seen := 0; seen < maxRunes; seen++ {
		_, size := utf8.DecodeRuneInString(value[index:])
		index += size
	}
	return value[:index]
}

func cleanPubkey(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if len(value) != 64 {
		return ""
	}
	for _, c := range value {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return ""
		}
	}
	return value
}

func cleanHTTPURL(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || utf8.RuneCountInString(value) > maxHTTPURLRunes {
		return ""
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return ""
	}
	switch strings.ToLower(parsed.Scheme) {
	case "https", "http":
	default:
		return ""
	}
	if parsed.Hostname() == "" {
		return ""
	}
	if err := relayurl.HostIsPublic(parsed.Hostname()); err != nil {
		return ""
	}
	return value
}

func cleanSoftware(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if strings.Contains(value, "://") {
		return cleanHTTPURL(value)
	}
	if softwareToken.MatchString(value) {
		return value
	}
	return ""
}

func cleanVersion(value string) string {
	value = strings.TrimSpace(value)
	if versionToken.MatchString(value) {
		return value
	}
	return ""
}

func parseNIPs(raw json.RawMessage) []int {
	if len(raw) == 0 {
		return nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil
	}
	seen := make(map[int]struct{}, len(items))
	out := make([]int, 0, len(items))
	for _, item := range items {
		if len(out) >= maxNIPs {
			break
		}
		n, ok := parseNonNegInt(item)
		if !ok || n > 100000 {
			continue
		}
		nip := int(n)
		if _, ok := seen[nip]; ok {
			continue
		}
		seen[nip] = struct{}{}
		out = append(out, nip)
	}
	slices.Sort(out)
	if len(out) == 0 {
		return nil
	}
	return out
}

func parseLimitation(raw json.RawMessage) *Limitation {
	fields, ok := objectFields(raw)
	if !ok {
		return nil
	}
	lim := &Limitation{
		MaxMessageLength:    parseBoundedInt(fields["max_message_length"]),
		MaxSubscriptions:    parseBoundedInt(fields["max_subscriptions"]),
		MaxFilters:          parseBoundedInt(fields["max_filters"]),
		MaxLimit:            parseBoundedInt(fields["max_limit"]),
		MaxSubidLength:      parseBoundedInt(fields["max_subid_length"]),
		MaxEventTags:        parseBoundedInt(fields["max_event_tags"]),
		MaxContentLength:    parseBoundedInt(fields["max_content_length"]),
		MinPowDifficulty:    parseBoundedInt(fields["min_pow_difficulty"]),
		AuthRequired:        parseBoolPtr(fields["auth_required"]),
		PaymentRequired:     parseBoolPtr(fields["payment_required"]),
		RestrictedWrites:    parseBoolPtr(fields["restricted_writes"]),
		CreatedAtLowerLimit: parseBoundedInt(fields["created_at_lower_limit"]),
		CreatedAtUpperLimit: parseBoundedInt(fields["created_at_upper_limit"]),
	}
	if *lim == (Limitation{}) {
		return nil
	}
	return lim
}

func parseFees(raw json.RawMessage) *Fees {
	fields, ok := objectFields(raw)
	if !ok {
		return nil
	}
	fees := &Fees{
		Admission:    parseFeeList(fields["admission"]),
		Subscription: parseFeeList(fields["subscription"]),
		Publication:  parseFeeList(fields["publication"]),
	}
	if len(fees.Admission) == 0 && len(fees.Subscription) == 0 && len(fees.Publication) == 0 {
		return nil
	}
	return fees
}

func parseFeeList(raw json.RawMessage) []Fee {
	var items []json.RawMessage
	if len(raw) == 0 || json.Unmarshal(raw, &items) != nil {
		return nil
	}
	out := make([]Fee, 0, len(items))
	for _, item := range items {
		if len(out) >= maxFeeEntries {
			break
		}
		fields, ok := objectFields(item)
		if !ok {
			continue
		}
		amount, ok := parseNonNegInt(fields["amount"])
		if !ok {
			continue
		}
		unit := rawString(fields["unit"])
		if !feeUnit.MatchString(unit) {
			continue
		}
		fee := Fee{Amount: amount, Unit: unit}
		if period, ok := parseNonNegInt(fields["period"]); ok && period <= maxLimitationValue {
			fee.Period = &period
		}
		fee.Kinds = parseKindNumbers(fields["kinds"])
		out = append(out, fee)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func parseKindNumbers(raw json.RawMessage) []int {
	var items []json.RawMessage
	if len(raw) == 0 || json.Unmarshal(raw, &items) != nil {
		return nil
	}
	out := make([]int, 0, len(items))
	for _, item := range items {
		if len(out) >= maxKindList {
			break
		}
		n, ok := parseNonNegInt(item)
		if !ok || n > 65535 {
			continue
		}
		out = append(out, int(n))
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func parseRetention(raw json.RawMessage) []Retention {
	var items []json.RawMessage
	if len(raw) == 0 || json.Unmarshal(raw, &items) != nil {
		return nil
	}
	out := make([]Retention, 0, len(items))
	for _, item := range items {
		if len(out) >= maxRetentionRules {
			break
		}
		fields, ok := objectFields(item)
		if !ok {
			continue
		}
		rule := Retention{Kinds: sanitizeKinds(fields["kinds"])}
		if n, ok := parseNonNegInt(fields["time"]); ok && n <= maxLimitationValue {
			rule.Time = &n
		}
		if n, ok := parseNonNegInt(fields["count"]); ok && n <= maxLimitationValue {
			rule.Count = &n
		}
		if len(rule.Kinds) == 0 && rule.Time == nil && rule.Count == nil {
			continue
		}
		out = append(out, rule)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// sanitizeKinds keeps a NIP-11 kinds array of numbers and two-element ranges.
func sanitizeKinds(raw json.RawMessage) json.RawMessage {
	var items []json.RawMessage
	if len(raw) == 0 || json.Unmarshal(raw, &items) != nil || len(items) == 0 {
		return nil
	}
	out := make([]any, 0, len(items))
	for _, item := range items {
		if len(out) >= maxKindList {
			break
		}
		if n, ok := parseNonNegInt(item); ok && n <= 65535 {
			out = append(out, int(n))
			continue
		}
		var pair []json.RawMessage
		if json.Unmarshal(item, &pair) != nil || len(pair) != 2 {
			continue
		}
		lo, okLo := parseNonNegInt(pair[0])
		hi, okHi := parseNonNegInt(pair[1])
		if !okLo || !okHi || hi < lo || hi > 65535 {
			continue
		}
		out = append(out, []int{int(lo), int(hi)})
	}
	if len(out) == 0 {
		return nil
	}
	encoded, err := json.Marshal(out)
	if err != nil {
		return nil
	}
	return encoded
}

func objectFields(raw json.RawMessage) (map[string]json.RawMessage, bool) {
	if len(raw) == 0 || raw[0] != '{' {
		return nil, false
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, false
	}
	return fields, true
}

func parseBoolPtr(raw json.RawMessage) *bool {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var value bool
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil
	}
	return &value
}

func parseBoundedInt(raw json.RawMessage) *int64 {
	n, ok := parseNonNegInt(raw)
	if !ok || n > maxLimitationValue {
		return nil
	}
	return &n
}

func parseNonNegInt(raw json.RawMessage) (int64, bool) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, false
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err == nil {
		return n, n >= 0
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, false
	}
	if f < 0 || f > math.MaxInt64 || f != math.Trunc(f) {
		return 0, false
	}
	return int64(f), true
}

// errNotJSON is the parse failure for a body that is not a NIP-11 object.
// The fetcher uses the same code when the response is HTML.
var errNotJSON = &FetchError{Code: "not_json"}
