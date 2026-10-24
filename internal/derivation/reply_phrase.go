package derivation

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

// repeatedReplyPhraseDiscount scales each replier who piled onto the same
// greeting or short phrase. Eighteen identical "GM" replies become about
// 3.6 weighted votes instead of 18, and a single "GM" is left alone.
const repeatedReplyPhraseDiscount = 0.2

// A phrase is low-substance when it is empty after normalization (emoji-only),
// a greeting, or a short phrase. Longer identical text is left at full weight.
const (
	lowSubstanceMaxWords = 4
	lowSubstanceMaxRunes = 24
)

var (
	replyURLPattern     = regexp.MustCompile(`https?://\S+|www\.\S+`)
	replyMentionPattern = regexp.MustCompile(`(?i)(?:nostr:)?(?:npub|nprofile|nevent|naddr|note)1[0-9a-z]+`)
	replyNonWordPattern = regexp.MustCompile(`[^\p{L}\p{N}\s]+`)
	replySpacePattern   = regexp.MustCompile(`\s+`)
)

// greetingTokens are the words that make a reply a greeting even when the
// same token is repeated past the short-phrase length ("gm gm gm gm gm").
var greetingTokens = map[string]struct{}{
	"gm": {}, "gn": {}, "ge": {}, "ga": {},
	"pv": {}, "pura": {}, "vida": {},
	"hi": {}, "hey": {}, "yo": {}, "sup": {}, "hello": {},
	"morning": {}, "night": {}, "evening": {}, "afternoon": {},
	"good": {},
}

type weightedReply struct {
	Pubkey  string
	Content string
	Weight  float64
}

// sumDiscountedReplyWeights totals one vote per pubkey. A replier's vote is
// their trust weight; their phrase is the longest normalized reply they
// wrote in the window, so a real sentence alongside "GM" keeps full weight.
// The discount applies only when at least two positive-weight repliers share
// that exact low-substance phrase.
func sumDiscountedReplyWeights(rows []weightedReply) float64 {
	type replier struct {
		weight float64
		phrase string
		low    bool
	}
	byPubkey := make(map[string]*replier, len(rows))
	for _, row := range rows {
		phrase := normalizeReplyPhrase(row.Content)
		low := isLowSubstanceReplyPhrase(phrase)
		current, ok := byPubkey[row.Pubkey]
		if !ok {
			byPubkey[row.Pubkey] = &replier{weight: row.Weight, phrase: phrase, low: low}
			continue
		}
		if row.Weight > current.weight {
			current.weight = row.Weight
		}
		if !low && (current.low || utf8.RuneCountInString(phrase) > utf8.RuneCountInString(current.phrase)) {
			current.phrase = phrase
			current.low = false
			continue
		}
		if current.low && low && utf8.RuneCountInString(phrase) > utf8.RuneCountInString(current.phrase) {
			current.phrase = phrase
		}
	}

	counts := make(map[string]int, len(byPubkey))
	for _, replier := range byPubkey {
		if replier.low && replier.weight > 0 {
			counts[replier.phrase]++
		}
	}
	total := 0.0
	for _, replier := range byPubkey {
		weight := replier.weight
		if replier.low && counts[replier.phrase] >= 2 {
			weight *= repeatedReplyPhraseDiscount
		}
		total += weight
	}
	return total
}

func normalizeReplyPhrase(content string) string {
	normalized := strings.ToLower(content)
	normalized = replyURLPattern.ReplaceAllString(normalized, " ")
	normalized = replyMentionPattern.ReplaceAllString(normalized, " ")
	normalized = replyNonWordPattern.ReplaceAllString(normalized, " ")
	normalized = replySpacePattern.ReplaceAllString(strings.TrimSpace(normalized), " ")
	return normalized
}

func isLowSubstanceReplyPhrase(phrase string) bool {
	if phrase == "" {
		return true
	}
	fields := strings.Fields(phrase)
	if len(fields) == 0 {
		return true
	}
	allGreeting := true
	for _, field := range fields {
		if _, ok := greetingTokens[field]; !ok {
			allGreeting = false
			break
		}
	}
	if allGreeting {
		return true
	}
	return len(fields) <= lowSubstanceMaxWords && utf8.RuneCountInString(phrase) <= lowSubstanceMaxRunes
}
