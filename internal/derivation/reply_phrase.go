package derivation

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

// repeatedReplyPhraseDiscount scales each replier in a pile. Eighteen
// identical "GM" replies become about 3.6 weighted votes instead of 18,
// and a single "GM" is left alone.
const repeatedReplyPhraseDiscount = 0.2

// A phrase is low-substance when it is empty after normalization (emoji-only),
// a greeting, or a short phrase. Longer identical text is left at full weight.
const (
	lowSubstanceMaxWords = 4
	lowSubstanceMaxRunes = 24
)

// oneLineWishMaxWords is the longest stock wish. A leading "GM" plus a wish
// of this length stays in the greeting pile. A reply that adds a remark uses
// other words, or is longer, and keeps full weight.
const (
	oneLineWishMaxWords = 8
	oneLineWishMaxRunes = 64
)

var (
	replyURLPattern     = regexp.MustCompile(`https?://\S+|www\.\S+`)
	replyMentionPattern = regexp.MustCompile(`(?i)(?:nostr:)?(?:npub|nprofile|nevent|naddr|note)1[0-9a-z]+`)
	replyNonWordPattern = regexp.MustCompile(`[^\p{L}\p{N}\s]+`)
	replySpacePattern   = regexp.MustCompile(`\s+`)
)

// greetingTokens are the words that make a reply a greeting even when the
// same token is repeated past the short-phrase length ("gm gm gm gm gm").
// "good" counts only as part of "good morning" and the other stock pairs,
// so "good point" stays a remark.
var greetingTokens = map[string]struct{}{
	"gm": {}, "gn": {}, "ge": {}, "ga": {},
	"pv": {}, "pura": {}, "vida": {},
	"hi": {}, "hey": {}, "yo": {}, "sup": {}, "hello": {},
	"morning": {}, "mornin": {}, "night": {}, "evening": {}, "afternoon": {},
	"good": {},
}

// wishWords are the rest of a one-line wish ("have a great day",
// "enjoy your wonderful day", "enjoy coffee have great day"). A phrase is
// a wish only when every word is in this set or greetingTokens, so a remark
// that happens to start with "GM" stays out of the pile.
var wishWords = map[string]struct{}{
	"a": {}, "and": {}, "your": {},
	"enjoy": {}, "have": {}, "great": {}, "wonderful": {}, "lovely": {},
	"nice": {}, "blessed": {}, "pleasant": {}, "beautiful": {}, "awesome": {},
	"sweet": {}, "dear": {}, "soul": {}, "day": {}, "peace": {}, "coffee": {},
	"rest": {}, "hug": {}, "hugs": {}, "thanks": {}, "thank": {},
	"wish": {}, "wishing": {}, "friend": {}, "friends": {}, "happy": {},
	"best": {}, "luck": {}, "one": {}, "weekend": {}, "dream": {}, "dreams": {},
	"well": {},
}

type weightedReply struct {
	Pubkey  string
	Content string
	Weight  float64
}

// replyWeightSum is one vote per pubkey after phrase discounts.
// Total includes a discounted greeting pile. Substantive is the part that
// unlocks reactions and reposts: the greeting pile is omitted, and a single
// greeting still counts.
type replyWeightSum struct {
	Total       float64
	Substantive float64
}

// sumDiscountedReplyWeights totals one vote per pubkey. A replier's vote is
// their trust weight. Their phrase is the longest remark they wrote, so a
// real sentence alongside "GM" keeps full weight. A one-line wish does not
// count as that remark.
//
// Greeting variants share one bucket: "gm", "good morning", "GM Gigi", "PV",
// a heart, and a short wish. The discount applies when at least two
// positive-weight repliers are in that bucket. Distinct short remarks still
// discount only when the text matches.
func sumDiscountedReplyWeights(rows []weightedReply) replyWeightSum {
	type replier struct {
		weight   float64
		phrase   string
		low      bool
		greeting bool
	}
	byPubkey := make(map[string]*replier, len(rows))
	for _, row := range rows {
		phrase := normalizeReplyPhrase(row.Content)
		greeting := isGreetingClass(phrase)
		low := isLowSubstanceReplyPhrase(phrase)
		current, ok := byPubkey[row.Pubkey]
		if !ok {
			byPubkey[row.Pubkey] = &replier{weight: row.Weight, phrase: phrase, low: low, greeting: greeting}
			continue
		}
		if row.Weight > current.weight {
			current.weight = row.Weight
		}
		// Any remark pulls the person out of the greeting pile. Among
		// remarks, the longest phrase wins so a sentence beats a short duplicate.
		if !greeting && (current.greeting || utf8.RuneCountInString(phrase) > utf8.RuneCountInString(current.phrase)) {
			current.phrase = phrase
			current.low = low
			current.greeting = false
			continue
		}
		if current.greeting && greeting && utf8.RuneCountInString(phrase) > utf8.RuneCountInString(current.phrase) {
			current.phrase = phrase
			current.low = low
		}
	}

	greetingCount := 0
	phraseCounts := make(map[string]int, len(byPubkey))
	for _, replier := range byPubkey {
		if replier.weight <= 0 {
			continue
		}
		if replier.greeting {
			greetingCount++
			continue
		}
		if replier.low {
			phraseCounts[replier.phrase]++
		}
	}

	var sum replyWeightSum
	for _, replier := range byPubkey {
		weight := replier.weight
		discountedGreeting := false
		if replier.greeting && greetingCount >= 2 {
			weight *= repeatedReplyPhraseDiscount
			discountedGreeting = true
		} else if !replier.greeting && replier.low && phraseCounts[replier.phrase] >= 2 {
			weight *= repeatedReplyPhraseDiscount
		}
		sum.Total += weight
		if !discountedGreeting {
			sum.Substantive += weight
		}
	}
	return sum
}

func normalizeReplyPhrase(content string) string {
	normalized := strings.ToLower(content)
	normalized = replyURLPattern.ReplaceAllString(normalized, " ")
	normalized = replyMentionPattern.ReplaceAllString(normalized, " ")
	normalized = replyNonWordPattern.ReplaceAllString(normalized, " ")
	normalized = replySpacePattern.ReplaceAllString(strings.TrimSpace(normalized), " ")
	return normalized
}

func isGreetingToken(word string) bool {
	_, ok := greetingTokens[word]
	return ok
}

func isWishWord(word string) bool {
	if isGreetingToken(word) {
		return true
	}
	_, ok := wishWords[word]
	return ok
}

func allGreeting(fields []string) bool {
	if len(fields) == 0 {
		return false
	}
	for _, field := range fields {
		if !isGreetingToken(field) {
			return false
		}
	}
	return true
}

func allWish(fields []string) bool {
	if len(fields) == 0 {
		return false
	}
	for _, field := range fields {
		if !isWishWord(field) {
			return false
		}
	}
	return true
}

// leadingSalutationWords is the greeting prefix. "good" starts one only when
// the next word is a greeting too, so "good morning" is a salutation and
// "good point" is not.
func leadingSalutationWords(fields []string) int {
	if len(fields) == 0 {
		return 0
	}
	n := 0
	switch {
	case fields[0] != "good" && isGreetingToken(fields[0]):
		n = 1
	case fields[0] == "good" && len(fields) > 1 && isGreetingToken(fields[1]) && fields[1] != "good":
		n = 2
	case len(fields) == 1 && fields[0] == "good":
		n = 1
	default:
		return 0
	}
	for n < len(fields) && isGreetingToken(fields[n]) {
		n++
	}
	return n
}

// isGreetingClass reports whether a normalized reply is a greeting variant
// or a one-line wish. Emoji-only replies are in the class. A salutation plus
// up to four leftover words is still the pile ("gm gigi", "gm now is
// coffeetime"). A longer tail has to be entirely a wish.
func isGreetingClass(phrase string) bool {
	if phrase == "" {
		return true
	}
	fields := strings.Fields(phrase)
	if len(fields) == 0 {
		return true
	}
	if allGreeting(fields) {
		return true
	}
	lead := leadingSalutationWords(fields)
	if lead > 0 {
		remainder := fields[lead:]
		if len(remainder) <= lowSubstanceMaxWords {
			return true
		}
		if len(remainder) <= oneLineWishMaxWords && utf8.RuneCountInString(strings.Join(remainder, " ")) <= oneLineWishMaxRunes && allWish(remainder) {
			return true
		}
		return false
	}
	return len(fields) <= oneLineWishMaxWords && utf8.RuneCountInString(phrase) <= oneLineWishMaxRunes && allWish(fields)
}

func isLowSubstanceReplyPhrase(phrase string) bool {
	if phrase == "" {
		return true
	}
	fields := strings.Fields(phrase)
	if len(fields) == 0 {
		return true
	}
	if allGreeting(fields) {
		return true
	}
	return len(fields) <= lowSubstanceMaxWords && utf8.RuneCountInString(phrase) <= lowSubstanceMaxRunes
}
