package derivation

import (
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

// lowSubstanceMaxWords is the longest tail that still counts as a greeting
// ("gm gigi") and the base length for a short reply. A short reply may run
// one word longer so a near-copy can add a single word. A repeated sentence
// past that length is still one pile when the text matches exactly.
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

// replyWeightSum is one vote per pubkey after phrase piles.
// Total includes one vote from each copied-phrase pile: the strongest trust
// weight in that pile, once. Substantive is the part that unlocks reactions
// and reposts. A greeting pile, a repeated short phrase, or a repeated
// sentence is omitted, and a single reply still counts. DiscountedPubkeys
// are the people in those piles; a repost from one of them is the same vote.
// UnlockShare stays 1 while a pile is not most of the repliers. When it is,
// the share is the fraction of repliers outside the pile.
type replyWeightSum struct {
	Total             float64
	Substantive       float64
	DiscountedPubkeys map[string]struct{}
	UnlockShare       float64
}

// sumDiscountedReplyWeights totals one vote per pubkey. A replier's vote is
// their trust weight. Their phrase is the longest remark they wrote, so a
// real sentence alongside "GM" keeps full weight. A one-line wish does not
// count as that remark.
//
// Greeting variants share one bucket: "gm", "good morning", "GM Gigi", "PV",
// a heart, and a short wish. A repeated short phrase does too, and a
// near-copy joins it: one extra or swapped word, or a one-character typo.
// A longer remark stays out of that near-copy pile. The same sentence
// repeated by two people is its own pile, whatever its length.
//
// A pile of two or more counts once, as the strongest trust weight in it.
// Extra people add nothing. One person still counts in full and still
// unlocks reactions and reposts. A replier with zero trust weight still
// counts as a person in the pile and in the share, and adds nothing to the vote.
func sumDiscountedReplyWeights(rows []weightedReply) replyWeightSum {
	type replier struct {
		weight   float64
		phrase   string
		greeting bool
	}
	byPubkey := make(map[string]*replier, len(rows))
	for _, row := range rows {
		phrase := normalizeReplyPhrase(row.Content)
		greeting := isGreetingClass(phrase)
		current, ok := byPubkey[row.Pubkey]
		if !ok {
			byPubkey[row.Pubkey] = &replier{weight: row.Weight, phrase: phrase, greeting: greeting}
			continue
		}
		if row.Weight > current.weight {
			current.weight = row.Weight
		}
		// Any remark pulls the person out of the greeting pile. Among
		// remarks, the longest phrase wins so a sentence beats a short duplicate.
		if !greeting && (current.greeting || utf8.RuneCountInString(phrase) > utf8.RuneCountInString(current.phrase)) {
			current.phrase = phrase
			current.greeting = false
			continue
		}
		if current.greeting && greeting && utf8.RuneCountInString(phrase) > utf8.RuneCountInString(current.phrase) {
			current.phrase = phrase
		}
	}

	greetingCount := 0
	greetingVote := 0.0
	shortCounts := make(map[string]int, len(byPubkey))
	longCounts := make(map[string]int, len(byPubkey))
	for _, replier := range byPubkey {
		if replier.greeting {
			greetingCount++
			if replier.weight > greetingVote {
				greetingVote = replier.weight
			}
			continue
		}
		if isShortReplyCandidate(replier.phrase) {
			shortCounts[replier.phrase]++
			continue
		}
		longCounts[replier.phrase]++
	}
	clusters := shortPhraseClusterKeys(shortCounts)
	clusterSize := make(map[string]int, len(shortCounts))
	for phrase, n := range shortCounts {
		clusterSize[clusters[phrase]] += n
	}

	var sum replyWeightSum
	clusterVote := make(map[string]float64, len(shortCounts))
	longVote := make(map[string]float64, len(longCounts))
	repliers := 0
	substantiveRepliers := 0
	for pubkey, replier := range byPubkey {
		repliers++
		weight := replier.weight
		pileKey, piled := replyPileKey(replier.greeting, replier.phrase, greetingCount, clusters, clusterSize, longCounts)
		if !piled {
			sum.Total += weight
			sum.Substantive += weight
			substantiveRepliers++
			continue
		}
		if sum.DiscountedPubkeys == nil {
			sum.DiscountedPubkeys = make(map[string]struct{})
		}
		sum.DiscountedPubkeys[pubkey] = struct{}{}
		// The greeting pile adds greetingVote once, after this loop.
		if replier.greeting {
			continue
		}
		if isShortReplyCandidate(replier.phrase) {
			if weight > clusterVote[pileKey] {
				clusterVote[pileKey] = weight
			}
			continue
		}
		if weight > longVote[pileKey] {
			longVote[pileKey] = weight
		}
	}
	if greetingCount >= 2 {
		sum.Total += greetingVote
	}
	for _, vote := range clusterVote {
		sum.Total += vote
	}
	for _, vote := range longVote {
		sum.Total += vote
	}
	sum.UnlockShare = unlockShare(repliers, substantiveRepliers)
	return sum
}

// replyPileKey reports the pile a replier belongs to once at least two
// people said that phrase. A short phrase uses its near-copy cluster. A
// longer phrase uses the exact text. One person has no pile.
func replyPileKey(greeting bool, phrase string, greetingCount int, clusters map[string]string, clusterSize map[string]int, longCounts map[string]int) (string, bool) {
	if greeting {
		if greetingCount >= 2 {
			return "greeting", true
		}
		return "", false
	}
	if isShortReplyCandidate(phrase) {
		key, ok := clusters[phrase]
		if ok && clusterSize[key] >= 2 {
			return key, true
		}
		return "", false
	}
	if longCounts[phrase] >= 2 {
		return phrase, true
	}
	return "", false
}

// unlockShare is 1 while the copied pile is not most of the repliers.
// When it is, the share is the fraction of repliers outside the pile.
func unlockShare(repliers, substantiveRepliers int) float64 {
	if repliers <= 0 {
		return 1
	}
	pile := repliers - substantiveRepliers
	if pile <= substantiveRepliers {
		return 1
	}
	return float64(substantiveRepliers) / float64(repliers)
}

// repostWeightOutsidePile sums repost votes whose author is not already in a
// discounted reply pile. The reply counted that person; the repost is not a
// second vote.
func repostWeightOutsidePile(rows []weightedReply, pile map[string]struct{}) float64 {
	best := make(map[string]float64, len(rows))
	for _, row := range rows {
		if row.Pubkey == "" || row.Weight <= 0 {
			continue
		}
		if row.Weight > best[row.Pubkey] {
			best[row.Pubkey] = row.Weight
		}
	}
	total := 0.0
	for pubkey, weight := range best {
		if _, ok := pile[pubkey]; ok {
			continue
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

// shortReplyMaxWords lets a low-substance phrase pick up one extra word
// ("bitcoin is the money") without pulling in a longer remark.
const shortReplyMaxWords = lowSubstanceMaxWords + 1

// shortReplyMaxRunes matches that extra word. A five-word phrase still
// fits; a sentence does not.
const shortReplyMaxRunes = lowSubstanceMaxRunes + 16

func isShortReplyCandidate(phrase string) bool {
	if phrase == "" {
		return false
	}
	fields := strings.Fields(phrase)
	if len(fields) == 0 || len(fields) > shortReplyMaxWords {
		return false
	}
	return utf8.RuneCountInString(phrase) <= shortReplyMaxRunes
}

// shortPhraseClusterKeys maps each short phrase to the heaviest near-copy
// it should share a pile with. A phrase is a near-copy when the strings
// differ by one rune, or the words differ by one insertion or one swap.
// A neighbor of that phrase does not pull it any further.
func shortPhraseClusterKeys(counts map[string]int) map[string]string {
	phrases := make([]string, 0, len(counts))
	for phrase := range counts {
		phrases = append(phrases, phrase)
	}
	sort.Strings(phrases)
	assigned := make(map[string]string, len(phrases))
	for _, phrase := range phrases {
		best := phrase
		bestCount := counts[phrase]
		bestLen := utf8.RuneCountInString(phrase)
		for _, other := range phrases {
			if other == phrase || !nearShortPhrase(phrase, other) {
				continue
			}
			count := counts[other]
			length := utf8.RuneCountInString(other)
			if count > bestCount || (count == bestCount && (length < bestLen || (length == bestLen && other < best))) {
				best = other
				bestCount = count
				bestLen = length
			}
		}
		assigned[phrase] = best
	}
	return assigned
}

func nearShortPhrase(a, b string) bool {
	if a == b {
		return true
	}
	if withinOneRune(a, b) {
		return true
	}
	return oneTokenEdit(strings.Fields(a), strings.Fields(b))
}

func withinOneRune(a, b string) bool {
	ar := []rune(a)
	br := []rune(b)
	if len(ar) < len(br) {
		ar, br = br, ar
	}
	if len(ar)-len(br) > 1 || len(br) < 3 {
		return false
	}
	if len(ar) == len(br) {
		diff := 0
		for i := range ar {
			if ar[i] != br[i] {
				diff++
				if diff > 1 {
					return false
				}
			}
		}
		return diff == 1
	}
	i := 0
	skipped := false
	for _, r := range ar {
		if i < len(br) && r == br[i] {
			i++
			continue
		}
		if skipped {
			return false
		}
		skipped = true
	}
	return skipped && i == len(br)
}

func oneTokenEdit(a, b []string) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	if len(a) == len(b) {
		if len(a) < 2 {
			return false
		}
		diff := 0
		for i := range a {
			if a[i] != b[i] {
				diff++
				if diff > 1 {
					return false
				}
			}
		}
		return diff == 1
	}
	if len(a) > len(b) {
		a, b = b, a
	}
	if len(b) != len(a)+1 {
		return false
	}
	i := 0
	skipped := false
	for _, tok := range b {
		if i < len(a) && tok == a[i] {
			i++
			continue
		}
		if skipped {
			return false
		}
		skipped = true
	}
	return i == len(a) && skipped
}
