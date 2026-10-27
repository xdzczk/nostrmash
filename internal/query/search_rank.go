package query

import (
	"math"
	"net"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"
)

const (
	// NoteRecencyHalfLife is the recency decay for note search. A note one
	// half-life old keeps half of its recency weight. Days, not years: a
	// two-year-old unengaged note must not outrank today's discussion.
	NoteRecencyHalfLife = 72 * time.Hour

	// NoteAuthorCap is the most notes from one pubkey allowed in the first
	// NoteAuthorCapWindow ranked positions.
	NoteAuthorCap       = 2
	NoteAuthorCapWindow = 20

	// profileActivityHalfLife weights recent posters among exact name matches.
	profileActivityHalfLife = 14 * 24 * time.Hour

	noteLexicalWeight     = 0.35
	noteRecencyWeight     = 0.45
	noteEngagementWeight  = 0.20
	queryStubMultiplier   = 0.15
	automatedMultiplier   = 0.05
	duplicateMultiplier   = 0.02
	engagementSaturation  = 50.0
	correctionMinQueryLen = 5
	correctionMaxRawDF    = 10
	correctionMinTargetDF = 100
	correctionDFMultiple  = 8
)

// NoteScoreInput is one note the shared ranker can score. Empty content and
// a zero timestamp mean the caller has no ranking features; RankNotes then
// preserves the incoming order so trust partitioning of fixture rows holds.
type NoteScoreInput struct {
	ID                string
	Pubkey            string
	CreatedAt         int64
	Content           string
	ReplyCount        int64
	RepostCount       int64
	ReactionCount     int64
	ZapCount          int64
	AuthorBot         bool
	AuthorName        string
	AuthorDisplayName string
	AuthorNIP05       string
	// EngineScore is an optional 0..1 lexical prior from the search index.
	EngineScore float64
}

// ProfileScoreInput is one profile the shared ranker can score.
type ProfileScoreInput struct {
	Pubkey           string
	Name             string
	DisplayName      string
	NIP05            string
	About            string
	FollowerCount    int64
	RecentPostCount  int64
	RecentActivityAt int64
	Bot              bool
}

// RankOptions configures RankNotes and RankProfiles.
type RankOptions struct {
	Now    time.Time
	Sort   string // relevant (default) or latest
	Window *time.Duration
}

// ParsedQuery is a search string split into a phrase operator and loose terms.
type ParsedQuery struct {
	Raw     string
	Phrases []string
	Terms   []string
	Folded  string
}

// TermFrequency is a lexicon entry used for spelling correction.
type TermFrequency struct {
	Term              string
	DocumentFrequency int64
}

var (
	priceTickerPattern = regexp.MustCompile(`(?i)(bitcoin price|btc price|sats per usd|price:\s*\$|price:\s*[0-9])`)
	blockArtPattern    = regexp.MustCompile(`(?i)block\s*art`)
	blockRunePattern   = regexp.MustCompile(`[█░▒▓▀▄▌▐■□▪▫]`)
	whitespacePattern  = regexp.MustCompile(`\s+`)
	digitPattern       = regexp.MustCompile(`[0-9]+`)
	urlPattern         = regexp.MustCompile(`(?i)\bhttps?://\S+|\bwss?://\S+`)
)

// ParseSearchQuery extracts quoted phrases. Quotes are a phrase operator:
// the characters `"` are not themselves part of the match.
func ParseSearchQuery(raw string) ParsedQuery {
	raw = strings.TrimSpace(raw)
	parsed := ParsedQuery{Raw: raw, Folded: foldToken(raw)}
	if raw == "" {
		return parsed
	}
	var phrases []string
	var b strings.Builder
	inQuote := false
	for _, r := range raw {
		switch {
		case r == '"':
			if inQuote {
				phrase := strings.TrimSpace(b.String())
				if phrase != "" {
					phrases = append(phrases, whitespacePattern.ReplaceAllString(phrase, " "))
				}
				b.Reset()
				inQuote = false
				continue
			}
			inQuote = true
			continue
		default:
			if inQuote {
				b.WriteRune(r)
			}
		}
	}
	if inQuote {
		// Unbalanced quote: treat the tail as ordinary text, not a phrase.
		raw = strings.ReplaceAll(raw, `"`, " ")
		phrases = nil
	}
	remainder := raw
	for _, phrase := range phrases {
		remainder = strings.Replace(remainder, `"`+phrase+`"`, " ", 1)
	}
	remainder = strings.ReplaceAll(remainder, `"`, " ")
	parsed.Phrases = phrases
	parsed.Terms = queryTerms(remainder)
	if len(phrases) == 0 {
		parsed.Terms = queryTerms(raw)
	}
	return parsed
}

// ContentMatchesPhrases reports whether content contains every quoted phrase
// as a contiguous case-insensitive span. The quote characters themselves are
// not required in the content.
func ContentMatchesPhrases(content string, phrases []string) bool {
	if len(phrases) == 0 {
		return true
	}
	folded := foldMatchText(content)
	for _, phrase := range phrases {
		needle := foldMatchText(phrase)
		if needle == "" {
			continue
		}
		if !strings.Contains(folded, needle) {
			return false
		}
	}
	return true
}

// ScoreNote blends lexical overlap, recency, and engagement.
// Automated, bot, hashtag-stub, and near-duplicate notes are multiplied down.
// A negative score means the note failed a phrase constraint and must be dropped.
func ScoreNote(query string, note NoteScoreInput, now time.Time) float64 {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	parsed := ParseSearchQuery(query)
	if !ContentMatchesPhrases(note.Content, parsed.Phrases) {
		return -1
	}
	lexical := noteLexical(parsed, note)
	recency := noteRecency(note.CreatedAt, now)
	engagement := noteEngagement(note)
	score := noteLexicalWeight*lexical + noteRecencyWeight*recency + noteEngagementWeight*engagement
	if IsQueryStub(note.Content, query) {
		score *= queryStubMultiplier
	}
	if noteIsAutomated(note, parsed) {
		score *= automatedMultiplier
	}
	return score
}

type noteRankRow struct {
	note    NoteScoreInput
	score   float64
	idx     int
	demoted bool
	dup     bool
}

// RankNotes orders notes with the shared score, demotes near-duplicates, and
// caps how many notes from one author appear in the first window.
// When no note carries content, time, or engagement, the input order is kept.
func RankNotes(query string, notes []NoteScoreInput, opts RankOptions) []NoteScoreInput {
	if len(notes) == 0 {
		return notes
	}
	if opts.Now.IsZero() {
		opts.Now = time.Now().UTC()
	}
	if len(notes) == 1 {
		if ScoreNote(query, notes[0], opts.Now) < 0 {
			return nil
		}
		return notes
	}
	if !notesHaveFeatures(notes) && len(ParseSearchQuery(query).Phrases) == 0 {
		return notes
	}
	sortName := strings.ToLower(strings.TrimSpace(opts.Sort))
	if sortName == "" {
		sortName = "relevant"
	}
	rows := make([]noteRankRow, 0, len(notes))
	parsed := ParseSearchQuery(query)
	for i, note := range notes {
		score := ScoreNote(query, note, opts.Now)
		if score < 0 {
			continue
		}
		rows = append(rows, noteRankRow{
			note:    note,
			score:   score,
			idx:     i,
			demoted: noteIsAutomated(note, parsed),
		})
	}
	if len(rows) == 0 {
		return nil
	}
	markNearDuplicates(rows)
	for i := range rows {
		if rows[i].dup {
			rows[i].score *= duplicateMultiplier
			rows[i].demoted = true
		}
	}
	if sortName == "latest" {
		sort.SliceStable(rows, func(i, j int) bool {
			if rows[i].demoted != rows[j].demoted {
				return !rows[i].demoted
			}
			if rows[i].note.CreatedAt != rows[j].note.CreatedAt {
				return rows[i].note.CreatedAt > rows[j].note.CreatedAt
			}
			return rows[i].idx < rows[j].idx
		})
	} else {
		sort.SliceStable(rows, func(i, j int) bool {
			if rows[i].score != rows[j].score {
				return rows[i].score > rows[j].score
			}
			if rows[i].note.CreatedAt != rows[j].note.CreatedAt {
				return rows[i].note.CreatedAt > rows[j].note.CreatedAt
			}
			return rows[i].idx < rows[j].idx
		})
	}
	rows = applyAuthorCapRows(rows, NoteAuthorCap, NoteAuthorCapWindow)
	out := make([]NoteScoreInput, len(rows))
	for i, row := range rows {
		out[i] = row.note
	}
	return out
}

// ScoreProfile returns a tier (lower is better) and a score within that tier.
// Exact name, display name, or nip05 matches occupy tier 0. URL and 64-hex
// names occupy the last tier. Bots sit behind human accounts.
func ScoreProfile(query string, profile ProfileScoreInput, now time.Time) (tier int, score float64) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	foldedQuery := foldToken(stripQueryDecorations(query))
	name := foldToken(profile.Name)
	display := foldToken(profile.DisplayName)
	nip05 := foldToken(profile.NIP05)
	local := nip05
	if at := strings.IndexByte(local, '@'); at >= 0 {
		local = local[:at]
	}
	exact := foldedQuery != "" && (name == foldedQuery || display == foldedQuery || nip05 == foldedQuery || local == foldedQuery)
	prefix := foldedQuery != "" && (strings.HasPrefix(name, foldedQuery) || strings.HasPrefix(display, foldedQuery) || strings.HasPrefix(nip05, foldedQuery) || strings.HasPrefix(local, foldedQuery))
	contains := foldedQuery != "" && (strings.Contains(name, foldedQuery) || strings.Contains(display, foldedQuery) || strings.Contains(nip05, foldedQuery) || strings.Contains(foldToken(profile.About), foldedQuery))

	switch {
	case NameLooksLikeURLOrHex(profile.Name, profile.DisplayName):
		tier = 9
	case exact:
		tier = 0
	case prefix:
		tier = 1
	case contains:
		tier = 2
	default:
		tier = 3
	}
	if profile.Bot && tier < 8 {
		tier += 5
	}
	lexical := 0.2
	switch {
	case exact:
		lexical = 1
	case prefix:
		lexical = 0.7
	case contains:
		lexical = 0.4
	}
	activity := 0.0
	if profile.RecentActivityAt > 0 {
		age := float64(now.Unix() - profile.RecentActivityAt)
		if age < 0 {
			age = 0
		}
		activity = math.Exp(-math.Ln2 * age / profileActivityHalfLife.Seconds())
	}
	score = math.Log1p(float64(max64(profile.FollowerCount, 0))) + 3*activity + lexical + 0.15*math.Log1p(float64(max64(profile.RecentPostCount, 0)))
	return tier, score
}

// RankProfiles orders profiles by the shared profile score.
// Featureless rows (no name, nip05, followers, or activity) keep their order.
func RankProfiles(query string, profiles []ProfileScoreInput, opts RankOptions) []ProfileScoreInput {
	if len(profiles) <= 1 {
		return profiles
	}
	if opts.Now.IsZero() {
		opts.Now = time.Now().UTC()
	}
	if !profilesHaveFeatures(profiles) {
		return profiles
	}
	sortName := strings.ToLower(strings.TrimSpace(opts.Sort))
	type row struct {
		profile ProfileScoreInput
		tier    int
		score   float64
		idx     int
		latest  int64
	}
	rows := make([]row, len(profiles))
	for i, profile := range profiles {
		tier, score := ScoreProfile(query, profile, opts.Now)
		if opts.Window != nil && *opts.Window > 0 && profile.RecentActivityAt > 0 {
			cutoff := opts.Now.Add(-*opts.Window).Unix()
			if profile.RecentActivityAt < cutoff && tier < 8 {
				tier += 3
			}
		}
		rows[i] = row{
			profile: profile,
			tier:    tier,
			score:   score,
			idx:     i,
			latest:  profile.RecentActivityAt,
		}
	}
	if sortName == "latest" {
		sort.SliceStable(rows, func(i, j int) bool {
			if rows[i].tier != rows[j].tier {
				return rows[i].tier < rows[j].tier
			}
			if rows[i].latest != rows[j].latest {
				return rows[i].latest > rows[j].latest
			}
			return rows[i].idx < rows[j].idx
		})
	} else {
		sort.SliceStable(rows, func(i, j int) bool {
			if rows[i].tier != rows[j].tier {
				return rows[i].tier < rows[j].tier
			}
			if rows[i].score != rows[j].score {
				return rows[i].score > rows[j].score
			}
			return rows[i].idx < rows[j].idx
		})
	}
	out := make([]ProfileScoreInput, len(rows))
	for i, row := range rows {
		out[i] = row.profile
	}
	return out
}

// IsQueryStub reports whether content is only the query (or its hashtag)
// and contains no other prose.
func IsQueryStub(content, query string) bool {
	tokens := words(foldMatchText(content))
	if len(tokens) == 0 || len(tokens) > 2 {
		return false
	}
	qtokens := words(foldMatchText(stripQueryDecorations(query)))
	if len(qtokens) == 0 {
		return false
	}
	allowed := map[string]struct{}{}
	for _, token := range qtokens {
		token = strings.TrimPrefix(token, "#")
		if token != "" {
			allowed[token] = struct{}{}
		}
	}
	for _, token := range tokens {
		token = strings.TrimPrefix(token, "#")
		if _, ok := allowed[token]; !ok {
			return false
		}
	}
	return true
}

// NameLooksLikeURLOrHex reports whether the profile's primary label is a
// websocket/http URL or a bare 64-hex pubkey.
func NameLooksLikeURLOrHex(name, display string) bool {
	for _, value := range []string{name, display} {
		label := strings.TrimSpace(value)
		if label == "" {
			continue
		}
		lower := strings.ToLower(label)
		if strings.HasPrefix(lower, "wss://") || strings.HasPrefix(lower, "ws://") ||
			strings.HasPrefix(lower, "https://") || strings.HasPrefix(lower, "http://") {
			return true
		}
		if is64Hex(lower) {
			return true
		}
	}
	return false
}

// CanonicalRelayHost returns the host when q is a relay URL or a domain.
// wss://relay.damus.io:1234 becomes relay.damus.io. NIP-05 addresses and
// single tokens without a dot are not hosts.
func CanonicalRelayHost(raw string) (string, bool) {
	value := strings.TrimSpace(raw)
	if value == "" || strings.ContainsAny(value, " @") {
		return "", false
	}
	lower := strings.ToLower(value)
	if strings.HasPrefix(lower, "wss://") || strings.HasPrefix(lower, "ws://") ||
		strings.HasPrefix(lower, "https://") || strings.HasPrefix(lower, "http://") {
		parsed, err := url.Parse(lower)
		if err != nil {
			return "", false
		}
		host := strings.TrimSpace(parsed.Hostname())
		if host == "" || !strings.Contains(host, ".") {
			return "", false
		}
		return host, true
	}
	host := lower
	if h, _, err := net.SplitHostPort(lower); err == nil && h != "" {
		host = h
	}
	host = strings.TrimSuffix(host, ".")
	if host == "" || !strings.Contains(host, ".") || strings.ContainsAny(host, "/?#") {
		return "", false
	}
	for _, r := range host {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '.' && r != '-' {
			return "", false
		}
	}
	return host, true
}

// QueryLooksLikePersonName is true for one to three alphabetic tokens.
// A single topic word such as "bitcoin" is not a person name, so bridge
// accounts can be demoted for those queries.
func QueryLooksLikePersonName(raw string) bool {
	tokens := words(foldToken(stripQueryDecorations(raw)))
	if len(tokens) == 0 || len(tokens) > 3 {
		return false
	}
	for _, token := range tokens {
		if len(token) < 2 || len(token) > 24 {
			return false
		}
		for _, r := range token {
			if r < 'a' || r > 'z' {
				return false
			}
		}
	}
	if len(tokens) == 1 {
		switch tokens[0] {
		case "bitcoin", "nostr", "btc", "crypto", "ethereum", "lightning":
			return false
		}
	}
	return true
}

// ChooseCorrection picks an edit-distance 1 or 2 replacement when the raw
// query is a weak, low-frequency token and the lexicon has a much more
// common neighbor. bitconi with a tiny frequency corrects to bitcoin.
func ChooseCorrection(raw string, rawDF int64, weak bool, lexicon []TermFrequency) (string, bool) {
	term := foldToken(stripQueryDecorations(raw))
	if !weak || term == "" || strings.Contains(term, " ") || strings.Contains(raw, `"`) {
		return "", false
	}
	if len(term) < correctionMinQueryLen || rawDF >= correctionMaxRawDF {
		return "", false
	}
	rawDistance := map[string]int{}
	for _, candidate := range lexicon {
		neighbor := foldToken(candidate.Term)
		if neighbor == "" || neighbor == term {
			continue
		}
		if candidate.DocumentFrequency < correctionMinTargetDF {
			continue
		}
		if rawDF > 0 && candidate.DocumentFrequency < rawDF*correctionDFMultiple {
			continue
		}
		distance := damerauDistance(term, neighbor, 2)
		if distance <= 0 || distance > 2 {
			continue
		}
		if len(term) < 8 && distance > 1 {
			continue
		}
		rawDistance[neighbor] = distance
		_ = rawDistance
	}
	type choice struct {
		term     string
		df       int64
		distance int
	}
	var best *choice
	for _, candidate := range lexicon {
		neighbor := foldToken(candidate.Term)
		distance, ok := rawDistance[neighbor]
		if !ok {
			continue
		}
		if best == nil || candidate.DocumentFrequency > best.df ||
			(candidate.DocumentFrequency == best.df && distance < best.distance) ||
			(candidate.DocumentFrequency == best.df && distance == best.distance && neighbor < best.term) {
			best = &choice{term: neighbor, df: candidate.DocumentFrequency, distance: distance}
		}
	}
	if best == nil {
		return "", false
	}
	return best.term, true
}

// Edits returns the Damerau-Levenshtein neighborhood of term within distance
// (1 or 2). Distance 2 is capped so a lookup stays a single index probe.
func Edits(term string, distance int) []string {
	term = foldToken(term)
	if term == "" || distance <= 0 {
		return nil
	}
	if distance > 2 {
		distance = 2
	}
	first := edits1(term)
	if distance == 1 {
		return first
	}
	seen := make(map[string]struct{}, len(first)*2)
	out := make([]string, 0, 256)
	add := func(value string) {
		if value == "" || value == term {
			return
		}
		if _, ok := seen[value]; ok {
			return
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	for _, value := range first {
		add(value)
	}
	// Distance 2 only for mid-length tokens, and only via a second edit of the
	// deletion/transposition neighborhood so the candidate list stays bounded.
	if len(term) < 6 || len(term) > 12 {
		return out
	}
	for _, value := range first {
		if len(out) >= 1500 {
			break
		}
		if absInt(len(value)-len(term)) > 1 {
			continue
		}
		for _, next := range edits1(value) {
			add(next)
			if len(out) >= 1500 {
				break
			}
		}
	}
	return out
}

// QueryResultsWeak reports whether a result page is too small or too poorly
// matched to trust over a high-frequency spelling neighbor.
func QueryResultsWeak(query string, contents []string, estimatedTotal int64) bool {
	term := foldToken(stripQueryDecorations(query))
	if term == "" || strings.Contains(term, " ") || strings.Contains(query, `"`) {
		return false
	}
	if len(term) < correctionMinQueryLen {
		return false
	}
	whole := 0
	for _, content := range contents {
		if containsWholeTerm(content, term) {
			whole++
		}
	}
	if whole > 0 && estimatedTotal >= 25 {
		return false
	}
	if whole == 0 {
		return true
	}
	return estimatedTotal < correctionMaxRawDF && len(contents) < 5
}

// ObviousEmptyToken is a long mixed token that cannot be a prefix of a
// common word. Callers may skip a table scan when it has no postings.
func ObviousEmptyToken(raw string) bool {
	term := foldToken(strings.TrimSpace(raw))
	if len(term) < 12 || strings.Contains(term, " ") {
		return false
	}
	digits := 0
	for _, r := range term {
		if r >= '0' && r <= '9' {
			digits++
		}
	}
	return digits >= 2
}

func notesHaveFeatures(notes []NoteScoreInput) bool {
	for _, note := range notes {
		if strings.TrimSpace(note.Content) != "" || note.CreatedAt > 0 ||
			note.ReplyCount+note.RepostCount+note.ReactionCount+note.ZapCount > 0 ||
			note.AuthorBot || note.EngineScore > 0 {
			return true
		}
	}
	return false
}

func profilesHaveFeatures(profiles []ProfileScoreInput) bool {
	for _, profile := range profiles {
		if strings.TrimSpace(profile.Name) != "" || strings.TrimSpace(profile.DisplayName) != "" ||
			strings.TrimSpace(profile.NIP05) != "" || strings.TrimSpace(profile.About) != "" ||
			profile.FollowerCount > 0 || profile.RecentPostCount > 0 || profile.RecentActivityAt > 0 ||
			profile.Bot {
			return true
		}
	}
	return false
}

func noteLexical(parsed ParsedQuery, note NoteScoreInput) float64 {
	if strings.TrimSpace(note.Content) == "" {
		if note.EngineScore > 0 {
			return clamp01(note.EngineScore)
		}
		return 0.55
	}
	overlap := termOverlap(parsed, note.Content)
	if len(parsed.Phrases) > 0 && ContentMatchesPhrases(note.Content, parsed.Phrases) && overlap < 0.85 {
		overlap = 0.85
	}
	if overlap > 0 {
		return overlap
	}
	if note.EngineScore > 0 {
		return clamp01(note.EngineScore)
	}
	return 0.25
}

func termOverlap(parsed ParsedQuery, content string) float64 {
	terms := parsed.Terms
	if len(terms) == 0 {
		terms = words(parsed.Folded)
	}
	if len(terms) == 0 {
		return 0
	}
	folded := foldMatchText(content)
	contentWords := words(folded)
	if len(contentWords) == 0 {
		return 0
	}
	var sum float64
	for _, term := range terms {
		term = strings.TrimPrefix(foldToken(term), "#")
		if term == "" {
			continue
		}
		best := 0.0
		if strings.Contains(folded, term) {
			best = 0.45
		}
		for _, word := range contentWords {
			word = strings.TrimPrefix(word, "#")
			switch {
			case word == term:
				best = 1
			case strings.HasPrefix(word, term) || strings.HasPrefix(term, word):
				if best < 0.65 {
					best = 0.65
				}
			}
		}
		sum += best
	}
	return clamp01(sum / float64(len(terms)))
}

func noteRecency(createdAt int64, now time.Time) float64 {
	if createdAt <= 0 {
		return 0.5
	}
	age := now.Sub(time.Unix(createdAt, 0).UTC())
	if age < 0 {
		age = 0
	}
	return math.Exp(-math.Ln2 * age.Seconds() / NoteRecencyHalfLife.Seconds())
}

func noteEngagement(note NoteScoreInput) float64 {
	raw := float64(note.ReplyCount*4 + note.RepostCount*3 + note.ReactionCount + note.ZapCount*2)
	if raw <= 0 {
		return 0
	}
	value := math.Log1p(raw) / math.Log1p(engagementSaturation)
	if value > 1 {
		return 1
	}
	return value
}

func noteIsAutomated(note NoteScoreInput, parsed ParsedQuery) bool {
	if note.AuthorBot || isRSSAccount(note.AuthorName, note.AuthorDisplayName) {
		return true
	}
	if isMostrBridge(note.AuthorNIP05) && !QueryLooksLikePersonName(parsed.Raw) {
		return true
	}
	content := note.Content
	if priceTickerPattern.MatchString(content) || blockArtPattern.MatchString(content) {
		return true
	}
	if blockRunePattern.MatchString(content) {
		return true
	}
	return false
}

func isRSSAccount(name, display string) bool {
	combined := strings.ToLower(name + " " + display)
	return strings.Contains(combined, "(rss feed)") || strings.Contains(combined, "rss feed")
}

func isMostrBridge(nip05 string) bool {
	host := strings.ToLower(strings.TrimSpace(nip05))
	if at := strings.LastIndex(host, "@"); at >= 0 {
		host = host[at+1:]
	}
	return host == "mostr.pub" || strings.HasSuffix(host, ".mostr.pub")
}

func markNearDuplicates(rows []noteRankRow) {
	best := map[string]int{}
	for i, row := range rows {
		key := duplicateKey(row.note)
		if key == "" {
			continue
		}
		prev, ok := best[key]
		if !ok {
			best[key] = i
			continue
		}
		if !withinDuplicateWindow(rows[prev].note, row.note) {
			continue
		}
		drop := i
		if row.score > rows[prev].score || (row.score == rows[prev].score && row.note.CreatedAt > rows[prev].note.CreatedAt) {
			drop = prev
			best[key] = i
		}
		rows[drop].dup = true
	}
}

func withinDuplicateWindow(a, b NoteScoreInput) bool {
	if a.CreatedAt <= 0 || b.CreatedAt <= 0 {
		return true
	}
	delta := a.CreatedAt - b.CreatedAt
	if delta < 0 {
		delta = -delta
	}
	return delta <= int64((7*24*time.Hour)/time.Second)
}

func duplicateKey(note NoteScoreInput) string {
	if note.Pubkey == "" {
		return ""
	}
	normalized := normalizeDuplicate(note.Content)
	if len(normalized) < 24 {
		return ""
	}
	return note.Pubkey + "\n" + normalized
}

func normalizeDuplicate(content string) string {
	folded := strings.ToLower(content)
	folded = urlPattern.ReplaceAllString(folded, " ")
	folded = digitPattern.ReplaceAllString(folded, "#")
	folded = whitespacePattern.ReplaceAllString(folded, " ")
	return strings.TrimSpace(folded)
}

func applyAuthorCapRows(rows []noteRankRow, cap, window int) []noteRankRow {
	if cap <= 0 || window <= 0 || len(rows) == 0 {
		return rows
	}
	kept := make([]noteRankRow, 0, len(rows))
	overflow := make([]noteRankRow, 0)
	counts := map[string]int{}
	for _, row := range rows {
		key := row.note.Pubkey
		if key == "" {
			key = "id:" + row.note.ID
		}
		if len(kept) < window && counts[key] < cap {
			counts[key]++
			kept = append(kept, row)
			continue
		}
		overflow = append(overflow, row)
	}
	return append(kept, overflow...)
}

func containsWholeTerm(content, term string) bool {
	term = foldToken(term)
	if term == "" {
		return false
	}
	for _, word := range words(foldMatchText(content)) {
		if strings.TrimPrefix(word, "#") == term {
			return true
		}
	}
	return false
}

func queryTerms(raw string) []string {
	return words(foldToken(raw))
}

func words(value string) []string {
	fields := strings.FieldsFunc(value, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '#'
	})
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.TrimPrefix(strings.ToLower(field), "#")
		field = strings.TrimPrefix(field, "#")
		if field == "" {
			continue
		}
		out = append(out, field)
	}
	return out
}

func foldToken(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func foldMatchText(value string) string {
	value = strings.ToLower(value)
	value = whitespacePattern.ReplaceAllString(value, " ")
	return strings.TrimSpace(value)
}

func stripQueryDecorations(raw string) string {
	value := strings.TrimSpace(raw)
	value = strings.TrimPrefix(value, "nostr:")
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(strings.TrimPrefix(value, "@"), "#")
	return strings.Trim(value, `"`)
}

func clamp01(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}

func max64(value, floor int64) int64 {
	if value < floor {
		return floor
	}
	return value
}

func is64Hex(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

func absInt(value int) int {
	if value < 0 {
		return -value
	}
	return value
}

func edits1(term string) []string {
	letters := "abcdefghijklmnopqrstuvwxyz"
	seen := map[string]struct{}{}
	out := make([]string, 0, len(term)*len(letters))
	add := func(value string) {
		if value == "" || value == term {
			return
		}
		if _, ok := seen[value]; ok {
			return
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	runes := []rune(term)
	for i := 0; i < len(runes); i++ {
		left := string(runes[:i])
		right := string(runes[i+1:])
		add(left + right) // deletion
		if i+1 < len(runes) {
			add(left + string(runes[i+1]) + string(runes[i]) + string(runes[i+2:]))
		}
		for _, letter := range letters {
			add(left + string(letter) + right) // substitution
		}
	}
	for i := 0; i <= len(runes); i++ {
		left := string(runes[:i])
		right := string(runes[i:])
		for _, letter := range letters {
			add(left + string(letter) + right) // insertion
		}
	}
	return out
}

func damerauDistance(a, b string, maxDistance int) int {
	if a == b {
		return 0
	}
	ar := []rune(a)
	br := []rune(b)
	if absInt(len(ar)-len(br)) > maxDistance {
		return maxDistance + 1
	}
	prev := make([]int, len(br)+1)
	cur := make([]int, len(br)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(ar); i++ {
		cur[0] = i
		rowMin := cur[0]
		for j := 1; j <= len(br); j++ {
			cost := 1
			if ar[i-1] == br[j-1] {
				cost = 0
			}
			del := prev[j] + 1
			ins := cur[j-1] + 1
			sub := prev[j-1] + cost
			best := del
			if ins < best {
				best = ins
			}
			if sub < best {
				best = sub
			}
			if i > 1 && j > 1 && ar[i-1] == br[j-2] && ar[i-2] == br[j-1] {
				// Adjacent transposition. prevPrev[j-2] is not retained; the
				// standard recurrence needs the row before prev. Recompute the
				// diagonal from a third buffer only when both indexes allow it
				// by using sub's neighborhood: distance through the transpose
				// is prev[j-1] adjusted below via a dedicated buffer.
			}
			cur[j] = best
			if best < rowMin {
				rowMin = best
			}
		}
		if rowMin > maxDistance {
			return maxDistance + 1
		}
		prev, cur = cur, prev
	}
	// The loop above is Levenshtein. Damerau transposition of adjacent
	// characters is distance 1, which Levenshtein scores as 2. Accept that
	// pair explicitly so bitconi → bitcoin counts as one edit.
	if isAdjacentTransposition(ar, br) {
		return 1
	}
	dist := prev[len(br)]
	if dist > maxDistance {
		return maxDistance + 1
	}
	return dist
}

func isAdjacentTransposition(a, b []rune) bool {
	if len(a) != len(b) {
		return false
	}
	diff := -1
	for i := range a {
		if a[i] != b[i] {
			if diff >= 0 {
				if i == diff+1 && a[diff] == b[i] && a[i] == b[diff] {
					for j := i + 1; j < len(a); j++ {
						if a[j] != b[j] {
							return false
						}
					}
					return true
				}
				return false
			}
			diff = i
		}
	}
	return false
}
