package derivation

import (
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"
	"unicode/utf8"

	"github.com/pemistahl/lingua-go"
)

const (
	defaultLanguageMinChars      = 20
	defaultLanguageMinConfidence = 0.60
	// languageMinRelativeDistance is the minimum gap between the top two
	// language probabilities. Those probabilities sum to 1 across the
	// configured set, so a near-tie can still post a moderate top score.
	languageMinRelativeDistance = 0.25
	// languageLowAccuracyMinRunes is lingua's own cutoff: below this, low
	// accuracy mode (trigrams only) is unreliable, so short notes use the
	// full n-gram models. Longer notes do not need them.
	languageLowAccuracyMinRunes = 120
	// languageInferredConfidence stays under the default content threshold so
	// a thread or author fallback is not treated as a direct detection.
	languageInferredConfidence  = 0.40
	languageAuthorPriorWindow   = 30 * 24 * 60 * 60
	languageAuthorPriorMinNotes = 3

	languageSourceContent   = "content"
	languageSourceTag       = "tag"
	languageSourceThread    = "thread"
	languageSourceAuthor    = "author"
	languageSourceShort     = "short"
	languageSourceUncertain = "uncertain"
)

// noteLanguages is the closed set the detector may return. Near-neighbors
// are included (Malay with Indonesian, Urdu with Arabic and Persian,
// Ukrainian with Russian) so a note is not forced into the adjacent
// language. Models stay resident after first use. The worker preloads them
// so the first notes of a process do not pay the unzip.
var noteLanguages = []lingua.Language{
	lingua.Arabic,
	lingua.Bengali,
	lingua.Bokmal,
	lingua.Bulgarian,
	lingua.Chinese,
	lingua.Czech,
	lingua.Danish,
	lingua.Dutch,
	lingua.English,
	lingua.Finnish,
	lingua.French,
	lingua.German,
	lingua.Greek,
	lingua.Hebrew,
	lingua.Hindi,
	lingua.Hungarian,
	lingua.Indonesian,
	lingua.Italian,
	lingua.Japanese,
	lingua.Korean,
	lingua.Malay,
	lingua.Marathi,
	lingua.Persian,
	lingua.Polish,
	lingua.Portuguese,
	lingua.Romanian,
	lingua.Russian,
	lingua.Serbian,
	lingua.Spanish,
	lingua.Swedish,
	lingua.Thai,
	lingua.Turkish,
	lingua.Ukrainian,
	lingua.Urdu,
	lingua.Vietnamese,
}

var (
	languagePreloadModels atomic.Bool

	languageDetectorOnce    sync.Once
	languageDetector        lingua.LanguageDetector
	languageDetectorLowOnce sync.Once
	languageDetectorLow     lingua.LanguageDetector

	// Opaque tokens that carry no language signal. URLs are removed entirely:
	// path segments such as "the" and "and" would otherwise look like English.
	// The bech32 and hex patterns match the tokens stripped before search indexing.
	languageURLPattern     = regexp.MustCompile(`(?i)\b(?:https?|wss?)://\S+`)
	languageBech32Pattern  = regexp.MustCompile(`(?i)\b(?:nostr:)?(?:npub|nsec|note|nevent|nprofile|naddr|nrelay|lnbc|lntb|lnbcrt)1[02-9ac-hj-np-z]{20,}\b`)
	languageHexPattern     = regexp.MustCompile(`\b[0-9a-fA-F]{32,}\b`)
	languageMentionPattern = regexp.MustCompile(`#\[\d+\]`)
	// ASCII-only. Thai, Chinese, and Japanese notes are often written
	// without spaces, so a generic non-whitespace run would delete them.
	languageLongRunPattern    = regexp.MustCompile(`[A-Za-z0-9+/=_-]{48,}`)
	languageWhitespacePattern = regexp.MustCompile(`\s+`)
	languageCodePattern       = regexp.MustCompile(`^[a-z]{2}$`)
)

type languageDetectionConfig struct {
	Enabled       bool
	MinChars      int
	MinConfidence float64
}

func readLanguageDetectionConfig() languageDetectionConfig {
	minChars := defaultLanguageMinChars
	if raw := strings.TrimSpace(os.Getenv("NOSTRMASH_LANGUAGE_DETECTION_MIN_CHARS")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			minChars = parsed
		}
	}
	minConfidence := defaultLanguageMinConfidence
	if raw := strings.TrimSpace(os.Getenv("NOSTRMASH_LANGUAGE_DETECTION_MIN_CONFIDENCE")); raw != "" {
		if parsed, err := strconv.ParseFloat(raw, 64); err == nil && parsed > 0 && parsed <= 1 {
			minConfidence = parsed
		}
	}
	return languageDetectionConfig{
		Enabled:       getEnvBoolOrDefault("NOSTRMASH_LANGUAGE_DETECTION_ENABLED", true),
		MinChars:      minChars,
		MinConfidence: minConfidence,
	}
}

// EnableLanguageModelPreload makes the first detector build load every
// configured model. Call it before notes are classified.
func EnableLanguageModelPreload() {
	languagePreloadModels.Store(true)
}

// PreloadLanguageDetectors loads the high-accuracy models used for short
// notes and the trigram models used for longer ones.
func PreloadLanguageDetectors() {
	EnableLanguageModelPreload()
	_ = noteLanguageDetector(false)
	_ = noteLanguageDetector(true)
}

func noteLanguageDetector(lowAccuracy bool) lingua.LanguageDetector {
	if lowAccuracy {
		languageDetectorLowOnce.Do(func() {
			languageDetectorLow = buildLanguageDetector(true)
		})
		return languageDetectorLow
	}
	languageDetectorOnce.Do(func() {
		languageDetector = buildLanguageDetector(false)
	})
	return languageDetector
}

func buildLanguageDetector(lowAccuracy bool) lingua.LanguageDetector {
	builder := lingua.NewLanguageDetectorBuilder().FromLanguages(noteLanguages...)
	if lowAccuracy {
		builder = builder.WithLowAccuracyMode()
	}
	if languagePreloadModels.Load() {
		builder = builder.WithPreloadedLanguageModels()
	}
	return builder.Build()
}

func detectPrimaryLanguageWithConfig(content string, cfg languageDetectionConfig) (*string, *float64) {
	if !cfg.Enabled {
		return nil, nil
	}
	cleaned := cleanNoteTextForLanguage(content)
	if cleaned == "" || letterCount(cleaned) < cfg.MinChars {
		return nil, nil
	}
	lang, conf, ok := detectCleanedLanguage(cleaned, cfg)
	if !ok {
		return nil, nil
	}
	return lang, conf
}

func detectCleanedLanguage(cleaned string, cfg languageDetectionConfig) (*string, *float64, bool) {
	lowAccuracy := utf8.RuneCountInString(cleaned) >= languageLowAccuracyMinRunes
	values := noteLanguageDetector(lowAccuracy).ComputeLanguageConfidenceValues(cleaned)
	if len(values) < 2 {
		return nil, nil, false
	}
	top := values[0]
	second := values[1]
	confidence := top.Value()
	if confidence < cfg.MinConfidence || confidence-second.Value() < languageMinRelativeDistance {
		return nil, nil, false
	}
	code := strings.ToLower(top.Language().IsoCode639_1().String())
	if len(code) != 2 {
		return nil, nil, false
	}
	return strPtr(code), floatPtr(confidence), true
}

func cleanNoteTextForLanguage(content string) string {
	s := strings.TrimSpace(content)
	if s == "" {
		return ""
	}
	s = languageURLPattern.ReplaceAllString(s, " ")
	s = languageBech32Pattern.ReplaceAllString(s, " ")
	s = languageHexPattern.ReplaceAllString(s, " ")
	s = languageMentionPattern.ReplaceAllString(s, " ")
	s = languageLongRunPattern.ReplaceAllString(s, " ")
	return strings.TrimSpace(languageWhitespacePattern.ReplaceAllString(s, " "))
}

func letterCount(value string) int {
	count := 0
	for _, r := range value {
		if unicode.IsLetter(r) {
			count++
		}
	}
	return count
}

func strPtr(value string) *string {
	out := value
	return &out
}

func floatPtr(value float64) *float64 {
	out := value
	return &out
}

func explicitNoteLanguage(tags [][]string) (string, bool) {
	for _, tag := range tags {
		if len(tag) < 2 {
			continue
		}
		name := strings.ToLower(strings.TrimSpace(tag[0]))
		value := strings.ToLower(strings.TrimSpace(tag[1]))
		switch name {
		case "l":
			if len(tag) >= 3 && !languageTagNamespaceOK(tag[2]) {
				continue
			}
			if code, ok := normalizeLanguageCode(value); ok {
				return code, true
			}
		case "lang":
			if code, ok := normalizeLanguageCode(value); ok {
				return code, true
			}
		}
	}
	return "", false
}

func languageTagNamespaceOK(namespace string) bool {
	compact := strings.NewReplacer(" ", "", "-", "", "_", "").Replace(strings.ToLower(strings.TrimSpace(namespace)))
	return compact == "" || compact == "iso6391"
}

func normalizeLanguageCode(value string) (string, bool) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "und" || !languageCodePattern.MatchString(value) {
		return "", false
	}
	return value, true
}

type authorLanguageCount struct {
	language string
	count    int64
}

func dominantAuthorLanguage(rows []authorLanguageCount) *string {
	if len(rows) == 0 || rows[0].count < languageAuthorPriorMinNotes {
		return nil
	}
	if len(rows) > 1 && rows[0].count < rows[1].count*2 {
		return nil
	}
	lang := rows[0].language
	return &lang
}

// decideNoteLanguage applies the label order: explicit tag, content
// detection, parent or root note, then the author's own content language.
func decideNoteLanguage(tag string, tagOK bool, contentLang *string, contentConf *float64, letters int, threadLang, authorLang *string, minChars int) (*string, *float64, string) {
	if tagOK {
		return strPtr(tag), floatPtr(1), languageSourceTag
	}
	if contentLang != nil {
		return contentLang, contentConf, languageSourceContent
	}
	if threadLang != nil && strings.TrimSpace(*threadLang) != "" {
		return threadLang, floatPtr(languageInferredConfidence), languageSourceThread
	}
	if authorLang != nil && strings.TrimSpace(*authorLang) != "" {
		return authorLang, floatPtr(languageInferredConfidence), languageSourceAuthor
	}
	if letters < minChars {
		return nil, nil, languageSourceShort
	}
	return nil, nil, languageSourceUncertain
}

// languageDecisionSettled reports a label the current detector already
// accepted. language_source is written only by that detector, so the row's
// derivation_version does not matter: production keeps an older active
// version until a full rebuild, and a settled label must survive an
// engagement refresh in the meantime. version is retained for callers.
func languageDecisionSettled(_ *int32, source *string, lang *string) bool {
	if lang == nil || strings.TrimSpace(*lang) == "" || source == nil {
		return false
	}
	switch *source {
	case languageSourceContent, languageSourceTag, languageSourceThread, languageSourceAuthor:
		return true
	default:
		return false
	}
}

// languageDecisionRetryThread is the cheap path for a note already marked
// short or uncertain: look for a parent or root label and otherwise keep
// the existing source. A null source still takes the full detector.
func languageDecisionRetryThread(_ *int32, source *string) bool {
	if source == nil {
		return false
	}
	return *source == languageSourceShort || *source == languageSourceUncertain
}

func getEnvBoolOrDefault(key string, defaultValue bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return defaultValue
	}
	parsed, err := strconv.ParseBool(raw)
	if err != nil {
		return defaultValue
	}
	return parsed
}
