package derivation

import (
	"os"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

const (
	defaultLanguageMinChars      = 20
	defaultLanguageMinConfidence = 0.60
)

var languageTokenPattern = regexp.MustCompile(`[\p{L}\p{N}']+`)

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

func detectPrimaryLanguage(content string) (*string, *float64) {
	cfg := readLanguageDetectionConfig()
	if !cfg.Enabled {
		return nil, nil
	}
	return detectPrimaryLanguageWithConfig(content, cfg)
}

func detectPrimaryLanguageWithConfig(content string, cfg languageDetectionConfig) (*string, *float64) {
	if !cfg.Enabled {
		return nil, nil
	}
	trimmed := strings.TrimSpace(content)
	if trimmed == "" {
		return nil, nil
	}
	if runeCount(trimmed) < cfg.MinChars {
		return nil, nil
	}
	if lang, confidence, ok := detectByScript(trimmed); ok && confidence >= cfg.MinConfidence {
		return strPtr(lang), floatPtr(confidence)
	}

	tokens := normalizeTokens(trimmed)
	if len(tokens) < 3 {
		return nil, nil
	}
	scores := scoreLatinLanguageTokens(tokens)
	bestLang := ""
	bestScore := 0
	totalScore := 0
	for lang, score := range scores {
		totalScore += score
		if score > bestScore {
			bestLang = lang
			bestScore = score
		}
	}
	if bestLang == "" || bestScore < 2 || totalScore == 0 {
		return nil, nil
	}
	confidence := float64(bestScore) / float64(totalScore)
	if confidence < cfg.MinConfidence {
		return nil, nil
	}
	return strPtr(bestLang), floatPtr(confidence)
}

func detectByScript(content string) (string, float64, bool) {
	var hasCyrillic bool
	var hasHan bool
	var hasHiraganaKatakana bool
	for _, r := range content {
		switch {
		case unicode.In(r, unicode.Cyrillic):
			hasCyrillic = true
		case unicode.In(r, unicode.Han):
			hasHan = true
		case unicode.In(r, unicode.Hiragana, unicode.Katakana):
			hasHiraganaKatakana = true
		}
	}
	if hasHiraganaKatakana {
		return "ja", 0.99, true
	}
	if hasHan {
		return "zh", 0.95, true
	}
	if hasCyrillic {
		return "ru", 0.95, true
	}
	return "", 0, false
}

func normalizeTokens(content string) []string {
	matches := languageTokenPattern.FindAllString(strings.ToLower(content), -1)
	out := make([]string, 0, len(matches))
	for _, match := range matches {
		token := strings.Trim(match, "'")
		if token != "" {
			out = append(out, token)
		}
	}
	return out
}

func scoreLatinLanguageTokens(tokens []string) map[string]int {
	dictionaries := map[string]map[string]struct{}{
		"en": languageWordsEN,
		"es": languageWordsES,
		"fr": languageWordsFR,
		"de": languageWordsDE,
		"pt": languageWordsPT,
	}
	scores := map[string]int{
		"en": 0,
		"es": 0,
		"fr": 0,
		"de": 0,
		"pt": 0,
	}
	for _, token := range tokens {
		for lang, words := range dictionaries {
			if _, ok := words[token]; ok {
				scores[lang]++
			}
		}
	}
	return scores
}

func runeCount(value string) int {
	count := 0
	for range value {
		count++
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

var languageWordsEN = map[string]struct{}{
	"the": {}, "and": {}, "for": {}, "that": {}, "with": {}, "this": {}, "you": {}, "from": {},
	"have": {}, "your": {}, "just": {}, "about": {}, "what": {}, "when": {}, "where": {}, "there": {},
	"will": {}, "would": {}, "not": {}, "are": {}, "was": {}, "were": {},
}

var languageWordsES = map[string]struct{}{
	"que": {}, "para": {}, "con": {}, "una": {}, "este": {}, "esta": {}, "como": {}, "pero": {},
	"por": {}, "las": {}, "los": {}, "del": {}, "sin": {}, "hola": {}, "gracias": {}, "porque": {},
	"cuando": {}, "donde": {}, "estoy": {}, "tengo": {}, "muy": {}, "tambien": {},
}

var languageWordsFR = map[string]struct{}{
	"que": {}, "pour": {}, "avec": {}, "dans": {}, "une": {}, "est": {}, "pas": {}, "vous": {},
	"nous": {}, "bonjour": {}, "merci": {}, "comme": {}, "mais": {}, "des": {}, "les": {}, "sur": {},
	"tout": {}, "plus": {}, "cela": {}, "etre": {}, "avoir": {}, "leur": {},
}

var languageWordsDE = map[string]struct{}{
	"und": {}, "der": {}, "die": {}, "das": {}, "mit": {}, "ich": {}, "nicht": {}, "ein": {},
	"eine": {}, "ist": {}, "fuer": {}, "auf": {}, "von": {}, "wir": {}, "sie": {}, "danke": {},
	"hallo": {}, "aber": {}, "wenn": {}, "auch": {}, "wie": {}, "was": {},
}

var languageWordsPT = map[string]struct{}{
	"que": {}, "para": {}, "com": {}, "uma": {}, "este": {}, "esta": {}, "como": {}, "mas": {},
	"por": {}, "dos": {}, "das": {}, "sem": {}, "ola": {}, "obrigado": {}, "porque": {}, "quando": {},
	"onde": {}, "estou": {}, "tenho": {}, "muito": {}, "tambem": {}, "voce": {},
}
