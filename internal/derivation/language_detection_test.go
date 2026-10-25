package derivation

import (
	"strings"
	"testing"
)

func testLangConfig() languageDetectionConfig {
	return languageDetectionConfig{Enabled: true, MinChars: defaultLanguageMinChars, MinConfidence: defaultLanguageMinConfidence}
}

func TestDetectPrimaryLanguageWithConfig_Disabled(t *testing.T) {
	cfg := testLangConfig()
	cfg.Enabled = false
	lang, conf := detectPrimaryLanguageWithConfig("the quick brown fox jumps over lazy dogs", cfg)
	if lang != nil || conf != nil {
		t.Fatalf("disabled detector must return nil,nil; got %v %v", lang, conf)
	}
}

func TestDetectPrimaryLanguageWithConfig_TooShort(t *testing.T) {
	cfg := testLangConfig()
	if lang, conf := detectPrimaryLanguageWithConfig("hi there", cfg); lang != nil || conf != nil {
		t.Fatalf("content under MinChars must be nil; got %v %v", lang, conf)
	}
	if lang, conf := detectPrimaryLanguageWithConfig("       ", cfg); lang != nil || conf != nil {
		t.Fatalf("blank content must be nil; got %v %v", lang, conf)
	}
}

func TestDetectPrimaryLanguageWithConfig_Languages(t *testing.T) {
	cases := []struct {
		name    string
		content string
		want    string
	}{
		{"english", "the weather is nice and you have that from your window", "en"},
		{"english technical", "Nostr clients should cache profiles locally before rendering feeds.", "en"},
		{"english with kana", "I think this is fine but こんにちは might confuse the detector a lot today", "en"},
		{"spanish", "Hola amigos, este es un mensaje para la comunidad de nostr.", "es"},
		{"spanish accents", "Buenos días, hoy el relay está lento otra vez y no carga.", "es"},
		{"portuguese", "Obrigado pelo relay, hoje a conexão caiu de novo e ninguém respondia.", "pt"},
		{"german", "Danke, der relay ist heute wieder langsam und die verbindung bricht ab.", "de"},
		{"italian", "Ciao, questo e un messaggio con parole italiane per il test lingua.", "it"},
		{"indonesian", "Halo, ini adalah catatan untuk pengujian bahasa dengan komunitas nostr.", "id"},
		{"turkish", "Merhaba bu bir not ve bu test icin yazildi, nasil gidiyor.", "tr"},
		{"dutch", "Dit is een gewone Nederlandse zin over relays en clients in het netwerk.", "nl"},
		{"japanese", "これはテストの文章です。とても長い文章になっています。", "ja"},
		{"korean", "이것은 테스트 문장입니다 매우 긴 문장 입니다 그렇습니다", "ko"},
		{"arabic", "هذه جملة اختبار طويلة جدا مكتوبة باللغة العربية الفصحى", "ar"},
		{"persian", "این یک جمله فارسی برای آزمایش تشخیص زبان است و کمی طولانی تر نوشته شده", "fa"},
		{"hindi", "नमस्ते, यह नोस्ट्र में भाषा पहचान का परीक्षण है।", "hi"},
		{"russian", "это очень длинное тестовое предложение написанное на русском языке", "ru"},
		{"ukrainian", "Це дуже довге тестове речення українською мовою для перевірки мови", "uk"},
		{"thai", "นี่คือประโยคทดสอบที่ยาวมากเขียนเป็นภาษาไทยเพื่อทดสอบระบบ", "th"},
		{"bengali", "হ্যালো, এটি নস্টরে ভাষা শনাক্তকরণের একটি পরীক্ষা।", "bn"},
		{"greek", "Αυτή είναι μια δοκιμαστική πρόταση στα ελληνικά για τον εντοπισμό γλώσσας", "el"},
		{"hebrew", "זה משפט בדיקה בעברית כדי לזהות את השפה של ההערה", "he"},
	}
	cfg := testLangConfig()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lang, conf := detectPrimaryLanguageWithConfig(tc.content, cfg)
			if lang == nil || conf == nil {
				t.Fatalf("expected %s detection, got nil", tc.want)
			}
			if *lang != tc.want {
				t.Fatalf("lang = %q want %q (confidence %v)", *lang, tc.want, *conf)
			}
			if *conf < cfg.MinConfidence || *conf > 1 {
				t.Fatalf("confidence %v outside [%v, 1]", *conf, cfg.MinConfidence)
			}
		})
	}
}

func TestDetectPrimaryLanguageWithConfig_UnknownLatinIsNil(t *testing.T) {
	cfg := testLangConfig()
	lang, conf := detectPrimaryLanguageWithConfig("zzzz qqqq wwww vvvv xxxx kkkk jjjj", cfg)
	if lang != nil || conf != nil {
		t.Fatalf("unknown Latin gibberish must be nil; got %v %v", lang, conf)
	}
}

func TestDetectPrimaryLanguageWithConfig_OpaqueTokensDoNotCount(t *testing.T) {
	cfg := testLangConfig()
	// The URL path is full of English function words. After stripping, only
	// "gm" remains, which is too short to classify.
	content := strings.Join([]string{
		"gm",
		"#[3]",
		"nostr:npub1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqx",
		"https://example.com/the/and/for/that/with/this/you/from/have",
		"0123456789abcdef0123456789abcdef",
	}, " ")
	if got := cleanNoteTextForLanguage(content); got != "gm" {
		t.Fatalf("clean = %q want gm", got)
	}
	lang, conf := detectPrimaryLanguageWithConfig(content, cfg)
	if lang != nil || conf != nil {
		t.Fatalf("opaque tokens must not produce a language; got %v %v", lang, conf)
	}

	withSentence := "Nostr clients should cache profiles locally before rendering feeds. " +
		"https://example.com/the/and/for nostr:npub1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqx"
	lang, _ = detectPrimaryLanguageWithConfig(withSentence, cfg)
	if lang == nil || *lang != "en" {
		t.Fatalf("english sentence with a link and npub = %v, want en", lang)
	}
}

func TestCleanNoteTextForLanguage(t *testing.T) {
	if got := cleanNoteTextForLanguage("  hello   world  "); got != "hello world" {
		t.Fatalf("clean spaces = %q", got)
	}
	if got := cleanNoteTextForLanguage(""); got != "" {
		t.Fatalf("clean empty = %q", got)
	}
	long := "keep " + strings.Repeat("a", 48) + " tail"
	if got := cleanNoteTextForLanguage(long); got != "keep tail" {
		t.Fatalf("clean long token = %q", got)
	}
	thai := "นี่คือประโยคทดสอบที่ยาวมากเขียนเป็นภาษาไทยเพื่อทดสอบระบบ"
	if got := cleanNoteTextForLanguage(thai); got != thai {
		t.Fatalf("unspaced Thai must be kept, got %q", got)
	}
}

func TestLetterCount(t *testing.T) {
	if got := letterCount("abc"); got != 3 {
		t.Fatalf("ascii letterCount = %d want 3", got)
	}
	if got := letterCount("こんにちは"); got != 5 {
		t.Fatalf("multibyte letterCount = %d want 5", got)
	}
	if got := letterCount("gm 🔥!!"); got != 2 {
		t.Fatalf("emoji letterCount = %d want 2", got)
	}
}

func TestDetectPrimaryLanguageWithConfig_LongText(t *testing.T) {
	cfg := testLangConfig()
	sentence := "Nostr clients should cache profiles locally before rendering feeds. "
	content := strings.Repeat(sentence, 4)
	if len(content) < languageLowAccuracyMinRunes {
		t.Fatalf("fixture is too short for the low-accuracy path: %d", len(content))
	}
	lang, conf := detectPrimaryLanguageWithConfig(content, cfg)
	if lang == nil || *lang != "en" {
		t.Fatalf("long english = %v, want en", lang)
	}
	if conf == nil || *conf < cfg.MinConfidence {
		t.Fatalf("confidence = %v", conf)
	}
}

func TestExplicitNoteLanguage(t *testing.T) {
	cases := []struct {
		name string
		tags [][]string
		want string
		ok   bool
	}{
		{"iso tag", [][]string{{"l", "de", "ISO-639-1"}}, "de", true},
		{"iso spaced", [][]string{{"l", "FR", "ISO 639-1"}}, "fr", true},
		{"bare l", [][]string{{"l", "ja"}}, "ja", true},
		{"lang tag", [][]string{{"lang", "pt"}}, "pt", true},
		{"iso-639-2 skipped", [][]string{{"l", "eng", "ISO-639-2"}, {"l", "es"}}, "es", true},
		{"und rejected", [][]string{{"l", "und"}}, "", false},
		{"empty", nil, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := explicitNoteLanguage(tc.tags)
			if ok != tc.ok || got != tc.want {
				t.Fatalf("explicit = %q %v, want %q %v", got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestDecideNoteLanguage_Order(t *testing.T) {
	cfg := testLangConfig()
	en := "en"
	ja := "ja"
	conf := 0.9
	lang, gotConf, source := decideNoteLanguage("de", true, &en, &conf, 40, &ja, &en, cfg.MinChars)
	if source != languageSourceTag || lang == nil || *lang != "de" || gotConf == nil || *gotConf != 1 {
		t.Fatalf("tag should win: %v %v %s", lang, gotConf, source)
	}
	lang, _, source = decideNoteLanguage("", false, &en, &conf, 40, &ja, nil, cfg.MinChars)
	if source != languageSourceContent || lang == nil || *lang != "en" {
		t.Fatalf("content should win over thread: %v %s", lang, source)
	}
	lang, _, source = decideNoteLanguage("", false, nil, nil, 2, &ja, &en, cfg.MinChars)
	if source != languageSourceThread || lang == nil || *lang != "ja" {
		t.Fatalf("thread should win over author: %v %s", lang, source)
	}
	lang, gotConf, source = decideNoteLanguage("", false, nil, nil, 2, nil, &en, cfg.MinChars)
	if source != languageSourceAuthor || lang == nil || *lang != "en" || gotConf == nil || *gotConf != languageInferredConfidence {
		t.Fatalf("author fallback: %v %v %s", lang, gotConf, source)
	}
	_, _, source = decideNoteLanguage("", false, nil, nil, 2, nil, nil, cfg.MinChars)
	if source != languageSourceShort {
		t.Fatalf("short source = %s", source)
	}
	_, _, source = decideNoteLanguage("", false, nil, nil, 40, nil, nil, cfg.MinChars)
	if source != languageSourceUncertain {
		t.Fatalf("uncertain source = %s", source)
	}
}

func TestDominantAuthorLanguage(t *testing.T) {
	if got := dominantAuthorLanguage(nil); got != nil {
		t.Fatalf("empty = %v", got)
	}
	if got := dominantAuthorLanguage([]authorLanguageCount{{"en", 2}}); got != nil {
		t.Fatalf("below minimum = %v", got)
	}
	got := dominantAuthorLanguage([]authorLanguageCount{{"en", 3}})
	if got == nil || *got != "en" {
		t.Fatalf("single language = %v", got)
	}
	if got := dominantAuthorLanguage([]authorLanguageCount{{"en", 4}, {"es", 3}}); got != nil {
		t.Fatalf("close pair should be nil, got %v", *got)
	}
	got = dominantAuthorLanguage([]authorLanguageCount{{"en", 8}, {"es", 3}})
	if got == nil || *got != "en" {
		t.Fatalf("dominant = %v", got)
	}
}

func TestLanguageDecisionSettled(t *testing.T) {
	version := int32(NoteDiscoveryStatsVersion)
	old := int32(NoteDiscoveryStatsVersion - 1)
	lang := "en"
	content := languageSourceContent
	short := languageSourceShort
	if languageDecisionSettled(&version, &content, &lang) != true {
		t.Fatal("content label at the current version is settled")
	}
	if !languageDecisionSettled(&old, &content, &lang) {
		t.Fatal("a content label stays settled when the row version is older")
	}
	if !languageDecisionSettled(nil, &content, &lang) {
		t.Fatal("a content label stays settled without a row version")
	}
	if languageDecisionSettled(&version, &short, nil) {
		t.Fatal("short unmarked notes are not settled")
	}
	if !languageDecisionRetryThread(&old, &short) {
		t.Fatal("short notes retry the thread regardless of row version")
	}
	if !languageDecisionRetryThread(&version, &short) {
		t.Fatal("short notes should retry the parent thread")
	}
	if languageDecisionRetryThread(&version, &content) {
		t.Fatal("content labels should not take the thread-only path")
	}
	if languageDecisionRetryThread(nil, nil) {
		t.Fatal("a missing source takes the full detector")
	}
}

func TestGetEnvBoolOrDefault(t *testing.T) {
	const key = "NOSTRMASH_TEST_BOOL_KEY"
	t.Setenv(key, "")
	if !getEnvBoolOrDefault(key, true) {
		t.Fatal("empty env must fall back to default true")
	}
	t.Setenv(key, "false")
	if getEnvBoolOrDefault(key, true) {
		t.Fatal("explicit false must override default")
	}
	t.Setenv(key, "notabool")
	if !getEnvBoolOrDefault(key, true) {
		t.Fatal("unparsable env must fall back to default")
	}
}

func TestReadLanguageDetectionConfig_EnvOverrides(t *testing.T) {
	t.Setenv("NOSTRMASH_LANGUAGE_DETECTION_ENABLED", "true")
	t.Setenv("NOSTRMASH_LANGUAGE_DETECTION_MIN_CHARS", "50")
	t.Setenv("NOSTRMASH_LANGUAGE_DETECTION_MIN_CONFIDENCE", "0.8")
	cfg := readLanguageDetectionConfig()
	if !cfg.Enabled || cfg.MinChars != 50 || cfg.MinConfidence != 0.8 {
		t.Fatalf("unexpected config: %+v", cfg)
	}

	t.Setenv("NOSTRMASH_LANGUAGE_DETECTION_MIN_CHARS", "-3")
	t.Setenv("NOSTRMASH_LANGUAGE_DETECTION_MIN_CONFIDENCE", "5")
	cfg = readLanguageDetectionConfig()
	if cfg.MinChars != defaultLanguageMinChars {
		t.Fatalf("invalid min chars should fall back, got %d", cfg.MinChars)
	}
	if cfg.MinConfidence != defaultLanguageMinConfidence {
		t.Fatalf("out-of-range confidence should fall back, got %v", cfg.MinConfidence)
	}
}
