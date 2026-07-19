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

func TestDetectPrimaryLanguageWithConfig_ScriptDetection(t *testing.T) {
	cfg := testLangConfig()
	cases := []struct {
		name    string
		content string
		want    string
	}{
		{"japanese", "これはテストの文章です。とても長い文章になっています。", "ja"},
		{"korean", "이것은 테스트 문장입니다 매우 긴 문장 입니다 그렇습니다", "ko"},
		{"arabic", "هذه جملة اختبار طويلة جدا مكتوبة باللغة العربية الفصحى", "ar"},
		{"cyrillic", "это очень длинное тестовое предложение написанное на русском языке", "ru"},
		{"han", "这是一个非常长的测试句子用中文写成的内容很多", "zh"},
		{"thai", "นี่คือประโยคทดสอบที่ยาวมากเขียนเป็นภาษาไทยเพื่อทดสอบระบบ", "th"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lang, conf := detectPrimaryLanguageWithConfig(tc.content, cfg)
			if lang == nil || conf == nil {
				t.Fatalf("expected %s detection, got nil", tc.want)
			}
			if *lang != tc.want {
				t.Fatalf("lang = %q want %q", *lang, tc.want)
			}
			if *conf < cfg.MinConfidence {
				t.Fatalf("confidence %v below min %v", *conf, cfg.MinConfidence)
			}
		})
	}
}

func TestDetectPrimaryLanguageWithConfig_LatinDictionary(t *testing.T) {
	cfg := testLangConfig()
	lang, conf := detectPrimaryLanguageWithConfig("the weather is nice and you have that from your window", cfg)
	if lang == nil {
		t.Fatal("expected English detection, got nil")
	}
	if *lang != "en" {
		t.Fatalf("lang = %q want en", *lang)
	}
	if *conf <= 0 || *conf > 1 {
		t.Fatalf("confidence out of range: %v", *conf)
	}
}

func TestDetectPrimaryLanguageWithConfig_UnknownLatinIsNil(t *testing.T) {
	cfg := testLangConfig()
	// Long enough, Latin, but no dictionary hits: must not force a guess.
	lang, conf := detectPrimaryLanguageWithConfig("zzzz qqqq wwww vvvv xxxx kkkk jjjj", cfg)
	if lang != nil || conf != nil {
		t.Fatalf("unknown Latin gibberish must be nil; got %v %v", lang, conf)
	}
}

func TestDetectByScript_NoScriptMatch(t *testing.T) {
	if lang, conf, ok := detectByScript("plain ascii text"); ok {
		t.Fatalf("ascii should not match a script; got %q %v", lang, conf)
	}
}

func TestNormalizeTokens(t *testing.T) {
	got := normalizeTokens("Hello, WORLD! it's a test-case 123")
	joined := strings.Join(got, "|")
	// Apostrophes are stripped to their outer quotes; hyphen splits tokens.
	want := "hello|world|it's|a|test|case|123"
	if joined != want {
		t.Fatalf("normalizeTokens = %q want %q", joined, want)
	}
}

func TestRuneCount(t *testing.T) {
	if got := runeCount("abc"); got != 3 {
		t.Fatalf("ascii runeCount = %d want 3", got)
	}
	if got := runeCount("こんにちは"); got != 5 {
		t.Fatalf("multibyte runeCount = %d want 5", got)
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
