package api

import (
	"strings"
	"testing"
)

func TestClassifyNotePreview_ModeSelection(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		content  string
		expected string
		compact  bool
	}{
		{
			name:     "standard prose",
			content:  "Shipping a small release today with cleaner onboarding copy.",
			expected: notePreviewModeStandardText,
			compact:  false,
		},
		{
			name:     "media note",
			content:  "check this https://cdn.example.com/image.png",
			expected: notePreviewModeMediaLed,
			compact:  false,
		},
		{
			name:     "url heavy note",
			content:  "https://one.example/a https://two.example/b https://three.example/c",
			expected: notePreviewModeLinkLed,
			compact:  false,
		},
		{
			name: "config note",
			content: strings.Join([]string{
				"{",
				`  "read": true,`,
				`  "write": false,`,
				`  "relay": "wss://relay.example"`,
				"}",
			}, "\n"),
			expected: notePreviewModeConfigRawData,
			compact:  true,
		},
		{
			name: "identifier heavy note",
			content: strings.Join([]string{
				"nevent1qqs8d6yvm3g4e8ls4ef5ds7fjg30pzx2f4lvs2z8n8t4jkew6a0r9nspz3mhxue69uhhyetvv9ujumt0d5h",
				"note1w4lwy52zv9pc0m62v2f33k2pq2d9rttc6v8h2nsnzp2u80gpz2aqsv2pg2",
			}, " "),
			expected: notePreviewModeIdentifierHeavy,
			compact:  true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			preview := classifyNotePreview(tc.content)
			if preview.Mode != tc.expected {
				t.Fatalf("unexpected mode: got %q want %q", preview.Mode, tc.expected)
			}
			if preview.Compact != tc.compact {
				t.Fatalf("unexpected compact flag: got %v want %v", preview.Compact, tc.compact)
			}
			if strings.TrimSpace(preview.DisplayContent) == "" {
				t.Fatalf("expected non-empty display content")
			}
			if strings.Contains(preview.DisplayContent, "Config-like note") ||
				strings.Contains(preview.DisplayContent, "Identifier-heavy note") ||
				strings.Contains(preview.DisplayContent, "Media note - from") ||
				strings.Contains(preview.DisplayContent, "Link-rich note - from") {
				t.Fatalf("display content should not use synthetic summary labels: %q", preview.DisplayContent)
			}
		})
	}
}

func TestClassifyNotePreview_NormalizesLinksForDisplay(t *testing.T) {
	t.Parallel()

	preview := classifyNotePreview("Read https://www.example.com/path?x=1 and https://nostr.example/abc")
	if strings.Contains(preview.DisplayContent, "https://") {
		t.Fatalf("display content should not include full urls: %q", preview.DisplayContent)
	}
	if !strings.Contains(preview.DisplayContent, "example.com") {
		t.Fatalf("display content should include normalized domains: %q", preview.DisplayContent)
	}
}

func TestClassifyNotePreview_OmitsMediaURLsFromDisplay(t *testing.T) {
	t.Parallel()

	preview := classifyNotePreview("GM ☕ https://blossom.primal.net/67abe3541726675f55edbcb2bf134c1d15c23bd1db0ba31b7e2aa4b4ddce7c78.jpg")
	if preview.Mode != notePreviewModeMediaLed {
		t.Fatalf("unexpected mode: got %q want %q", preview.Mode, notePreviewModeMediaLed)
	}
	if strings.Contains(preview.DisplayContent, "https://") {
		t.Fatalf("display content should not include media urls: %q", preview.DisplayContent)
	}
	if strings.Contains(preview.DisplayContent, "[blossom.primal.net]") {
		t.Fatalf("display content should not include media host placeholders: %q", preview.DisplayContent)
	}
	if !strings.Contains(preview.DisplayContent, "GM") {
		t.Fatalf("display content should keep surrounding prose: %q", preview.DisplayContent)
	}
}
