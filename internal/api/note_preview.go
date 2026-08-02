package api

import (
	"encoding/json"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"unicode"
)

const (
	notePreviewModeStandardText    = "standard_text_preview"
	notePreviewModeMediaLed        = "media_led_preview"
	notePreviewModeLinkLed         = "link_led_preview"
	notePreviewModeConfigRawData   = "config_raw_data_preview"
	notePreviewModeIdentifierHeavy = "long_identifier_heavy_preview"
)

var (
	notePreviewURLPattern          = regexp.MustCompile(`https?://[^\s]+`)
	notePreviewWhitespacePattern   = regexp.MustCompile(`\s+`)
	notePreviewRelayPattern        = regexp.MustCompile(`(?i)\bwss://[^\s]+`)
	notePreviewBech32TokenPattern  = regexp.MustCompile(`(?i)\b(?:note|nevent|nprofile|npub|nsec|naddr|nrelay)1[023456789acdefghjklmnpqrstuvwxyz]{20,}\b`)
	notePreviewHexTokenPattern     = regexp.MustCompile(`\b[a-f0-9]{48,}\b`)
	notePreviewImageURLPattern     = regexp.MustCompile(`(?i)\.(?:jpg|jpeg|png|gif|webp|avif|svg)(?:\?[^\s]*)?$`)
	notePreviewVideoURLPattern     = regexp.MustCompile(`(?i)\.(?:mp4|webm|mov|m4v|m3u8)(?:\?[^\s]*)?$`)
	notePreviewURLSchemeTrimRight  = ".,;:!?)]}"
	notePreviewDefaultDisplayLimit = 220
)

type notePreviewMetadata struct {
	Mode             string
	DisplayContent   string
	FirstLine        string
	Compact          bool
	ContainsRawShape bool
	LinkDomains      []string
}

type rawNotePreviewCandidate struct {
	ID        string `json:"id"`
	Pubkey    string `json:"pubkey"`
	CreatedAt int64  `json:"created_at"`
	Content   string `json:"content"`
}

func buildNotePreviewPayload(eventID string, content string) map[string]any {
	preview := classifyNotePreview(content)
	payload := map[string]any{
		"mode":             preview.Mode,
		"display_content":  preview.DisplayContent,
		"is_compact":       preview.Compact,
		"contains_raw":     preview.ContainsRawShape,
		"open_note_url":    "/api/v1/notes/" + eventID + "/summary",
		"contains_content": strings.TrimSpace(content) != "",
	}
	if preview.FirstLine != "" {
		payload["first_line"] = preview.FirstLine
	}
	if len(preview.LinkDomains) > 0 {
		payload["domains"] = preview.LinkDomains
	}
	return payload
}

func buildRawNotePreviewItems(notes []json.RawMessage) []map[string]any {
	items := make([]map[string]any, 0, len(notes))
	for _, raw := range notes {
		var candidate rawNotePreviewCandidate
		if err := json.Unmarshal(raw, &candidate); err != nil {
			continue
		}
		id := strings.TrimSpace(candidate.ID)
		if id == "" {
			continue
		}
		item := map[string]any{
			"event_id":      id,
			"author_pubkey": candidate.Pubkey,
			"created_at":    candidate.CreatedAt,
			"content":       candidate.Content,
			"preview":       buildNotePreviewPayload(id, candidate.Content),
		}
		items = append(items, item)
	}
	return items
}

func classifyNotePreview(content string) notePreviewMetadata {
	trimmed := strings.TrimSpace(content)
	if trimmed == "" {
		return notePreviewMetadata{
			Mode:           notePreviewModeStandardText,
			DisplayContent: "",
			FirstLine:      "",
		}
	}

	urls := notePreviewURLPattern.FindAllString(trimmed, -1)
	urlHeavy := isURLHeavy(trimmed, urls)
	configLike := isConfigLike(trimmed, urls)
	identifierHeavy := isIdentifierHeavy(trimmed)
	hasMedia := hasMediaURL(urls)
	firstLine := previewFirstLine(trimmed, 140)
	domains := extractURLDomains(urls)

	normalized := normalizeContentForDisplay(trimmed, notePreviewDefaultDisplayLimit)
	mode := notePreviewModeStandardText
	compact := false
	containsRaw := false
	display := normalized

	switch {
	case configLike:
		mode = notePreviewModeConfigRawData
		compact = true
		containsRaw = true
		display = normalizeContentForDisplay(trimmed, 140)
	case identifierHeavy:
		mode = notePreviewModeIdentifierHeavy
		compact = true
		containsRaw = true
		display = normalizeContentForDisplay(trimmed, 140)
	case hasMedia:
		mode = notePreviewModeMediaLed
		display = normalizeContentForDisplay(trimmed, 280)
	case urlHeavy:
		mode = notePreviewModeLinkLed
		display = normalizeContentForDisplay(trimmed, 220)
	}

	return notePreviewMetadata{
		Mode:             mode,
		DisplayContent:   display,
		FirstLine:        firstLine,
		Compact:          compact,
		ContainsRawShape: containsRaw,
		LinkDomains:      domains,
	}
}

func isConfigLike(content string, urls []string) bool {
	trimmed := strings.TrimSpace(content)
	if trimmed == "" {
		return false
	}

	lines := splitNonEmptyLines(trimmed)
	if len(lines) >= 3 {
		kvLines := 0
		braceHeavy := 0
		relayLines := 0
		for _, line := range lines {
			if strings.Contains(line, ":") || strings.Contains(line, "=") {
				kvLines++
			}
			if strings.ContainsAny(line, "{}[]") {
				braceHeavy++
			}
			if notePreviewRelayPattern.MatchString(line) {
				relayLines++
			}
		}
		if kvLines >= 3 && (braceHeavy >= 2 || relayLines >= 2) {
			return true
		}
		if relayLines >= 3 {
			return true
		}
	}

	if strings.HasPrefix(trimmed, "{") && strings.HasSuffix(trimmed, "}") && len(trimmed) >= 80 {
		return true
	}
	if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") && len(trimmed) >= 80 {
		return true
	}

	if len(urls) >= 3 && strings.Count(trimmed, "\n") >= 2 {
		urlChars := 0
		for _, u := range urls {
			urlChars += len(u)
		}
		if float64(urlChars)/float64(len(trimmed)) >= 0.55 {
			return true
		}
	}
	return false
}

func isURLHeavy(content string, urls []string) bool {
	if len(urls) == 0 {
		return false
	}
	urlChars := 0
	for _, u := range urls {
		urlChars += len(u)
	}
	ratio := float64(urlChars) / float64(len(content))
	return len(urls) >= 3 || ratio >= 0.45
}

func isIdentifierHeavy(content string) bool {
	tokens := strings.Fields(content)
	if len(tokens) == 0 {
		return false
	}
	longRuns := 0
	maxTokenLen := 0
	for _, token := range tokens {
		cleaned := strings.Trim(token, ".,;:!?()[]{}<>\"'")
		if len(cleaned) > maxTokenLen {
			maxTokenLen = len(cleaned)
		}
		if len(cleaned) >= 64 && mostlyIdentifierRunes(cleaned) {
			longRuns++
		}
	}
	if maxTokenLen >= 128 {
		return true
	}
	bech32Tokens := len(notePreviewBech32TokenPattern.FindAllString(content, -1))
	hexTokens := len(notePreviewHexTokenPattern.FindAllString(strings.ToLower(content), -1))
	return longRuns >= 2 || bech32Tokens >= 2 || hexTokens >= 3
}

func mostlyIdentifierRunes(value string) bool {
	if value == "" {
		return false
	}
	identifierCount := 0
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-' {
			identifierCount++
		}
	}
	return float64(identifierCount)/float64(len(value)) >= 0.9
}

func hasMediaURL(urls []string) bool {
	for _, value := range urls {
		clean := strings.TrimRight(value, notePreviewURLSchemeTrimRight)
		if notePreviewImageURLPattern.MatchString(clean) || notePreviewVideoURLPattern.MatchString(clean) {
			return true
		}
	}
	return false
}

func normalizeContentForDisplay(content string, maxLen int) string {
	if strings.TrimSpace(content) == "" {
		return ""
	}
	replaced := notePreviewURLPattern.ReplaceAllStringFunc(content, func(raw string) string {
		trimmed := strings.TrimRight(raw, notePreviewURLSchemeTrimRight)
		// Media URLs are rendered as attachments; never leave them in preview copy.
		if notePreviewImageURLPattern.MatchString(trimmed) || notePreviewVideoURLPattern.MatchString(trimmed) {
			return ""
		}
		domain := urlDomain(trimmed)
		if domain == "" {
			return "[link]"
		}
		return "[" + domain + "]"
	})
	replaced = notePreviewWhitespacePattern.ReplaceAllString(strings.TrimSpace(replaced), " ")
	if maxLen <= 0 || len(replaced) <= maxLen {
		return replaced
	}
	return strings.TrimSpace(replaced[:maxLen-1]) + "..."
}

func previewFirstLine(content string, maxLen int) string {
	if strings.TrimSpace(content) == "" {
		return ""
	}
	lines := splitNonEmptyLines(content)
	if len(lines) == 0 {
		return ""
	}
	first := normalizeContentForDisplay(lines[0], maxLen)
	return strings.TrimSpace(first)
}

func extractURLDomains(urls []string) []string {
	if len(urls) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(urls))
	for _, raw := range urls {
		domain := urlDomain(strings.TrimRight(raw, notePreviewURLSchemeTrimRight))
		if domain == "" {
			continue
		}
		if _, ok := seen[domain]; ok {
			continue
		}
		seen[domain] = struct{}{}
		out = append(out, domain)
	}
	sort.Strings(out)
	return out
}

func urlDomain(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	host := strings.ToLower(strings.TrimSpace(parsed.Hostname()))
	return strings.TrimPrefix(host, "www.")
}

func splitNonEmptyLines(value string) []string {
	rawLines := strings.Split(value, "\n")
	lines := make([]string, 0, len(rawLines))
	for _, line := range rawLines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		lines = append(lines, line)
	}
	return lines
}
