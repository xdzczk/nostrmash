package query

import (
	"regexp"
	"strings"
)

var hashtagLookupPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

func normalizeHashtagForLookup(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	normalized = strings.TrimPrefix(normalized, "#")
	normalized = strings.TrimSpace(normalized)
	if normalized == "" || !hashtagLookupPattern.MatchString(normalized) || isNumericHashtag(normalized) {
		return ""
	}
	return normalized
}

func isNumericHashtag(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
