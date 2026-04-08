package domainnorm

import (
	"net"
	"net/url"
	"regexp"
	"strings"
)

var domainLabelPattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

// NormalizeHost normalizes a host/domain token into canonical lowercase form.
func NormalizeHost(value string) string {
	host := strings.ToLower(strings.TrimSpace(value))
	host = strings.TrimSuffix(host, ".")
	if host == "" {
		return ""
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.String()
	}
	if len(host) > 253 {
		return ""
	}
	labels := strings.Split(host, ".")
	for _, label := range labels {
		if !domainLabelPattern.MatchString(label) {
			return ""
		}
	}
	return host
}

// NormalizeLookupValue accepts either a bare domain/host token or URL-like input.
func NormalizeLookupValue(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	candidate := trimmed
	if !strings.Contains(candidate, "://") {
		candidate = "//" + candidate
	}
	if parsed, err := url.Parse(candidate); err == nil {
		if host := strings.TrimSpace(parsed.Hostname()); host != "" {
			if normalized := NormalizeHost(host); normalized != "" {
				return normalized
			}
		}
	}
	return NormalizeHost(trimmed)
}
