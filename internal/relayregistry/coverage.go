package relayregistry

import (
	"net/url"
	"strings"
)

// AdmissionIndexed reports whether the relay is in the set the index reads.
// Probation, candidate, inactive, blocked, and draining are outside it.
func AdmissionIndexed(state AdmissionState) bool {
	return state == AdmissionActive || state == AdmissionPinned
}

// CoverageChangeKind is "added" or "dropped" when a relay crosses the indexed
// boundary. Score-only updates and moves that stay inside one side are not
// coverage changes.
func CoverageChangeKind(from, to AdmissionState) (string, bool) {
	was := AdmissionIndexed(from)
	now := AdmissionIndexed(to)
	if was == now {
		return "", false
	}
	if now {
		return "added", true
	}
	return "dropped", true
}

// CoverageHost is the hostname stored on a coverage-change row.
func CoverageHost(normalizedURL string) string {
	parsed, err := url.Parse(normalizedURL)
	host := ""
	if err == nil {
		host = parsed.Hostname()
	}
	if host == "" {
		host = strings.TrimPrefix(normalizedURL, "wss://")
		host = strings.TrimPrefix(host, "ws://")
		host = strings.TrimPrefix(host, "https://")
		host = strings.TrimPrefix(host, "http://")
		host, _, _ = strings.Cut(host, "/")
		host, _, _ = strings.Cut(host, ":")
	}
	return strings.ToLower(strings.TrimSuffix(host, "."))
}

// CoverageLabel is the sentence the public log returns for one change.
func CoverageLabel(kind, host string) string {
	switch kind {
	case "added":
		return "Added " + host
	case "dropped":
		return "Dropped " + host
	default:
		return host
	}
}
