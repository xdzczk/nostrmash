package relaylookup

import (
	"net/url"
	"strings"
)

// directoryRelayHosts are kind-0 / directory relays. They are useful for
// profile fallback and a waste of the event-fallback timeout budget.
var directoryRelayHosts = map[string]struct{}{
	"purplepag.es": {},
}

// IsDirectoryRelay reports whether url is a known profile-directory relay.
func IsDirectoryRelay(raw string) bool {
	host := relayHost(raw)
	if host == "" {
		return false
	}
	if _, ok := directoryRelayHosts[host]; ok {
		return true
	}
	return strings.HasSuffix(host, ".purplepag.es")
}

// WithoutDirectoryRelays drops known directory relays from a URL list.
func WithoutDirectoryRelays(relays []string) []string {
	if len(relays) == 0 {
		return nil
	}
	out := make([]string, 0, len(relays))
	for _, relay := range relays {
		if IsDirectoryRelay(relay) {
			continue
		}
		out = append(out, relay)
	}
	return out
}

// MergeEventFallbackRelays prefers ranked healthy registry relays, then pads
// with the static floor, excluding directory relays and capping at limit.
func MergeEventFallbackRelays(ranked, static []string, limit int) []string {
	if limit <= 0 {
		limit = 3
	}
	seen := make(map[string]struct{}, limit)
	out := make([]string, 0, limit)
	appendUnique := func(relays []string) {
		for _, relay := range relays {
			relay = strings.TrimSpace(relay)
			if relay == "" || IsDirectoryRelay(relay) {
				continue
			}
			if _, exists := seen[relay]; exists {
				continue
			}
			seen[relay] = struct{}{}
			out = append(out, relay)
			if len(out) >= limit {
				return
			}
		}
	}
	appendUnique(ranked)
	if len(out) < limit {
		appendUnique(static)
	}
	return out
}

func relayHost(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return ""
	}
	return strings.ToLower(parsed.Hostname())
}
