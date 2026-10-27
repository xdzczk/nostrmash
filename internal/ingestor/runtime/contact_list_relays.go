package runtime

import (
	"encoding/json"
	"strings"

	"github.com/xdzczk/nostrmash/internal/relayurl"
)

// writeRelaysFromKind10002 returns relay URLs from a kind-10002 event whose
// NIP-65 marker is missing or "write". Read-only relays are skipped.
// relay_lists_latest drops that marker, so the crawl reads the raw event.
func writeRelaysFromKind10002(raw []byte) []string {
	if len(raw) == 0 {
		return nil
	}
	var event struct {
		Kind int        `json:"kind"`
		Tags [][]string `json:"tags"`
	}
	if err := json.Unmarshal(raw, &event); err != nil || event.Kind != 10002 {
		return nil
	}
	out := make([]string, 0)
	for _, tag := range event.Tags {
		if len(tag) < 2 || tag[0] != "r" {
			continue
		}
		if len(tag) >= 3 {
			marker := strings.ToLower(strings.TrimSpace(tag[2]))
			if marker != "" && marker != "write" {
				continue
			}
		}
		relayURL := strings.TrimSpace(tag[1])
		if relayURL == "" {
			continue
		}
		out = append(out, relayURL)
	}
	return out
}

// selectContactCrawlRelays prefers the author's write relays when they are on
// the crawl allowlist, and otherwise uses the configured ingest relays.
// Disabled relays are dropped from both lists. The result is normalized and capped.
func selectContactCrawlRelays(writeRelays, fallback, allowlist, disabled []string, requireTLS bool, max int) []string {
	if max <= 0 {
		max = 3
	}
	opts := relayurl.NormalizeOptions{RequireTLS: requireTLS}
	allow := normalizedRelaySet(allowlist, opts)
	blocked := normalizedRelaySet(disabled, opts)
	picked := filterNormalizedRelays(writeRelays, opts, func(relayURL string) bool {
		if _, ok := allow[relayURL]; !ok {
			return false
		}
		_, skip := blocked[relayURL]
		return !skip
	})
	if len(picked) == 0 {
		picked = filterNormalizedRelays(fallback, opts, func(relayURL string) bool {
			_, skip := blocked[relayURL]
			return !skip
		})
	}
	if len(picked) > max {
		return append([]string(nil), picked[:max]...)
	}
	return picked
}

func normalizedRelaySet(relays []string, opts relayurl.NormalizeOptions) map[string]struct{} {
	out := make(map[string]struct{}, len(relays))
	for _, relayURL := range relays {
		normalized, err := relayurl.Normalize(relayURL, opts)
		if err != nil || normalized == "" {
			continue
		}
		out[normalized] = struct{}{}
	}
	return out
}

func filterNormalizedRelays(relays []string, opts relayurl.NormalizeOptions, keep func(string) bool) []string {
	if len(relays) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(relays))
	out := make([]string, 0, len(relays))
	for _, relayURL := range relays {
		normalized, err := relayurl.Normalize(relayURL, opts)
		if err != nil || normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		if keep != nil && !keep(normalized) {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out
}
