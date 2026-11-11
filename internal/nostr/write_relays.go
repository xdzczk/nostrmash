package nostr

import (
	"encoding/json"
	"strings"

	"github.com/xdzczk/nostrmash/internal/relayurl"
)

// MaxExtraAuthorWriteRelays is how many of an author's own write relays a
// note lookup adds on top of the relays already being queried. Three is
// enough to cover a client that publishes a post to only some of its write
// relays, without dialing the whole relay list on every refetch.
const MaxExtraAuthorWriteRelays = 3

// AuthorWriteRelays returns the relays an author publishes to. A kind-10002
// event wins: r tags whose marker is missing or "write" are included, and a
// list that names only read relays does not fall through to an older kind-3
// map. When no relay list is stored, the legacy kind-3 content object
// {"wss://relay":{"read":bool,"write":bool}} supplies the write:true entries
// in document order.
func AuthorWriteRelays(kind10002Raw, kind3Raw []byte) []string {
	if relays, declared := writeRelaysFromKind10002(kind10002Raw); declared {
		return relays
	}
	return writeRelaysFromKind3(kind3Raw)
}

// WriteRelaysFromKind10002 returns write relays from a kind-10002 event.
// A non-10002 payload, including a kind-3 event that happens to carry r tags,
// returns nil.
func WriteRelaysFromKind10002(raw []byte) []string {
	relays, _ := writeRelaysFromKind10002(raw)
	return relays
}

func writeRelaysFromKind10002(raw []byte) ([]string, bool) {
	if len(raw) == 0 {
		return nil, false
	}
	var event struct {
		Kind int        `json:"kind"`
		Tags [][]string `json:"tags"`
	}
	if err := json.Unmarshal(raw, &event); err != nil || event.Kind != 10002 {
		return nil, false
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
	return out, true
}

func writeRelaysFromKind3(raw []byte) []string {
	if len(raw) == 0 {
		return nil
	}
	var event struct {
		Kind    int    `json:"kind"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal(raw, &event); err != nil || event.Kind != 3 {
		return nil
	}
	content := strings.TrimSpace(event.Content)
	if content == "" {
		return nil
	}
	dec := json.NewDecoder(strings.NewReader(content))
	token, err := dec.Token()
	if err != nil || token != json.Delim('{') {
		return nil
	}
	out := make([]string, 0)
	for dec.More() {
		keyToken, err := dec.Token()
		if err != nil {
			return out
		}
		key, _ := keyToken.(string)
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return out
		}
		var spec struct {
			Write bool `json:"write"`
		}
		if err := json.Unmarshal(value, &spec); err != nil || !spec.Write {
			continue
		}
		relayURL := strings.TrimSpace(key)
		if relayURL == "" {
			continue
		}
		out = append(out, relayURL)
	}
	return out
}

// NormalizedAuthorWriteRelays returns AuthorWriteRelays after URL
// normalization. Entries that are not public ws/wss URLs are dropped.
// requireTLS drops ws:// URLs.
func NormalizedAuthorWriteRelays(kind10002Raw, kind3Raw []byte, requireTLS bool) []string {
	return normalizeRelayList(AuthorWriteRelays(kind10002Raw, kind3Raw), requireTLS)
}

// PickExtraWriteRelays keeps up to max candidates that the registry has
// probed successfully and that are not already in base or disabled.
// candidates, and the keys of healthy, must already be normalized with the
// same requireTLS flag used here for base and disabled.
func PickExtraWriteRelays(candidates, base, disabled []string, healthy map[string]struct{}, requireTLS bool, max int) []string {
	if max <= 0 || len(candidates) == 0 || len(healthy) == 0 {
		return nil
	}
	baseSet := normalizeRelaySet(base, requireTLS)
	disabledSet := normalizeRelaySet(disabled, requireTLS)
	out := make([]string, 0, max)
	seen := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		if _, dup := seen[candidate]; dup {
			continue
		}
		seen[candidate] = struct{}{}
		if _, skip := baseSet[candidate]; skip {
			continue
		}
		if _, skip := disabledSet[candidate]; skip {
			continue
		}
		if _, ok := healthy[candidate]; !ok {
			continue
		}
		out = append(out, candidate)
		if len(out) >= max {
			break
		}
	}
	return out
}

func normalizeRelayList(relays []string, requireTLS bool) []string {
	if len(relays) == 0 {
		return nil
	}
	opts := relayurl.NormalizeOptions{RequireTLS: requireTLS}
	out := make([]string, 0, len(relays))
	seen := make(map[string]struct{}, len(relays))
	for _, relayURL := range relays {
		normalized, err := relayurl.Normalize(relayURL, opts)
		if err != nil || normalized == "" {
			continue
		}
		if err := relayurl.Validate(normalized, relayurl.ValidateOptions{}); err != nil {
			continue
		}
		if _, dup := seen[normalized]; dup {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out
}

func normalizeRelaySet(relays []string, requireTLS bool) map[string]struct{} {
	list := normalizeRelayList(relays, requireTLS)
	out := make(map[string]struct{}, len(list))
	for _, relayURL := range list {
		out[relayURL] = struct{}{}
	}
	return out
}
