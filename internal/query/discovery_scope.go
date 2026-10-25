package query

import "strings"

// DiscoveryScope narrows one ranking to a single language. The zero value
// ranks every language and must keep the unscoped response unchanged.
type DiscoveryScope struct {
	Language string
}

// Lang returns the normalized language code, or empty for every language.
func (s DiscoveryScope) Lang() string {
	return strings.ToLower(strings.TrimSpace(s.Language))
}
