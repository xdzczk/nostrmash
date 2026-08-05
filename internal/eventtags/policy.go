// Package eventtags owns the persistence policy for the event_tags index
// table. event_tags is a derived join index over events.raw_json — not a
// source of truth — so only tag names that have production SQL readers are
// persisted. Full tags always remain recoverable from raw_json.
//
// Lives outside internal/store so both the ingest path and the retention
// bounded context can share one allowlist without crossing store-context
// import boundaries.
package eventtags

import "slices"

// Nostr kind numbers that carry high-cardinality tags used only by
// derivation handlers that already parse events.raw_json.
const (
	KindContactList = 3
	KindRelayList   = 10002
)

// AllowedTagNames is the closed set of tag names persisted into event_tags.
// Every name here has at least one production SQL reader; everything else is
// junk for the index and is filtered at ingest + pruned historically.
// Sorted for stable diffs and deterministic SQL array params.
var AllowedTagNames = []string{
	"a",
	"d",
	"e",
	"g",
	"group",
	"image",
	"imeta",
	"m",
	"p",
	"r",
	"series",
	"t",
	"thumb",
	"u",
	"url",
	"video",
	"word",
}

var allowedTagNameSet = func() map[string]struct{} {
	out := make(map[string]struct{}, len(AllowedTagNames))
	for _, name := range AllowedTagNames {
		out[name] = struct{}{}
	}
	return out
}()

// ShouldPersist reports whether a single expanded tag value row should be
// written to event_tags for an event of the given kind.
//
// Kind-scoped exclusions:
//   - kind 3 (contact list) p-tags: follower edges are projected from
//     raw_json into follower_edges; indexing them made contact lists show up
//     as "mentions" and dominated ~49% of event_tags bytes.
//   - kind 10002 (relay list) r-tags: relay-list projection reads raw_json;
//     note media/link readers only join kind-1 r-tags. Dominated ~20% of
//     event_tags bytes.
func ShouldPersist(kind int, tagName string) bool {
	if tagName == "" {
		return false
	}
	if tagName == "p" && kind == KindContactList {
		return false
	}
	if tagName == "r" && kind == KindRelayList {
		return false
	}
	_, ok := allowedTagNameSet[tagName]
	return ok
}

// AllowedTagNamesCopy returns a defensive copy of AllowedTagNames suitable
// for passing as a SQL parameter without risk of caller mutation.
func AllowedTagNamesCopy() []string {
	return slices.Clone(AllowedTagNames)
}
