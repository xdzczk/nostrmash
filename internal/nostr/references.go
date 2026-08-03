package nostr

import "strings"

// ETagRef is one NIP-10 e-tag after marker / legacy positional normalization.
type ETagRef struct {
	ID        string
	Relation  string
	TagIndex  int
	RelayHint string
}

// ParseRelationMarker normalizes root/reply/mention markers.
func ParseRelationMarker(value string) (string, bool) {
	marker := strings.ToLower(strings.TrimSpace(value))
	switch marker {
	case "root", "reply", "mention":
		return marker, true
	default:
		return "", false
	}
}

// DeriveETagReferences returns normalized e-tag references for tags.
// Unmarked e-tags follow the legacy positional scheme: first → root, last →
// reply (when more than one), middle → mention.
func DeriveETagReferences(tags [][]string) []ETagRef {
	refs := make([]ETagRef, 0)
	unmarkedIdx := make([]int, 0)

	for i, tag := range tags {
		if len(tag) < 2 || tag[0] != "e" {
			continue
		}
		referenced := strings.TrimSpace(tag[1])
		if referenced == "" {
			continue
		}

		relayHint := ""
		if len(tag) > 2 {
			relayHint = strings.TrimSpace(tag[2])
		}
		marker := ""
		if len(tag) > 3 {
			marker = strings.TrimSpace(tag[3])
		}
		relation, marked := ParseRelationMarker(marker)
		if marked {
			refs = append(refs, ETagRef{
				ID:        referenced,
				Relation:  relation,
				TagIndex:  i,
				RelayHint: relayHint,
			})
			continue
		}

		refs = append(refs, ETagRef{
			ID:        referenced,
			TagIndex:  i,
			RelayHint: relayHint,
		})
		unmarkedIdx = append(unmarkedIdx, len(refs)-1)
	}

	assignLegacyRelations(refs, unmarkedIdx)
	filtered := refs[:0]
	for _, ref := range refs {
		if ref.Relation == "" {
			continue
		}
		filtered = append(filtered, ref)
	}
	return filtered
}

func assignLegacyRelations(refs []ETagRef, unmarkedIdx []int) {
	if len(unmarkedIdx) == 0 {
		return
	}

	for _, idx := range unmarkedIdx {
		refs[idx].Relation = "mention"
	}

	rootSet := false
	replySet := false
	for _, ref := range refs {
		switch ref.Relation {
		case "root":
			rootSet = true
		case "reply":
			replySet = true
		}
	}

	if !rootSet {
		refs[unmarkedIdx[0]].Relation = "root"
	}
	if !replySet && len(unmarkedIdx) > 1 {
		refs[unmarkedIdx[len(unmarkedIdx)-1]].Relation = "reply"
	}
}

// FirstETagByRelation returns the first referenced id with the given relation.
func FirstETagByRelation(refs []ETagRef, relation string) string {
	for _, ref := range refs {
		if ref.Relation == relation {
			return ref.ID
		}
	}
	return ""
}

// ReplyParentEventID returns the thread parent for a note using the same rule
// as thread projection: prefer relation=reply, else fall back to relation=root.
// Single unmarked #e (legacy positional root-only) therefore counts as a reply
// to that event. Mentions alone do not yield a parent.
func ReplyParentEventID(tags [][]string) string {
	refs := DeriveETagReferences(tags)
	if parent := FirstETagByRelation(refs, "reply"); parent != "" {
		return parent
	}
	return FirstETagByRelation(refs, "root")
}
