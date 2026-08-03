package derivation

import "github.com/xdzczk/nostrmash/internal/nostr"

type derivedReference struct {
	SourceEventID string
	Referenced    string
	Relation      string
	TagIndex      int
	RelayHint     string
	Marker        string
}

func deriveEventReferences(sourceEventID string, tags [][]string) []derivedReference {
	raw := nostr.DeriveETagReferences(tags)
	refs := make([]derivedReference, 0, len(raw))
	for _, ref := range raw {
		refs = append(refs, derivedReference{
			SourceEventID: sourceEventID,
			Referenced:    ref.ID,
			Relation:      ref.Relation,
			TagIndex:      ref.TagIndex,
			RelayHint:     ref.RelayHint,
			Marker:        ref.Relation,
		})
	}
	return refs
}

func firstReferenceByRelation(refs []derivedReference, relation string) string {
	for _, ref := range refs {
		if ref.Relation == relation {
			return ref.Referenced
		}
	}
	return ""
}

// replyParentEventID returns the thread parent for a note using the same rule as
// thread projection: prefer relation=reply, else fall back to relation=root.
func replyParentEventID(refs []derivedReference) string {
	parent := firstReferenceByRelation(refs, "reply")
	if parent != "" {
		return parent
	}
	return firstReferenceByRelation(refs, "root")
}

// ReplyParentEventID resolves the NIP-10 thread parent from raw e-tags.
// Delegates to internal/nostr so ingest and derivation share one definition.
func ReplyParentEventID(tags [][]string) string {
	return nostr.ReplyParentEventID(tags)
}

// replyAffectTargets returns note ids whose reply aggregates may change when a
// kind-1 event is (re)projected: the direct parent and, when distinct, the root.
func replyAffectTargets(refs []derivedReference) []string {
	parent := replyParentEventID(refs)
	if parent == "" {
		return nil
	}
	out := []string{parent}
	if root := firstReferenceByRelation(refs, "root"); root != "" && root != parent {
		out = append(out, root)
	}
	return out
}
