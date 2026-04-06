package derivation

import "strings"

type derivedReference struct {
	SourceEventID string
	Referenced    string
	Relation      string
	TagIndex      int
	RelayHint     string
	Marker        string
}

func deriveEventReferences(sourceEventID string, tags [][]string) []derivedReference {
	return deriveReferencesByTagName(sourceEventID, tags, "e")
}

func derivePubkeyReferences(sourceEventID string, tags [][]string) []derivedReference {
	return deriveReferencesByTagName(sourceEventID, tags, "p")
}

func deriveReferencesByTagName(sourceEventID string, tags [][]string, tagName string) []derivedReference {
	refs := make([]derivedReference, 0)
	unmarkedIdx := make([]int, 0)

	for i, tag := range tags {
		if len(tag) < 2 || tag[0] != tagName {
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
			refs = append(refs, derivedReference{
				SourceEventID: sourceEventID,
				Referenced:    referenced,
				Relation:      relation,
				TagIndex:      i,
				RelayHint:     relayHint,
				Marker:        relation,
			})
			continue
		}

		refs = append(refs, derivedReference{
			SourceEventID: sourceEventID,
			Referenced:    referenced,
			TagIndex:      i,
			RelayHint:     relayHint,
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

func assignLegacyRelations(refs []derivedReference, unmarkedIdx []int) {
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
		first := unmarkedIdx[0]
		refs[first].Relation = "root"
	}
	if !replySet && len(unmarkedIdx) > 1 {
		last := unmarkedIdx[len(unmarkedIdx)-1]
		refs[last].Relation = "reply"
	}
}

func firstReferenceByRelation(refs []derivedReference, relation string) string {
	for _, ref := range refs {
		if ref.Relation == relation {
			return ref.Referenced
		}
	}
	return ""
}
