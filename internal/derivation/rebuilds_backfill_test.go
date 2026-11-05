package derivation

import "testing"

func TestEcosystemBackfillRebuildsAreFullScope(t *testing.T) {
	var h Handlers
	for _, name := range []string{
		DerivationEventAttributes,
		DerivationProfileFields,
		DerivationAuthorRecentEvents,
		DerivationZapReceipts,
	} {
		def, err := h.projectionDefinition(name)
		if err != nil {
			t.Fatalf("definition %s: %v", name, err)
		}
		if def.rebuildFull == nil {
			t.Fatalf("%s full rebuild is not wired", name)
		}
	}
}
