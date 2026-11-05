package nostr

import "fmt"

// EventStillLiveSQL is true when alias has no NIP-40 expiration already in
// the past. alias must be a fixed SQL identifier, never request input.
// Events with no event_attributes row stay visible.
func EventStillLiveSQL(alias string) string {
	if alias == "" {
		alias = "events"
	}
	return fmt.Sprintf(`NOT EXISTS (
		SELECT 1 FROM event_attributes ea_exp
		WHERE ea_exp.event_id = %s.id
		  AND ea_exp.expires_at IS NOT NULL
		  AND ea_exp.expires_at <= EXTRACT(EPOCH FROM now())::bigint
	)`, alias)
}
