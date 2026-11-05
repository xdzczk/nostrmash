package read

import (
	"context"
	"fmt"

	"github.com/xdzczk/nostrmash/internal/discoveryctx"
)

// nativeEventFilter drops events that carry a NIP-48 proxy tag. Events
// with no event_attributes row stay, because absence is not a bridge.
func nativeEventFilter(ctx context.Context, eventIDSQL string) string {
	if !discoveryctx.ExcludeBridged(ctx) {
		return ""
	}
	return fmt.Sprintf(` AND NOT EXISTS (
		SELECT 1 FROM event_attributes ea_bridge
		WHERE ea_bridge.event_id = %s
		  AND ea_bridge.proxy_protocol <> ''
	)`, eventIDSQL)
}

// profileBridgeFilter drops profiles whose latest kind-0 event is bridged.
func profileBridgeFilter(ctx context.Context) string {
	if !discoveryctx.ExcludeBridged(ctx) {
		return ""
	}
	return ` AND NOT EXISTS (
		SELECT 1
		FROM profiles_latest pl_bridge
		JOIN event_attributes ea_bridge ON ea_bridge.event_id = pl_bridge.metadata_event_id
		WHERE pl_bridge.pubkey = profile_discovery_stats.pubkey
		  AND ea_bridge.proxy_protocol <> ''
	)`
}
