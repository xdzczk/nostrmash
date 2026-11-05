package read

import (
	"context"
	"strings"
	"testing"

	"github.com/xdzczk/nostrmash/internal/discoveryctx"
)

func TestNativeEventFilter(t *testing.T) {
	if got := nativeEventFilter(context.Background(), "e.id"); got != "" {
		t.Fatalf("default filter = %q", got)
	}
	ctx := discoveryctx.WithExcludeBridged(context.Background(), true)
	got := nativeEventFilter(ctx, "e.id")
	if !strings.Contains(got, "ea_bridge.event_id = e.id") || !strings.Contains(got, "proxy_protocol") {
		t.Fatalf("filter = %q", got)
	}
	profile := profileBridgeFilter(ctx)
	if !strings.Contains(profile, "profiles_latest") || !strings.Contains(profile, "metadata_event_id") {
		t.Fatalf("profile filter = %q", profile)
	}
}
