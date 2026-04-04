package relay

import (
	"context"
	"errors"
)

// NoopConnector is a placeholder until websocket transport wiring is added.
type NoopConnector struct{}

func (NoopConnector) Connect(ctx context.Context, relayURL string) (Connection, error) {
	return nil, errors.New("relay transport not implemented yet")
}
