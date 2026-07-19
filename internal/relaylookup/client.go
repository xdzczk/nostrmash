package relaylookup

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/nostr"
	"github.com/xdzczk/nostrmash/internal/store"
)

type Client struct {
	relays    []string
	timeout   time.Duration
	maxFanout int
	log       *slog.Logger
}

func NewClient(relays []string, timeout time.Duration, maxFanout int) *Client {
	normalized := normalizeRelays(relays)
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	if maxFanout <= 0 {
		maxFanout = len(normalized)
	}
	if maxFanout > len(normalized) {
		maxFanout = len(normalized)
	}
	return &Client{
		relays:    normalized,
		timeout:   timeout,
		maxFanout: maxFanout,
		log:       logging.New("relaylookup"),
	}
}

func (c *Client) Enabled() bool {
	return c != nil && len(c.relays) > 0 && c.maxFanout > 0
}

func (c *Client) FetchEventsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	if !c.Enabled() {
		return map[string]json.RawMessage{}, nil
	}
	normalizedIDs := normalizeStrings(ids)
	if len(normalizedIDs) == 0 {
		return map[string]json.RawMessage{}, nil
	}

	events, err := c.collectFromRelays(ctx, map[string]any{
		"ids": normalizedIDs,
	}, len(normalizedIDs))
	if err != nil {
		return nil, err
	}
	requestedSet := make(map[string]struct{}, len(normalizedIDs))
	for _, id := range normalizedIDs {
		requestedSet[id] = struct{}{}
	}
	return filterRequestedValidatedEvents(events, requestedSet, c.log), nil
}

func (c *Client) FetchProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
	if !c.Enabled() {
		return map[string]store.ProfileProjection{}, nil
	}
	normalizedPubkeys := normalizeStrings(pubkeys)
	if len(normalizedPubkeys) == 0 {
		return map[string]store.ProfileProjection{}, nil
	}

	events, err := c.collectFromRelays(ctx, map[string]any{
		"kinds":   []int{0},
		"authors": normalizedPubkeys,
		"limit":   len(normalizedPubkeys) * 2,
	}, len(normalizedPubkeys)*2)
	if err != nil {
		return nil, err
	}
	requestedSet := make(map[string]struct{}, len(normalizedPubkeys))
	for _, pubkey := range normalizedPubkeys {
		requestedSet[pubkey] = struct{}{}
	}

	winners := make(map[string]store.ProfileProjection, len(normalizedPubkeys))
	for _, raw := range events {
		evt, validatedRaw, validateErr := validateFetchedEvent(raw)
		if validateErr != nil {
			c.log.Debug("fallback_profile_validation_failed", "error", validateErr)
			continue
		}
		pubkey := strings.TrimSpace(evt.Pubkey)
		if evt.Kind != 0 || pubkey == "" {
			continue
		}
		if _, requested := requestedSet[pubkey]; !requested {
			continue
		}
		winner, exists := winners[pubkey]
		if exists {
			if evt.CreatedAt < winner.MetadataCreatedAt {
				continue
			}
			if evt.CreatedAt == winner.MetadataCreatedAt && evt.ID <= winner.MetadataEventID {
				continue
			}
		}

		profileJSON := json.RawMessage(`{}`)
		if strings.TrimSpace(evt.Content) != "" {
			var obj map[string]any
			if err := json.Unmarshal([]byte(evt.Content), &obj); err == nil {
				if encoded, encErr := json.Marshal(obj); encErr == nil {
					profileJSON = encoded
				}
			}
		}

		winners[pubkey] = store.ProfileProjection{
			Pubkey:            pubkey,
			MetadataEventID:   strings.TrimSpace(evt.ID),
			MetadataCreatedAt: evt.CreatedAt,
			ProfileJSON:       profileJSON,
		}
		_ = validatedRaw // explicitly validated; use event fields and normalized profile body
	}
	return winners, nil
}

func (c *Client) collectFromRelays(ctx context.Context, filter map[string]any, maxEvents int) ([]json.RawMessage, error) {
	relays := c.relays
	if c.maxFanout < len(relays) {
		relays = relays[:c.maxFanout]
	}
	if len(relays) == 0 {
		return []json.RawMessage{}, nil
	}

	type relayResult struct {
		events []json.RawMessage
		err    error
	}
	results := make(chan relayResult, len(relays))
	relayCtx, cancelAll := context.WithCancel(ctx)
	defer cancelAll()
	var wg sync.WaitGroup
	for _, relayURL := range relays {
		wg.Add(1)
		go func(relayURL string) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					results <- relayResult{err: fmt.Errorf("relay query panicked (%s): %v", relayURL, r)}
				}
			}()
			relayCallCtx, cancel := context.WithTimeout(relayCtx, c.timeout)
			defer cancel()
			events, err := queryRelay(relayCallCtx, relayURL, filter, maxEvents)
			results <- relayResult{events: events, err: err}
		}(relayURL)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	out := make([]json.RawMessage, 0)
	successes := 0
	for result := range results {
		if result.err != nil {
			c.log.Debug("fallback_relay_query_failed", "error", result.err)
			continue
		}
		successes++
		out = append(out, result.events...)
		if maxEvents > 0 && len(out) >= maxEvents {
			cancelAll()
		}
	}
	if successes == 0 {
		return nil, fmt.Errorf("all fallback relay queries failed")
	}
	return out, nil
}

func queryRelay(ctx context.Context, relayURL string, filter map[string]any, maxEvents int) ([]json.RawMessage, error) {
	dialer := websocket.Dialer{}
	conn, resp, err := dialer.DialContext(ctx, relayURL, nil)
	if resp != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		return nil, fmt.Errorf("dial relay %s: %w", relayURL, err)
	}
	defer conn.Close()

	// Force-close the connection when the context is cancelled so that
	// a blocking ReadMessage unblocks immediately instead of waiting
	// for the deadline.
	closeCh := make(chan struct{})
	defer close(closeCh)
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-closeCh:
		}
	}()

	subID := fmt.Sprintf("nm-fallback-%d", time.Now().UnixNano())
	req := []any{"REQ", subID, filter}
	rawReq, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal fallback REQ: %w", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, rawReq); err != nil {
		return nil, fmt.Errorf("write fallback REQ: %w", err)
	}

	// Set a single read deadline for the entire read phase. gorilla/websocket
	// treats any read error (including timeouts) as fatal — calling ReadMessage
	// again after an error panics.
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetReadDeadline(deadline)
	}

	out := make([]json.RawMessage, 0, 8)
	for {
		_, frame, err := conn.ReadMessage()
		if err != nil {
			break
		}
		var envelope []json.RawMessage
		if err := json.Unmarshal(frame, &envelope); err != nil || len(envelope) == 0 {
			continue
		}
		var envelopeType string
		if err := json.Unmarshal(envelope[0], &envelopeType); err != nil {
			continue
		}
		switch envelopeType {
		case "EOSE":
			goto done
		case "EVENT":
			if len(envelope) >= 3 {
				rawEvent := json.RawMessage(strings.TrimSpace(string(envelope[2])))
				if len(rawEvent) > 0 {
					out = append(out, rawEvent)
				}
				if maxEvents > 0 && len(out) >= maxEvents {
					goto done
				}
			}
		}
	}
done:
	_ = conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`["CLOSE","%s"]`, subID)))
	return out, nil
}

func validateFetchedEvent(raw json.RawMessage) (*nostr.Event, json.RawMessage, error) {
	result := nostr.ParseAndValidate(raw, nostr.Options{})
	if !result.Valid() {
		if result.Err != nil {
			return nil, nil, fmt.Errorf("event validation failed: %s", result.Err.Error())
		}
		return nil, nil, fmt.Errorf("event validation failed")
	}
	return result.Event, result.RawJSON, nil
}

func filterRequestedValidatedEvents(
	events []json.RawMessage,
	requestedSet map[string]struct{},
	log *slog.Logger,
) map[string]json.RawMessage {
	out := make(map[string]json.RawMessage, len(requestedSet))
	for _, raw := range events {
		evt, validatedRaw, validateErr := validateFetchedEvent(raw)
		if validateErr != nil {
			if log != nil {
				log.Debug("fallback_event_validation_failed", "error", validateErr)
			}
			continue
		}
		if _, requested := requestedSet[evt.ID]; !requested {
			continue
		}
		if _, exists := out[evt.ID]; exists {
			continue
		}
		out[evt.ID] = validatedRaw
		if len(out) >= len(requestedSet) {
			break
		}
	}
	return out
}

func normalizeRelays(relays []string) []string {
	out := make([]string, 0, len(relays))
	seen := make(map[string]struct{}, len(relays))
	for _, relay := range relays {
		relay = strings.TrimSpace(relay)
		if relay == "" {
			continue
		}
		if _, ok := seen[relay]; ok {
			continue
		}
		seen[relay] = struct{}{}
		out = append(out, relay)
	}
	return out
}

func normalizeStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
