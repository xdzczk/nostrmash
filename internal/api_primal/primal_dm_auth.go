package api_primal

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/nostr"
)

func validatePubkeyHex(pubkey string) error {
	pubkey = strings.TrimSpace(pubkey)
	if len(pubkey) != 64 {
		return errors.New("invalid pubkey")
	}
	for _, r := range pubkey {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') {
			continue
		}
		if r >= 'A' && r <= 'F' {
			continue
		}
		return fmt.Errorf("invalid pubkey")
	}
	return nil
}

func parseAndValidateDMResetAuth(kwargs map[string]any) (receiver string, sender string, err error) {
	eventFromUser, ok := kwargs["event_from_user"]
	if !ok {
		return "", "", errors.New("event_from_user is required")
	}
	payload, err := json.Marshal(eventFromUser)
	if err != nil {
		return "", "", errors.New("event_from_user is malformed")
	}
	result := nostr.ParseAndValidate(payload, nostr.Options{})
	if !result.Valid() {
		return "", "", errors.New("verification failed")
	}
	now := time.Now().Unix()
	if result.Event.CreatedAt <= now-300 {
		return "", "", errors.New("event is too old")
	}
	if result.Event.CreatedAt >= now+300 {
		return "", "", errors.New("event from the future")
	}
	receiver = strings.TrimSpace(result.Event.Pubkey)
	if err := validatePubkeyHex(receiver); err != nil {
		return "", "", err
	}
	sender, _ = kwargs["peer_pubkey"].(string)
	if strings.TrimSpace(sender) == "" {
		sender, _ = kwargs["sender"].(string)
	}
	if err := validatePubkeyHex(sender); err != nil {
		return "", "", err
	}
	return receiver, sender, nil
}

func parseAndValidateDMResetAllAuth(kwargs map[string]any) (string, error) {
	eventFromUser, ok := kwargs["event_from_user"]
	if !ok {
		return "", errors.New("event_from_user is required")
	}
	payload, err := json.Marshal(eventFromUser)
	if err != nil {
		return "", errors.New("event_from_user is malformed")
	}
	result := nostr.ParseAndValidate(payload, nostr.Options{})
	if !result.Valid() {
		return "", errors.New("verification failed")
	}
	now := time.Now().Unix()
	if result.Event.CreatedAt <= now-300 {
		return "", errors.New("event is too old")
	}
	if result.Event.CreatedAt >= now+300 {
		return "", errors.New("event from the future")
	}
	receiver := strings.TrimSpace(result.Event.Pubkey)
	if err := validatePubkeyHex(receiver); err != nil {
		return "", err
	}
	return receiver, nil
}
