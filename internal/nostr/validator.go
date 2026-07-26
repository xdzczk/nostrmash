package nostr

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/btcsuite/btcd/btcec/v2/schnorr"
)

// ParseAndValidate performs 4-stage Nostr event parsing and validation.
func ParseAndValidate(payload []byte, options Options) ValidationResult {
	opts := options.withDefaults()

	if len(payload) == 0 {
		return ValidationResult{
			Err: &ValidationError{
				Code:    ErrPayloadEmpty,
				Message: "payload is empty",
				Stage:   StageStructural,
			},
		}
	}
	if len(payload) > opts.MaxRawJSONBytes {
		return ValidationResult{
			Err: &ValidationError{
				Code:    ErrPayloadTooLarge,
				Message: fmt.Sprintf("payload exceeds %d bytes", opts.MaxRawJSONBytes),
				Stage:   StageStructural,
			},
		}
	}

	trimmed := bytes.TrimSpace(payload)
	if !json.Valid(trimmed) {
		return ValidationResult{
			Err: &ValidationError{
				Code:    ErrInvalidJSON,
				Message: "payload is not valid JSON",
				Stage:   StageStructural,
			},
		}
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &fields); err != nil {
		return ValidationResult{
			Err: &ValidationError{
				Code:    ErrJSONNotObject,
				Message: "payload must be a JSON object",
				Stage:   StageStructural,
			},
		}
	}

	// Raw JSON is retained only after we know this is a bounded valid JSON object.
	rawJSON := append(json.RawMessage(nil), trimmed...)

	for _, key := range []string{"id", "pubkey", "created_at", "kind", "tags", "content", "sig"} {
		if _, ok := fields[key]; !ok {
			return ValidationResult{
				RawJSON: rawJSON,
				Err: &ValidationError{
					Code:    ErrMissingField,
					Message: fmt.Sprintf("missing required field: %s", key),
					Stage:   StageStructural,
				},
			}
		}
	}

	event, err := parseCanonical(fields)
	if err != nil {
		return ValidationResult{
			RawJSON: rawJSON,
			Err:     err,
		}
	}

	if err := verifySignature(event); err != nil {
		return ValidationResult{
			RawJSON: rawJSON,
			Err:     err,
		}
	}

	if err := checkContentSafety(event, opts); err != nil {
		return ValidationResult{
			RawJSON: rawJSON,
			Err:     err,
		}
	}

	return ValidationResult{
		Event:   event,
		RawJSON: rawJSON,
	}
}

func parseCanonical(fields map[string]json.RawMessage) (*Event, *ValidationError) {
	id, err := parseStringField(fields, "id")
	if err != nil {
		return nil, err
	}
	pubkey, err := parseStringField(fields, "pubkey")
	if err != nil {
		return nil, err
	}
	createdAt, err := parseInt64Field(fields, "created_at")
	if err != nil {
		return nil, err
	}
	kind, err := parseIntField(fields, "kind")
	if err != nil {
		return nil, err
	}
	tags, err := parseTags(fields["tags"])
	if err != nil {
		return nil, err
	}
	content, err := parseStringField(fields, "content")
	if err != nil {
		return nil, err
	}
	sig, err := parseStringField(fields, "sig")
	if err != nil {
		return nil, err
	}

	switch {
	case !isLowerHex(id, 64):
		return nil, &ValidationError{
			Code:    ErrIDFormatInvalid,
			Message: "id must be 64 lowercase hex characters",
			Stage:   StageCanonical,
		}
	case !isLowerHex(pubkey, 64):
		return nil, &ValidationError{
			Code:    ErrPubkeyFormatInvalid,
			Message: "pubkey must be 64 lowercase hex characters",
			Stage:   StageCanonical,
		}
	case !isLowerHex(sig, 128):
		return nil, &ValidationError{
			Code:    ErrSigFormatInvalid,
			Message: "sig must be 128 lowercase hex characters",
			Stage:   StageCanonical,
		}
	case createdAt < 0:
		return nil, &ValidationError{
			Code:    ErrCreatedAtInvalid,
			Message: "created_at must be a non-negative unix timestamp",
			Stage:   StageCanonical,
		}
	case createdAt > MaxUnixCreatedAt:
		return nil, &ValidationError{
			Code:    ErrCreatedAtInvalid,
			Message: "created_at is out of range for a unix timestamp",
			Stage:   StageCanonical,
		}
	case kind < 0:
		return nil, &ValidationError{
			Code:    ErrKindInvalid,
			Message: "kind must be a non-negative integer",
			Stage:   StageCanonical,
		}
	case kind > 65535:
		return nil, &ValidationError{
			Code:    ErrKindInvalid,
			Message: "kind must be <= 65535",
			Stage:   StageCanonical,
		}
	}

	canonicalJSON, canonicalErr := marshalCanonical(pubkey, createdAt, kind, tags, content)
	if canonicalErr != nil {
		return nil, &ValidationError{
			Code:    ErrCanonicalIDMismatch,
			Message: "failed to build canonical event serialization",
			Stage:   StageCanonical,
		}
	}
	sum := sha256.Sum256(canonicalJSON)
	computedID := hex.EncodeToString(sum[:])
	if computedID != id {
		return nil, &ValidationError{
			Code:    ErrCanonicalIDMismatch,
			Message: "id does not match canonical event hash",
			Stage:   StageCanonical,
		}
	}

	return &Event{
		ID:        id,
		Pubkey:    pubkey,
		CreatedAt: createdAt,
		Kind:      kind,
		Tags:      tags,
		Content:   content,
		Sig:       sig,
	}, nil
}

func verifySignature(event *Event) *ValidationError {
	pubBytes, _ := hex.DecodeString(event.Pubkey)
	idBytes, _ := hex.DecodeString(event.ID)
	sigBytes, _ := hex.DecodeString(event.Sig)

	pubkey, err := schnorr.ParsePubKey(pubBytes)
	if err != nil {
		return &ValidationError{
			Code:    ErrSignatureInvalid,
			Message: "pubkey could not be parsed for schnorr verification",
			Stage:   StageSignature,
		}
	}
	signature, err := schnorr.ParseSignature(sigBytes)
	if err != nil {
		return &ValidationError{
			Code:    ErrSignatureInvalid,
			Message: "sig could not be parsed for schnorr verification",
			Stage:   StageSignature,
		}
	}
	if !signature.Verify(idBytes, pubkey) {
		return &ValidationError{
			Code:    ErrSignatureInvalid,
			Message: "signature does not verify against id and pubkey",
			Stage:   StageSignature,
		}
	}
	return nil
}

func checkContentSafety(event *Event, opts Options) *ValidationError {
	if len(event.Content) > opts.MaxContentBytes {
		return &ValidationError{
			Code:    ErrContentTooLarge,
			Message: fmt.Sprintf("content exceeds %d bytes", opts.MaxContentBytes),
			Stage:   StageSafety,
		}
	}

	for i := 0; i < len(event.Content); i++ {
		b := event.Content[i]
		if b < 0x20 && b != '\n' && b != '\r' && b != '\t' {
			return &ValidationError{
				Code:    ErrContentControlChars,
				Message: "content contains disallowed control characters",
				Stage:   StageSafety,
			}
		}
	}
	return nil
}

func parseStringField(fields map[string]json.RawMessage, name string) (string, *ValidationError) {
	var value string
	if err := json.Unmarshal(fields[name], &value); err != nil {
		return "", &ValidationError{
			Code:    ErrFieldTypeInvalid,
			Message: fmt.Sprintf("field %s must be a string", name),
			Stage:   StageCanonical,
		}
	}
	return value, nil
}

func parseInt64Field(fields map[string]json.RawMessage, name string) (int64, *ValidationError) {
	var value int64
	if err := json.Unmarshal(fields[name], &value); err != nil {
		return 0, &ValidationError{
			Code:    ErrFieldTypeInvalid,
			Message: fmt.Sprintf("field %s must be an integer", name),
			Stage:   StageCanonical,
		}
	}
	return value, nil
}

func parseIntField(fields map[string]json.RawMessage, name string) (int, *ValidationError) {
	var value int
	if err := json.Unmarshal(fields[name], &value); err != nil {
		return 0, &ValidationError{
			Code:    ErrFieldTypeInvalid,
			Message: fmt.Sprintf("field %s must be an integer", name),
			Stage:   StageCanonical,
		}
	}
	return value, nil
}

func parseTags(raw json.RawMessage) ([][]string, *ValidationError) {
	var list []json.RawMessage
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, &ValidationError{
			Code:    ErrFieldTypeInvalid,
			Message: "field tags must be an array",
			Stage:   StageCanonical,
		}
	}

	tags := make([][]string, 0, len(list))
	for i, rawTag := range list {
		var values []string
		if err := json.Unmarshal(rawTag, &values); err != nil {
			return nil, &ValidationError{
				Code:    ErrTagsInvalid,
				Message: fmt.Sprintf("tag at index %d must be an array of strings", i),
				Stage:   StageCanonical,
			}
		}
		if len(values) == 0 {
			return nil, &ValidationError{
				Code:    ErrTagsInvalid,
				Message: fmt.Sprintf("tag at index %d must contain at least one string", i),
				Stage:   StageCanonical,
			}
		}
		tags = append(tags, values)
	}
	return tags, nil
}

// marshalCanonical produces the NIP-01 canonical JSON serialization of an event
// for ID hashing. Go's json.Marshal HTML-escapes <, >, & into \uXXXX sequences
// by default, which differs from the serialization signers use. SetEscapeHTML(false)
// prevents this, matching the behavior of JS/Rust/Python JSON serializers.
func marshalCanonical(pubkey string, createdAt int64, kind int, tags [][]string, content string) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode([]any{0, pubkey, createdAt, kind, tags, content}); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

func isLowerHex(s string, expectedLen int) bool {
	if len(s) != expectedLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		isDigit := c >= '0' && c <= '9'
		isLower := c >= 'a' && c <= 'f'
		if !isDigit && !isLower {
			return false
		}
	}
	return true
}
