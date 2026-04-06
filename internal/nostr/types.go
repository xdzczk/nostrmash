package nostr

import "encoding/json"

// Event is the canonical Nostr event shape used by this validation pipeline.
type Event struct {
	ID        string
	Pubkey    string
	CreatedAt int64
	Kind      int
	Tags      [][]string
	Content   string
	Sig       string
}

// Stage identifies the validation phase where a failure occurred.
type Stage string

const (
	StageStructural Stage = "stage_1_structural"
	StageCanonical  Stage = "stage_2_canonical"
	StageSignature  Stage = "stage_3_signature"
	StageSafety     Stage = "stage_4_safety"
)

// ErrorCode is a stable machine-readable validation code.
type ErrorCode string

const (
	ErrPayloadEmpty        ErrorCode = "payload_empty"
	ErrPayloadTooLarge     ErrorCode = "payload_too_large"
	ErrInvalidJSON         ErrorCode = "invalid_json"
	ErrJSONNotObject       ErrorCode = "json_not_object"
	ErrMissingField        ErrorCode = "missing_field"
	ErrFieldTypeInvalid    ErrorCode = "field_type_invalid"
	ErrIDFormatInvalid     ErrorCode = "id_format_invalid"
	ErrPubkeyFormatInvalid ErrorCode = "pubkey_format_invalid"
	ErrSigFormatInvalid    ErrorCode = "sig_format_invalid"
	ErrCreatedAtInvalid    ErrorCode = "created_at_invalid"
	ErrKindInvalid         ErrorCode = "kind_invalid"
	ErrTagsInvalid         ErrorCode = "tags_invalid"
	ErrCanonicalIDMismatch ErrorCode = "canonical_id_mismatch"
	ErrSignatureInvalid    ErrorCode = "signature_invalid"
	ErrContentControlChars ErrorCode = "content_control_chars"
	ErrContentTooLarge     ErrorCode = "content_too_large"
)

// ValidationError is suitable for invalid_events error_code/error_message fields.
type ValidationError struct {
	Code    ErrorCode
	Message string
	Stage   Stage
}

func (e *ValidationError) Error() string {
	if e == nil {
		return ""
	}
	return string(e.Code) + ": " + e.Message
}

// ValidationResult contains either a validated event or a typed validation error.
// RawJSON is retained only when the payload is safe to preserve.
type ValidationResult struct {
	Event   *Event
	RawJSON json.RawMessage
	Err     *ValidationError
}

func (r ValidationResult) Valid() bool {
	return r.Err == nil && r.Event != nil
}

// Options controls deterministic safety limits in validation.
type Options struct {
	// MaxRawJSONBytes is the largest payload that may be retained as raw JSON.
	MaxRawJSONBytes int
	// MaxContentBytes is the largest allowed event content value.
	MaxContentBytes int
}

func (o Options) withDefaults() Options {
	if o.MaxRawJSONBytes <= 0 {
		o.MaxRawJSONBytes = 256 * 1024
	}
	if o.MaxContentBytes <= 0 {
		o.MaxContentBytes = 64 * 1024
	}
	return o
}
