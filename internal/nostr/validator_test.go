package nostr

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestParseAndValidate_ValidFixture(t *testing.T) {
	payload := readFixture(t, "valid/basic_text_note.json")

	result := ParseAndValidate(payload, Options{})
	if !result.Valid() {
		t.Fatalf("expected valid result, got error: %+v", result.Err)
	}
	if result.Event == nil {
		t.Fatalf("expected event in valid result")
	}
	if len(result.RawJSON) == 0 {
		t.Fatalf("expected raw JSON to be retained for valid event")
	}
}

func TestParseAndValidate_InvalidFixtures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		fixture       string
		wantCode      ErrorCode
		wantStage     Stage
		wantRawRetain bool
	}{
		{
			name:          "malformed JSON",
			fixture:       "invalid/malformed_json.json",
			wantCode:      ErrInvalidJSON,
			wantStage:     StageStructural,
			wantRawRetain: false,
		},
		{
			name:          "missing required field",
			fixture:       "invalid/missing_sig.json",
			wantCode:      ErrMissingField,
			wantStage:     StageStructural,
			wantRawRetain: true,
		},
		{
			name:          "field type mismatch",
			fixture:       "invalid/bad_field_types.json",
			wantCode:      ErrFieldTypeInvalid,
			wantStage:     StageCanonical,
			wantRawRetain: true,
		},
		{
			name:          "canonical id mismatch",
			fixture:       "invalid/id_mismatch.json",
			wantCode:      ErrCanonicalIDMismatch,
			wantStage:     StageCanonical,
			wantRawRetain: true,
		},
		{
			name:          "invalid signature",
			fixture:       "invalid/bad_signature.json",
			wantCode:      ErrSignatureInvalid,
			wantStage:     StageSignature,
			wantRawRetain: true,
		},
		{
			name:          "malicious control character",
			fixture:       "malicious/control_char_content.json",
			wantCode:      ErrContentControlChars,
			wantStage:     StageSafety,
			wantRawRetain: true,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			payload := readFixture(t, tc.fixture)
			result := ParseAndValidate(payload, Options{})

			if result.Err == nil {
				t.Fatalf("expected error, got success")
			}
			if result.Err.Code != tc.wantCode {
				t.Fatalf("unexpected code: got %s, want %s", result.Err.Code, tc.wantCode)
			}
			if result.Err.Stage != tc.wantStage {
				t.Fatalf("unexpected stage: got %s, want %s", result.Err.Stage, tc.wantStage)
			}
			if tc.wantRawRetain && len(result.RawJSON) == 0 {
				t.Fatalf("expected raw JSON to be retained")
			}
			if !tc.wantRawRetain && len(result.RawJSON) != 0 {
				t.Fatalf("expected raw JSON to be empty")
			}
		})
	}
}

func TestParseAndValidate_ContentTooLargeOption(t *testing.T) {
	payload := readFixture(t, "valid/basic_text_note.json")

	result := ParseAndValidate(payload, Options{
		MaxContentBytes: 8,
	})
	if result.Err == nil {
		t.Fatalf("expected content-too-large error")
	}
	if result.Err.Code != ErrContentTooLarge {
		t.Fatalf("unexpected error code: got %s, want %s", result.Err.Code, ErrContentTooLarge)
	}
	if result.Err.Stage != StageSafety {
		t.Fatalf("unexpected stage: got %s, want %s", result.Err.Stage, StageSafety)
	}
}

func TestParseAndValidate_PayloadTooLargeOption(t *testing.T) {
	payload := readFixture(t, "valid/basic_text_note.json")

	result := ParseAndValidate(payload, Options{
		MaxRawJSONBytes: len(payload) - 1,
	})
	if result.Err == nil {
		t.Fatalf("expected payload-too-large error")
	}
	if result.Err.Code != ErrPayloadTooLarge {
		t.Fatalf("unexpected error code: got %s, want %s", result.Err.Code, ErrPayloadTooLarge)
	}
	if len(result.RawJSON) != 0 {
		t.Fatalf("expected raw JSON to be dropped for oversized payload")
	}
}

func TestParseAndValidate_RejectsJSONObjectArrays(t *testing.T) {
	payload, err := json.Marshal([]any{"not", "an", "event"})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	result := ParseAndValidate(payload, Options{})
	if result.Err == nil {
		t.Fatalf("expected json_not_object error")
	}
	if result.Err.Code != ErrJSONNotObject {
		t.Fatalf("unexpected error code: got %s, want %s", result.Err.Code, ErrJSONNotObject)
	}
}

func readFixture(t *testing.T, relPath string) []byte {
	t.Helper()
	fullPath := filepath.Join("testdata", relPath)
	payload, err := os.ReadFile(fullPath)
	if err != nil {
		t.Fatalf("read fixture %s: %v", relPath, err)
	}
	return payload
}
