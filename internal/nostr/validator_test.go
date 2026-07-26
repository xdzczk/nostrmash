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

func TestParseAndValidate_RejectsOutOfRangeCreatedAt(t *testing.T) {
	payload := readFixture(t, "valid/basic_text_note.json")
	var envelope map[string]any
	if err := json.Unmarshal(payload, &envelope); err != nil {
		t.Fatalf("unmarshal fixture: %v", err)
	}
	envelope["created_at"] = MaxUnixCreatedAt + 1
	// Keep id/sig mismatched; canonical stage should fail on created_at first.
	mutated, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal mutated fixture: %v", err)
	}

	result := ParseAndValidate(mutated, Options{})
	if result.Err == nil {
		t.Fatal("expected created_at_invalid error")
	}
	if result.Err.Code != ErrCreatedAtInvalid {
		t.Fatalf("unexpected code: got %s, want %s", result.Err.Code, ErrCreatedAtInvalid)
	}
	if result.Err.Stage != StageCanonical {
		t.Fatalf("unexpected stage: got %s, want %s", result.Err.Stage, StageCanonical)
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

func TestMarshalCanonical_NoHTMLEscaping(t *testing.T) {
	got, err := marshalCanonical(
		"37ce94259421d17a13e04382205c6061323ebc6bbfa46aab1f73e6f93c774a5e",
		1700000000,
		1,
		[][]string{{"t", "test"}},
		"hello <world> & goodbye",
	)
	if err != nil {
		t.Fatalf("marshalCanonical: %v", err)
	}

	s := string(got)
	if !contains(s, "<world>") {
		t.Fatalf("expected literal <world>, got HTML-escaped output: %s", s)
	}
	if !contains(s, "& goodbye") {
		t.Fatalf("expected literal &, got HTML-escaped output: %s", s)
	}
}

func TestParseAndValidate_ContentWithHTMLChars(t *testing.T) {
	content := "check <this> & that"
	pubkeyHex := "37ce94259421d17a13e04382205c6061323ebc6bbfa46aab1f73e6f93c774a5e"
	var createdAt int64 = 1700000000
	kind := 1
	tags := [][]string{{"t", "nostr"}}

	canonical, err := marshalCanonical(pubkeyHex, createdAt, kind, tags, content)
	if err != nil {
		t.Fatalf("marshalCanonical: %v", err)
	}

	if contains(string(canonical), `\u003c`) || contains(string(canonical), `\u003e`) || contains(string(canonical), `\u0026`) {
		t.Fatalf("canonical JSON must not HTML-escape: %s", canonical)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchSubstring(s, substr)
}

func searchSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
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
