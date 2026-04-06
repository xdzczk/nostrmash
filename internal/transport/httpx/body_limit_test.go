package httpx

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDecodeJSONBodyLimited_Success(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(`{"ids":["a"]}`))
	rec := httptest.NewRecorder()
	var dst struct {
		IDs []string `json:"ids"`
	}
	if err := DecodeJSONBodyLimited(rec, req, 1024, &dst, DecodeJSONOptions{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(dst.IDs) != 1 || dst.IDs[0] != "a" {
		t.Fatalf("unexpected decoded payload: %#v", dst)
	}
}

func TestDecodeJSONBodyLimited_TooLarge(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(`{"x":"`+strings.Repeat("a", 2048)+`"}`))
	rec := httptest.NewRecorder()
	var dst map[string]any
	err := DecodeJSONBodyLimited(rec, req, 64, &dst, DecodeJSONOptions{})
	if err != ErrPayloadTooLarge {
		t.Fatalf("expected ErrPayloadTooLarge, got %v", err)
	}
}

func TestDecodeJSONBodyLimited_TrailingJSON(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(`{"x":1} {"y":2}`))
	rec := httptest.NewRecorder()
	var dst map[string]any
	err := DecodeJSONBodyLimited(rec, req, 1024, &dst, DecodeJSONOptions{})
	if err != ErrMultipleJSON {
		t.Fatalf("expected ErrMultipleJSON for trailing payload, got %v", err)
	}
}

func TestDecodeJSONBodyLimited_DisallowUnknown(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(`{"x":1}`))
	rec := httptest.NewRecorder()
	var dst struct {
		Y int `json:"y"`
	}
	err := DecodeJSONBodyLimited(rec, req, 1024, &dst, DecodeJSONOptions{DisallowUnknown: true})
	if err != ErrInvalidJSON {
		t.Fatalf("expected ErrInvalidJSON with unknown field, got %v", err)
	}
}
