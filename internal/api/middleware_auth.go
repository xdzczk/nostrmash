package api

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// RequireBearerToken protects endpoints with an env-configured bearer token.
func RequireBearerToken(token string, next http.Handler) http.Handler {
	expected := strings.TrimSpace(token)
	if next == nil {
		next = http.NotFoundHandler()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if expected == "" {
			writeError(r.Context(), w, http.StatusServiceUnavailable, "admin_unavailable", "admin auth is not configured")
			return
		}
		got, ok := bearerTokenFromHeader(r.Header.Get("Authorization"))
		if !ok {
			writeError(r.Context(), w, http.StatusUnauthorized, "unauthorized", "missing or invalid bearer token")
			return
		}
		if subtle.ConstantTimeCompare([]byte(got), []byte(expected)) != 1 {
			writeError(r.Context(), w, http.StatusUnauthorized, "unauthorized", "missing or invalid bearer token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func bearerTokenFromHeader(value string) (string, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", false
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(value, prefix) {
		return "", false
	}
	token := strings.TrimSpace(strings.TrimPrefix(value, prefix))
	if token == "" {
		return "", false
	}
	return token, true
}
