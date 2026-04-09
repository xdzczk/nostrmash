package api

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
)

// PublicRequestGuardOptions controls query-cost limits for public read APIs.
type PublicRequestGuardOptions struct {
	MaxResultLimit          int
	MaxPageSize             int
	MaxPageOffset           int
	MaxSearchWindowHours    int
	MaxDiscoveryWindowHours int
}

// WithPublicRequestGuards rejects high-cost and malformed query parameters on public endpoints.
func WithPublicRequestGuards(opts PublicRequestGuardOptions, next http.Handler) http.Handler {
	if opts.MaxResultLimit <= 0 {
		opts.MaxResultLimit = 100
	}
	if opts.MaxPageSize <= 0 {
		opts.MaxPageSize = 100
	}
	if opts.MaxPageOffset <= 0 {
		opts.MaxPageOffset = 5000
	}
	if opts.MaxSearchWindowHours <= 0 {
		opts.MaxSearchWindowHours = 7 * 24
	}
	if opts.MaxDiscoveryWindowHours <= 0 {
		opts.MaxDiscoveryWindowHours = 30 * 24
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		endpointClass := classifyPublicEndpoint(r.URL.Path)
		if endpointClass == publicEndpointClassUnknown {
			next.ServeHTTP(w, r)
			return
		}
		if err := validatePublicQueryGuards(r, endpointClass, opts); err != nil {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		next.ServeHTTP(w, r)
	})
}

func validatePublicQueryGuards(r *http.Request, endpointClass publicEndpointClass, opts PublicRequestGuardOptions) error {
	query := r.URL.Query()
	for _, key := range []string{"limit", "notes_limit", "hashtags_limit", "profiles_limit", "hashtag_stat_limit", "hashtag_limit"} {
		if err := validatePositiveQueryAtMost(query.Get(key), key, minInt(opts.MaxResultLimit, opts.MaxPageSize)); err != nil {
			return err
		}
	}
	if err := validateNonNegativeQueryAtMost(query.Get("offset"), "offset", opts.MaxPageOffset); err != nil {
		return err
	}

	var maxWindowHours int
	switch endpointClass {
	case publicEndpointClassSearch, publicEndpointClassSuggest:
		maxWindowHours = opts.MaxSearchWindowHours
	case publicEndpointClassDiscovery, publicEndpointClassPublicStats:
		maxWindowHours = opts.MaxDiscoveryWindowHours
	default:
		maxWindowHours = 0
	}
	if maxWindowHours > 0 {
		if err := validateWindowQuery(query.Get("window"), maxWindowHours); err != nil {
			return err
		}
	}

	return nil
}

func validatePositiveQueryAtMost(raw string, key string, max int) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed <= 0 {
		return errors.New(key + " must be a positive integer")
	}
	if parsed > max {
		return errors.New(key + " exceeds maximum allowed value")
	}
	return nil
}

func validateNonNegativeQueryAtMost(raw string, key string, max int) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed < 0 {
		return errors.New(key + " must be a non-negative integer")
	}
	if parsed > max {
		return errors.New(key + " exceeds maximum allowed value")
	}
	return nil
}

func validateWindowQuery(raw string, maxWindowHours int) error {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return nil
	}
	hours, unbounded, err := parseWindowHours(raw)
	if err != nil {
		return err
	}
	if unbounded || hours > maxWindowHours {
		return errors.New("window exceeds maximum allowed value")
	}
	return nil
}

func parseWindowHours(raw string) (int, bool, error) {
	switch raw {
	case "24h":
		return 24, false, nil
	case "7d":
		return 7 * 24, false, nil
	case "30d":
		return 30 * 24, false, nil
	case "all":
		return 0, true, nil
	default:
		return 0, false, errors.New("window must be one of: 24h, 7d, 30d, all")
	}
}

func minInt(a int, b int) int {
	if a <= b {
		return a
	}
	return b
}
