package api

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/languagelabel"
)

var errInvalidTrendingWindow = errors.New("window must be one of: 24h, 7d, 30d")
var errInvalidExcludeBridged = errors.New("exclude_bridged must be true or false")
var errInvalidDiscoveryLanguage = errors.New(`language must be "und", "und-uncertain", or a 2-8 letter code`)
var errInvalidHashtagNotesWindow = errors.New("window must be one of: 24h, 7d, 30d, all")
var errInvalidHashtagNotesSort = errors.New("sort must be one of: latest, top")
var errInvalidDomainNotesWindow = errors.New("window must be one of: 24h, 7d, 30d, all")
var errInvalidDomainNotesSort = errors.New("sort must be one of: latest, top")

func parseExcludeBridged(r *http.Request) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("exclude_bridged"))) {
	case "", "false", "0":
		return false, nil
	case "true", "1":
		return true, nil
	default:
		return false, errInvalidExcludeBridged
	}
}

func parseTrendingHashtagWindow(r *http.Request) (time.Duration, string, error) {
	return parseTrendingWindow(r)
}

func parseHashtagNotesWindow(r *http.Request) (string, error) {
	raw := r.URL.Query().Get("window")
	switch raw {
	case "", "24h":
		return "24h", nil
	case "7d":
		return "7d", nil
	case "30d":
		return "30d", nil
	case "all":
		return "all", nil
	default:
		return "", errInvalidHashtagNotesWindow
	}
}

func parseHashtagNotesSort(r *http.Request) (string, error) {
	raw := r.URL.Query().Get("sort")
	switch raw {
	case "", "latest":
		return "latest", nil
	case "top":
		return "top", nil
	default:
		return "", errInvalidHashtagNotesSort
	}
}

func parseDomainNotesWindow(r *http.Request) (string, error) {
	raw := r.URL.Query().Get("window")
	switch raw {
	case "", "24h":
		return "24h", nil
	case "7d":
		return "7d", nil
	case "30d":
		return "30d", nil
	case "all":
		return "all", nil
	default:
		return "", errInvalidDomainNotesWindow
	}
}

func parseDomainNotesSort(r *http.Request) (string, error) {
	raw := r.URL.Query().Get("sort")
	switch raw {
	case "", "latest":
		return "latest", nil
	case "top":
		return "top", nil
	default:
		return "", errInvalidDomainNotesSort
	}
}

func parseOptionalDiscoveryLanguage(r *http.Request) (string, error) {
	raw := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("lang")))
	if raw == "" {
		return "", nil
	}
	if !languagelabel.ValidFilter(raw) {
		return "", errInvalidDiscoveryLanguage
	}
	return raw, nil
}

func parseTrendingWindow(r *http.Request) (time.Duration, string, error) {
	return parseRankingWindow(r, "24h")
}

// parseRankingWindow accepts 24h, 7d, and 30d. An empty value uses fallback,
// which is 24h for trending, 7d for most-zapped notes, and 30d for most-supported profiles.
func parseRankingWindow(r *http.Request, fallback string) (time.Duration, string, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("window"))
	if raw == "" {
		raw = fallback
	}
	switch raw {
	case "24h":
		return 24 * time.Hour, "24h", nil
	case "7d":
		return 7 * 24 * time.Hour, "7d", nil
	case "30d":
		return 30 * 24 * time.Hour, "30d", nil
	default:
		return 0, "", errInvalidTrendingWindow
	}
}
