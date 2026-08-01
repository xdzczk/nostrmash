package derivation

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"slices"
	"strings"

	"github.com/xdzczk/nostrmash/internal/domainnorm"
)

const maxEventURLLinks = 32

var (
	urlCandidatePattern = regexp.MustCompile(`(?i)https?://[^\s<>"']+`)
)

type normalizedEventURL struct {
	URL             string
	Domain          string
	CanonicalDomain string
}

func (h *Handlers) ProjectEventURLs(ctx context.Context, eventID string) error {
	return h.projectEventURLsWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectEventURLsWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	var authorPubkey string
	var createdAt int64
	var kind int
	var content string
	if err := h.pool.QueryRow(ctx, `
		SELECT pubkey, created_at, kind, content
		FROM events
		WHERE id = $1
	`, eventID).Scan(&authorPubkey, &createdAt, &kind, &content); err != nil {
		return fmt.Errorf("load event for URL projection: %w", err)
	}
	urls := extractNormalizedEventURLs(content, maxEventURLLinks)

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationEventURLs,
		EventURLsVersion,
		"Project normalized URLs with observed and canonical domains from note-like events",
		versionOverride,
	)
	if err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `DELETE FROM event_urls WHERE event_id = $1`, eventID); err != nil {
		return fmt.Errorf("delete prior event urls: %w", err)
	}
	// Links from authors outside the Web of Trust are never recorded here
	// (as opposed to just being excluded from the homepage trending
	// snapshot — see trustedAuthorJoinClause in
	// projection_relay_window_snapshots.go). The DELETE above still runs
	// unconditionally so re-deriving an event whose author has since
	// dropped out of the trust graph cleans up any URLs recorded while
	// they were still trusted.
	if isNoteDiscoveryProjectableKind(kind) && len(urls) > 0 {
		excluded, err := authorOutsideTrustGraph(ctx, tx, authorPubkey)
		if err != nil {
			return err
		}
		if excluded {
			urls = nil
		}
	}
	if !isNoteDiscoveryProjectableKind(kind) || len(urls) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit no-op URL projection tx: %w", err)
		}
		return nil
	}

	for _, row := range urls {
		if _, err := tx.Exec(ctx, `
			INSERT INTO event_urls (
				event_id, author_pubkey, created_at, url, domain, canonical_domain, derivation_version
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (event_id, url) DO UPDATE
			SET author_pubkey = EXCLUDED.author_pubkey,
			    created_at = EXCLUDED.created_at,
			    domain = EXCLUDED.domain,
			    canonical_domain = EXCLUDED.canonical_domain,
			    derivation_version = EXCLUDED.derivation_version,
			    projected_at = now()
		`, eventID, authorPubkey, createdAt, row.URL, row.Domain, row.CanonicalDomain, writeVersion); err != nil {
			return fmt.Errorf("upsert event url: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit URL projection tx: %w", err)
	}
	return nil
}

func extractNormalizedEventURLs(content string, maxLinks int) []normalizedEventURL {
	if maxLinks <= 0 {
		maxLinks = maxEventURLLinks
	}
	candidates := urlCandidatePattern.FindAllString(content, -1)
	if len(candidates) == 0 {
		return nil
	}
	out := make([]normalizedEventURL, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, raw := range candidates {
		if len(out) >= maxLinks {
			break
		}
		normalized, domain, ok := normalizeEventURLCandidate(raw)
		if !ok {
			continue
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalizedEventURL{
			URL:             normalized,
			Domain:          domain,
			CanonicalDomain: domainnorm.CanonicalizeDiscoveryDomain(domain),
		})
	}
	slices.SortFunc(out, func(a, b normalizedEventURL) int {
		if cmp := strings.Compare(a.Domain, b.Domain); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.URL, b.URL)
	})
	return out
}

func normalizeEventURLCandidate(raw string) (string, string, bool) {
	trimmed := strings.TrimSpace(strings.TrimRight(raw, ".,!?;:'\")]}"))
	if trimmed == "" {
		return "", "", false
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return "", "", false
	}
	scheme := strings.ToLower(strings.TrimSpace(parsed.Scheme))
	if scheme != "http" && scheme != "https" {
		return "", "", false
	}
	domain := domainnorm.NormalizeHost(parsed.Hostname())
	if domain == "" {
		return "", "", false
	}
	host := domain
	if port := strings.TrimSpace(parsed.Port()); port != "" && !isDefaultHTTPPort(scheme, port) {
		host = net.JoinHostPort(domain, port)
	}
	normalized := url.URL{
		Scheme:   scheme,
		Host:     host,
		Path:     parsed.EscapedPath(),
		RawQuery: parsed.RawQuery,
	}
	return normalized.String(), domain, true
}

func isDefaultHTTPPort(scheme string, port string) bool {
	return (scheme == "http" && port == "80") || (scheme == "https" && port == "443")
}
