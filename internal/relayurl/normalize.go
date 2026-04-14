package relayurl

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"strings"
)

type NormalizeOptions struct {
	RequireTLS bool
}

func Normalize(raw string, options NormalizeOptions) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("parse: %w", err)
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("host is required")
	}
	if parsed.Fragment != "" {
		return "", fmt.Errorf("fragments are not allowed")
	}
	if parsed.RawQuery != "" {
		return "", fmt.Errorf("query parameters are not allowed")
	}
	if parsed.User != nil {
		return "", fmt.Errorf("userinfo is not allowed")
	}

	scheme := strings.ToLower(parsed.Scheme)
	switch scheme {
	case "wss":
	case "ws":
		if options.RequireTLS {
			return "", fmt.Errorf("ws scheme is disallowed when TLS is required")
		}
	default:
		return "", fmt.Errorf("scheme must be ws or wss")
	}

	host := strings.ToLower(parsed.Host)
	path := strings.TrimSpace(parsed.EscapedPath())
	if path == "/" {
		path = ""
	}
	return fmt.Sprintf("%s://%s%s", scheme, host, path), nil
}

// ValidateOptions controls what kinds of relay URLs are acceptable.
type ValidateOptions struct {
	AllowPrivateNetwork bool
}

// Validate checks a previously normalized relay URL for safety constraints.
func Validate(normalized string, opts ValidateOptions) error {
	if normalized == "" {
		return fmt.Errorf("empty relay URL")
	}
	parsed, err := url.Parse(normalized)
	if err != nil {
		return fmt.Errorf("parse: %w", err)
	}
	hostname := parsed.Hostname()
	if hostname == "" {
		return fmt.Errorf("hostname is required")
	}
	if !opts.AllowPrivateNetwork {
		if err := rejectPrivateHost(hostname); err != nil {
			return err
		}
	}
	return nil
}

// CanonicalKey returns a stable, compact key for a normalized relay URL suitable
// for database primary keys and deduplication.
func CanonicalKey(normalized string) string {
	h := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(h[:16])
}

func rejectPrivateHost(hostname string) error {
	lower := strings.ToLower(hostname)
	if lower == "localhost" {
		return fmt.Errorf("localhost is not allowed")
	}
	ip := net.ParseIP(hostname)
	if ip == nil {
		host, _, err := net.SplitHostPort(hostname)
		if err == nil {
			ip = net.ParseIP(host)
		}
	}
	if ip != nil {
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
			return fmt.Errorf("private/local IP address %s is not allowed", ip)
		}
	}
	return nil
}
