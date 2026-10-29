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

// loopbackRelayHostPattern matches the host[:port][/path] of a relay URL
// after the scheme. It covers localhost, 127.0.0.0/8, and [::1].
const loopbackRelayHostPattern = `^(localhost|127\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}|\[::1\])([:/]|$)`

// LoopbackRelayURLExcludedSQL is a SQL boolean expression over a column
// named relay_url. It is true when that URL is not localhost, an address in
// 127.0.0.0/8, or [::1]. NormalizePublic is the full check and also rejects
// other private and link-local hosts.
const LoopbackRelayURLExcludedSQL = `split_part(relay_url, '://', 2) !~* '` + loopbackRelayHostPattern + `'`

// NormalizePublic normalizes a relay URL and rejects hosts that are not
// publicly reachable. Localhost, loopback, link-local, and private-network
// addresses are rejected so client-local URLs such as ws://localhost and
// ws://127.0.0.1 do not rank as network relays. Cleartext ws:// to a public
// host is kept; callers that require TLS should keep using Normalize.
func NormalizePublic(raw string) (string, error) {
	normalized, err := Normalize(raw, NormalizeOptions{})
	if err != nil {
		return "", err
	}
	if err := Validate(normalized, ValidateOptions{}); err != nil {
		return "", err
	}
	return normalized, nil
}

// CanonicalKey returns a stable, compact key for a normalized relay URL suitable
// for database primary keys and deduplication.
func CanonicalKey(normalized string) string {
	h := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(h[:16])
}

// HostIsPublic reports whether hostname may be contacted as a public relay.
// It rejects localhost and loopback, link-local, private, unspecified, and
// multicast addresses. A DNS name that is not itself an IP is accepted; the
// caller still has to check the addresses that name resolves to.
func HostIsPublic(hostname string) error {
	host := hostname
	if ip := net.ParseIP(hostname); ip != nil {
		if ip4 := ip.To4(); ip4 != nil {
			host = ip4.String()
		}
	}
	if err := rejectPrivateHost(host); err != nil {
		return err
	}
	if ip := net.ParseIP(host); ip != nil && (ip.IsUnspecified() || ip.IsMulticast()) {
		return fmt.Errorf("non-public IP address %s is not allowed", ip)
	}
	return nil
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
