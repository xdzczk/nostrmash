package relayurl

import (
	"fmt"
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
