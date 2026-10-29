package relaynip11

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/relayurl"
)

const maxDocumentBytes = 64 * 1024

// FetchError is a stable, log-safe failure code. The wrapped transport error
// is not retained, so relay pages never echo upstream addresses.
type FetchError struct {
	Code string
}

func (e *FetchError) Error() string {
	if e == nil || e.Code == "" {
		return "connect_failed"
	}
	return e.Code
}

var (
	errRedirectRejected = errors.New("redirect_rejected")
	errPrivateAddress   = errors.New("private_address")
)

// Fetcher loads a relay's NIP-11 document over HTTP.
type Fetcher struct {
	client   *http.Client
	timeout  time.Duration
	maxBytes int
}

// NewFetcher builds a client that refuses private and loopback targets.
func NewFetcher(timeout time.Duration) *Fetcher {
	if timeout <= 0 {
		timeout = 8 * time.Second
	}
	return &Fetcher{
		client:   newPublicHTTPClient(timeout),
		timeout:  timeout,
		maxBytes: maxDocumentBytes,
	}
}

// Fetch requests the NIP-11 document for a normalized websocket relay URL.
func (f *Fetcher) Fetch(ctx context.Context, relayURL string) (Info, error) {
	docURL, err := DocumentURL(relayURL)
	if err != nil {
		return Info{}, &FetchError{Code: "connect_failed"}
	}
	parsed, err := url.Parse(docURL)
	if err != nil {
		return Info{}, &FetchError{Code: "connect_failed"}
	}
	if err := relayurl.HostIsPublic(parsed.Hostname()); err != nil {
		return Info{}, &FetchError{Code: "private_address"}
	}
	return f.fetchURL(ctx, docURL)
}

// DocumentURL is the HTTP(S) URL of the relay's NIP-11 document.
func DocumentURL(relayURL string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(relayURL))
	if err != nil || parsed.Host == "" {
		return "", fmt.Errorf("invalid relay url")
	}
	switch strings.ToLower(parsed.Scheme) {
	case "wss":
		parsed.Scheme = "https"
	case "ws":
		parsed.Scheme = "http"
	default:
		return "", fmt.Errorf("invalid relay scheme")
	}
	return parsed.String(), nil
}

func (f *Fetcher) fetchURL(ctx context.Context, docURL string) (Info, error) {
	if f.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, f.timeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, docURL, nil)
	if err != nil {
		return Info{}, &FetchError{Code: "connect_failed"}
	}
	req.Header.Set("Accept", "application/nostr+json")
	req.Header.Set("User-Agent", "NostrMash")

	resp, err := f.client.Do(req)
	if err != nil {
		if resp != nil && resp.Body != nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
		}
		return Info{}, classifyTransport(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return Info{}, httpStatusError(resp.StatusCode)
	}
	if isHTML(resp.Header.Get("Content-Type")) {
		return Info{}, errNotJSON
	}
	limit := f.maxBytes
	if limit <= 0 {
		limit = maxDocumentBytes
	}
	if resp.ContentLength > int64(limit) {
		return Info{}, &FetchError{Code: "too_large"}
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, int64(limit)+1))
	if err != nil {
		return Info{}, classifyTransport(err)
	}
	if len(body) > limit {
		return Info{}, &FetchError{Code: "too_large"}
	}
	return Parse(body)
}

func httpStatusError(status int) *FetchError {
	if status < 100 || status > 599 {
		return &FetchError{Code: "http_error"}
	}
	return &FetchError{Code: fmt.Sprintf("http_%d", status)}
}

func isHTML(contentType string) bool {
	contentType = strings.ToLower(contentType)
	return strings.Contains(contentType, "text/html") || strings.Contains(contentType, "application/xhtml")
}

func classifyTransport(err error) error {
	switch {
	case errors.Is(err, errRedirectRejected):
		return &FetchError{Code: "redirect_rejected"}
	case errors.Is(err, errPrivateAddress):
		return &FetchError{Code: "private_address"}
	case errors.Is(err, context.DeadlineExceeded):
		return &FetchError{Code: "timeout"}
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return &FetchError{Code: "timeout"}
	}
	return &FetchError{Code: "connect_failed"}
}

func newPublicHTTPClient(timeout time.Duration) *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return publicDialContext(ctx, timeout, network, addr)
		},
		ForceAttemptHTTP2:     false,
		TLSHandshakeTimeout:   timeout,
		ResponseHeaderTimeout: timeout,
		DisableKeepAlives:     true,
		MaxIdleConns:          0,
	}
	return &http.Client{
		Timeout:       timeout,
		Transport:     transport,
		CheckRedirect: checkRedirect,
	}
}

func checkRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= 2 {
		return errRedirectRejected
	}
	if req.URL == nil || len(via) == 0 || via[0].URL == nil {
		return errRedirectRejected
	}
	switch strings.ToLower(req.URL.Scheme) {
	case "https":
	case "http":
		if strings.EqualFold(via[0].URL.Scheme, "https") {
			return errRedirectRejected
		}
	default:
		return errRedirectRejected
	}
	if req.URL.User != nil || !strings.EqualFold(req.URL.Hostname(), via[0].URL.Hostname()) {
		return errRedirectRejected
	}
	if err := relayurl.HostIsPublic(req.URL.Hostname()); err != nil {
		return errRedirectRejected
	}
	return nil
}

func publicDialContext(ctx context.Context, timeout time.Duration, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	ip, err := publicIP(ctx, host)
	if err != nil {
		return nil, err
	}
	dialer := net.Dialer{Timeout: timeout}
	return dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
}

func publicIP(ctx context.Context, host string) (net.IP, error) {
	if ip := net.ParseIP(host); ip != nil {
		if err := relayurl.HostIsPublic(ip.String()); err != nil {
			return nil, errPrivateAddress
		}
		return ip, nil
	}
	if err := relayurl.HostIsPublic(host); err != nil {
		return nil, errPrivateAddress
	}
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	for _, candidate := range ips {
		if relayurl.HostIsPublic(candidate.IP.String()) == nil {
			return candidate.IP, nil
		}
	}
	if len(ips) > 0 {
		return nil, errPrivateAddress
	}
	return nil, fmt.Errorf("no address for host")
}
