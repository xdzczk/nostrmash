package nip05

import (
	"context"
	"encoding/json"
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

// Status values stored on nip05_verifications.
const (
	StatusPending     = "pending"
	StatusVerified    = "verified"
	StatusMismatch    = "mismatch"
	StatusNotFound    = "not_found"
	StatusUnreachable = "unreachable"
	StatusInvalid     = "invalid"
	StatusBlocked     = "blocked"
)

// Result is one NIP-05 lookup.
type Result struct {
	Status    string
	ErrorCode string
}

// Fetcher loads https://domain/.well-known/nostr.json.
type Fetcher struct {
	client           *http.Client
	allowPrivate     bool
	endpointOverride func(domain, local string) string
}

// NewFetcher refuses private, loopback, and link-local addresses.
func NewFetcher(timeout time.Duration) *Fetcher {
	if timeout <= 0 {
		timeout = 8 * time.Second
	}
	return &Fetcher{client: newClient(timeout, false)}
}

// NewFetcherForTest allows the loopback address httptest serves on.
func NewFetcherForTest(timeout time.Duration) *Fetcher {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &Fetcher{client: newClient(timeout, true), allowPrivate: true}
}

// Check verifies that name@domain publishes pubkey. name may be "_".
func (f *Fetcher) Check(ctx context.Context, local, domain, pubkey string) Result {
	if f == nil || f.client == nil {
		return Result{Status: StatusUnreachable, ErrorCode: "unconfigured"}
	}
	domain = strings.ToLower(strings.TrimSpace(domain))
	local = strings.TrimSpace(local)
	pubkey = strings.ToLower(strings.TrimSpace(pubkey))
	if domain == "" || local == "" || len(pubkey) != 64 {
		return Result{Status: StatusInvalid, ErrorCode: "bad_identifier"}
	}
	if !f.allowPrivate {
		if err := relayurl.HostIsPublic(domain); err != nil {
			return Result{Status: StatusBlocked, ErrorCode: "private_address"}
		}
	}
	endpoint := "https://" + domain + "/.well-known/nostr.json?name=" + url.QueryEscape(local)
	if f.endpointOverride != nil {
		endpoint = f.endpointOverride(domain, local)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return Result{Status: StatusInvalid, ErrorCode: "bad_url"}
	}
	req.Header.Set("Accept", "application/json")
	resp, err := f.client.Do(req)
	if err != nil {
		return Result{Status: StatusUnreachable, ErrorCode: "connect_failed"}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxDocumentBytes+1))
	if err != nil {
		return Result{Status: StatusUnreachable, ErrorCode: "read_failed"}
	}
	if len(body) > maxDocumentBytes {
		return Result{Status: StatusInvalid, ErrorCode: "too_large"}
	}
	switch resp.StatusCode {
	case http.StatusNotFound:
		return Result{Status: StatusNotFound, ErrorCode: "404"}
	case http.StatusOK:
	default:
		return Result{Status: StatusUnreachable, ErrorCode: fmt.Sprintf("http_%d", resp.StatusCode)}
	}
	var doc struct {
		Names map[string]string `json:"names"`
	}
	if err := json.Unmarshal(body, &doc); err != nil || doc.Names == nil {
		return Result{Status: StatusInvalid, ErrorCode: "bad_json"}
	}
	published, ok := doc.Names[local]
	if !ok {
		return Result{Status: StatusMismatch, ErrorCode: "name_missing"}
	}
	if strings.ToLower(strings.TrimSpace(published)) != pubkey {
		return Result{Status: StatusMismatch, ErrorCode: "pubkey_mismatch"}
	}
	return Result{Status: StatusVerified}
}

func newClient(timeout time.Duration, allowPrivate bool) *http.Client {
	dialer := &net.Dialer{Timeout: timeout}
	transport := &http.Transport{
		Proxy:                 nil,
		ResponseHeaderTimeout: timeout,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   timeout,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			if err := checkHost(ctx, host, allowPrivate); err != nil {
				return nil, err
			}
			return dialer.DialContext(ctx, network, net.JoinHostPort(host, port))
		},
	}
	return &http.Client{
		Timeout:   timeout,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 2 {
				return errors.New("redirect_rejected")
			}
			if req.URL.Scheme != "https" && !allowPrivate {
				return errors.New("redirect_rejected")
			}
			if !allowPrivate {
				if err := relayurl.HostIsPublic(req.URL.Hostname()); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

func checkHost(ctx context.Context, host string, allowPrivate bool) error {
	if allowPrivate {
		return nil
	}
	if err := relayurl.HostIsPublic(host); err != nil {
		return err
	}
	if net.ParseIP(host) != nil {
		return nil
	}
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return err
	}
	if len(ips) == 0 {
		return errors.New("no addresses")
	}
	for _, ip := range ips {
		if err := relayurl.HostIsPublic(ip.IP.String()); err != nil {
			return err
		}
	}
	return nil
}
