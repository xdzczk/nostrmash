package api_primal

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

type mediaResource struct {
	URL      string
	Mimetype string
	Width    int
	Height   int
}

func (g WSGateway) primalV1MediaMetadata(ctx context.Context, kwargs map[string]any) ([]any, error) {
	urls := toStringSlice(kwargs["urls"])
	if len(urls) > 20 {
		urls = urls[:20]
	}
	resources := make([]map[string]any, 0, len(urls))
	stored := map[string]readmodel.MediaMetadata{}
	if rows, err := g.query.MediaMetadataByURLs(ctx, urls); err == nil {
		for _, row := range rows {
			stored[row.URL] = row
		}
	}
	fetched := 0
	for _, rawURL := range urls {
		row, ok := stored[rawURL]
		if !ok && g.opts.MediaFetchEnabled && fetched < 3 && publicHTTPS(rawURL) {
			if loaded, link, err := fetchMediaMetadata(ctx, rawURL); err == nil {
				_ = g.query.SaveMediaMetadata(ctx, loaded)
				if link != nil {
					_ = g.query.SaveLinkMetadata(ctx, *link)
				}
				row = loaded
				ok = true
				fetched++
			}
		}
		item := map[string]any{"url": rawURL}
		if ok {
			item["mimetype"] = row.Mimetype
			item["width"] = row.Width
			item["height"] = row.Height
			item["duration"] = row.Duration
			item["blurhash"] = row.Blurhash
		}
		resources = append(resources, item)
	}
	body := map[string]any{"resources": resources, "thumbnails": map[string]any{}}
	if eventID := strings.TrimSpace(stringValue(kwargs["eid"])); eventID != "" {
		body["event_id"] = eventID
	}
	return []any{kindObject(primalKindMediaMetadata, body)}, nil
}

func mediaResourcesFromEvent(raw json.RawMessage) []mediaResource {
	var payload struct {
		Tags [][]string `json:"tags"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil
	}
	out := make([]mediaResource, 0)
	seen := map[string]struct{}{}
	for _, tag := range payload.Tags {
		if len(tag) < 2 || tag[0] != "imeta" {
			continue
		}
		resource := mediaResource{}
		for _, field := range tag[1:] {
			key, value, ok := strings.Cut(field, " ")
			if !ok {
				continue
			}
			switch key {
			case "url":
				resource.URL = strings.TrimSpace(value)
			case "m":
				resource.Mimetype = strings.TrimSpace(value)
			case "dim":
				width, height, ok := strings.Cut(value, "x")
				if !ok {
					continue
				}
				resource.Width, _ = strconv.Atoi(width)
				resource.Height, _ = strconv.Atoi(height)
			}
		}
		if resource.URL == "" {
			continue
		}
		if _, ok := seen[resource.URL]; ok {
			continue
		}
		seen[resource.URL] = struct{}{}
		out = append(out, resource)
	}
	return out
}

func publicHTTPS(rawURL string) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return false
	}
	host := parsed.Hostname()
	if host == "localhost" || strings.HasSuffix(host, ".local") || strings.HasSuffix(host, ".internal") {
		return false
	}
	if ip := net.ParseIP(host); ip != nil {
		return !ip.IsLoopback() && !ip.IsPrivate() && !ip.IsLinkLocalUnicast() && !ip.IsLinkLocalMulticast()
	}
	return true
}

func fetchMediaMetadata(ctx context.Context, rawURL string) (readmodel.MediaMetadata, *readmodel.LinkMetadata, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return readmodel.MediaMetadata{}, nil, err
	}
	req.Header.Set("User-Agent", "nostrmash-primal-metadata")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return readmodel.MediaMetadata{}, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 256*1024))
	if err != nil {
		return readmodel.MediaMetadata{}, nil, err
	}
	row := readmodel.MediaMetadata{URL: rawURL, Mimetype: resp.Header.Get("Content-Type")}
	if idx := strings.Index(row.Mimetype, ";"); idx >= 0 {
		row.Mimetype = strings.TrimSpace(row.Mimetype[:idx])
	}
	var link *readmodel.LinkMetadata
	if strings.Contains(row.Mimetype, "html") || strings.Contains(strings.ToLower(string(body[:min(len(body), 64)])), "<html") {
		title, desc, image := ogTags(string(body))
		if title != "" || desc != "" || image != "" {
			link = &readmodel.LinkMetadata{URL: rawURL, Title: title, Description: desc, Image: image}
		}
	}
	return row, link, nil
}

func ogTags(html string) (string, string, string) {
	lower := strings.ToLower(html)
	return metaContent(lower, html, "og:title"), metaContent(lower, html, "og:description"), metaContent(lower, html, "og:image")
}

func metaContent(lower string, original string, property string) string {
	idx := strings.Index(lower, `property="`+property+`"`)
	if idx < 0 {
		idx = strings.Index(lower, `name="`+property+`"`)
	}
	if idx < 0 {
		return ""
	}
	snippet := original[idx:min(len(original), idx+400)]
	content := strings.Index(strings.ToLower(snippet), `content="`)
	if content < 0 {
		return ""
	}
	rest := snippet[content+len(`content="`):]
	end := strings.Index(rest, `"`)
	if end < 0 {
		return ""
	}
	return rest[:end]
}
