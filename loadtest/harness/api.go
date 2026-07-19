package main

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"
)

type apiResult struct {
	samples []sample
	names   []string
}

// runAPIClient issues native HTTP reads in round-robin against baseURL until
// ctx is cancelled. Samples recorded before warmupDone are discarded.
func runAPIClient(
	ctx context.Context,
	baseURL string,
	reqs []apiRequest,
	f fixtures,
	timeout time.Duration,
	warmupDone <-chan struct{},
) apiResult {
	res := apiResult{}
	client := &http.Client{Timeout: timeout}
	i := 0
	for {
		if ctx.Err() != nil {
			return res
		}

		req := reqs[i%len(reqs)]
		i++
		s := doAPIRequest(ctx, client, baseURL, req, f)
		if isClosed(warmupDone) {
			res.samples = append(res.samples, s)
			res.names = append(res.names, req.Name)
		}
	}
}

func doAPIRequest(ctx context.Context, client *http.Client, baseURL string, req apiRequest, f fixtures) sample {
	method := req.Method
	if method == "" {
		method = http.MethodGet
	}
	target := strings.TrimRight(baseURL, "/") + resolvePath(req.Path, f)

	start := time.Now()
	httpReq, err := http.NewRequestWithContext(ctx, method, target, nil)
	if err != nil {
		return sample{latency: time.Since(start), ok: false, class: "transport"}
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		class := "transport"
		if ctx.Err() == nil && time.Since(start) >= client.Timeout && client.Timeout > 0 {
			class = "timeout"
		}
		return sample{latency: time.Since(start), ok: false, class: class}
	}
	// Drain and close so the connection can be reused.
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	latency := time.Since(start)

	class := classifyStatus(resp.StatusCode)
	// 2xx and 404 are both "healthy" read outcomes against sparse datasets;
	// only 5xx and transport failures count as errors.
	ok := resp.StatusCode < 500
	return sample{latency: latency, ok: ok, class: class}
}

func classifyStatus(code int) string {
	switch {
	case code >= 500:
		return "5xx"
	case code >= 400:
		return "4xx"
	case code >= 300:
		return "3xx"
	default:
		return "2xx"
	}
}
