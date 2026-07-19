// Command harness is a WS + native-API load generator for NostrMash.
//
// It opens N concurrent WebSocket clients that speak the Primal cache protocol
// (["REQ", subID, {"cache": [verb, params]}] frames) and M concurrent HTTP
// clients that hit native /api/v1 read endpoints, then reports p50/p95/p99
// latency and error rates per channel. Request shapes mirror the golden
// contract fixtures and the WS dispatch registry.
//
// Example:
//
//	go run ./loadtest/harness \
//	  -base-url http://localhost:8080 \
//	  -ws-clients 32 -api-clients 32 -duration 30s \
//	  -out loadtest/results/summary.json
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

type config struct {
	baseURL    string
	wsURL      string
	duration   time.Duration
	warmup     time.Duration
	timeout    time.Duration
	wsClients  int
	apiClients int
	fixtures   fixtures
	scenario   string
	out        string
}

type runReport struct {
	StartedAt  time.Time        `json:"started_at"`
	DurationMS int64            `json:"duration_ms"`
	WSClients  int              `json:"ws_clients"`
	APIClients int              `json:"api_clients"`
	BaseURL    string           `json:"base_url"`
	WSURL      string           `json:"ws_url"`
	Channels   []channelSummary `json:"channels"`
}

func main() {
	cfg := parseFlags()
	if err := run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "loadtest:", err)
		os.Exit(1)
	}
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.baseURL, "base-url", envOr("LOADTEST_BASE_URL", "http://localhost:8080"), "base URL for native API and WS derivation")
	flag.StringVar(&cfg.wsURL, "ws-url", os.Getenv("LOADTEST_WS_URL"), "WS URL (default: derived from base-url + /primal/ws)")
	flag.DurationVar(&cfg.duration, "duration", 30*time.Second, "measurement window duration")
	flag.DurationVar(&cfg.warmup, "warmup", 2*time.Second, "warmup window excluded from stats")
	flag.DurationVar(&cfg.timeout, "timeout", 10*time.Second, "per-request timeout")
	flag.IntVar(&cfg.wsClients, "ws-clients", 16, "concurrent WS clients (0 disables the WS channel)")
	flag.IntVar(&cfg.apiClients, "api-clients", 16, "concurrent API clients (0 disables the API channel)")
	flag.StringVar(&cfg.fixtures.Pubkey, "pubkey", envOr("LOADTEST_PUBKEY", "0000000000000000000000000000000000000000000000000000000000000001"), "pubkey fixture")
	flag.StringVar(&cfg.fixtures.EventID, "event-id", envOr("LOADTEST_EVENT_ID", "0000000000000000000000000000000000000000000000000000000000000002"), "event id fixture")
	flag.StringVar(&cfg.fixtures.Hashtag, "hashtag", envOr("LOADTEST_HASHTAG", "nostr"), "hashtag fixture")
	flag.StringVar(&cfg.fixtures.Query, "query", envOr("LOADTEST_QUERY", "nostr"), "search query fixture")
	flag.StringVar(&cfg.scenario, "scenario", "", "optional scenario JSON file overriding default request shapes")
	flag.StringVar(&cfg.out, "out", "", "optional path to write the JSON summary")
	flag.Parse()

	if strings.TrimSpace(cfg.wsURL) == "" {
		cfg.wsURL = deriveWSURL(cfg.baseURL)
	}
	return cfg
}

func run(cfg config) error {
	sc := defaultScenario()
	if cfg.scenario != "" {
		loaded, err := loadScenario(cfg.scenario)
		if err != nil {
			return err
		}
		sc = loaded
	}

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	total := cfg.warmup + cfg.duration
	ctx, cancel := context.WithTimeout(rootCtx, total)
	defer cancel()

	warmupDone := make(chan struct{})
	startedAt := time.Now()

	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		wsAll  wsResult
		apiAll apiResult
	)

	for c := 0; c < cfg.wsClients; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := runWSClient(ctx, cfg.wsURL, sc.WS, cfg.fixtures, cfg.timeout, warmupDone)
			mu.Lock()
			wsAll.samples = append(wsAll.samples, r.samples...)
			wsAll.names = append(wsAll.names, r.names...)
			mu.Unlock()
		}()
	}
	for c := 0; c < cfg.apiClients; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := runAPIClient(ctx, cfg.baseURL, sc.API, cfg.fixtures, cfg.timeout, warmupDone)
			mu.Lock()
			apiAll.samples = append(apiAll.samples, r.samples...)
			apiAll.names = append(apiAll.names, r.names...)
			mu.Unlock()
		}()
	}

	fmt.Fprintf(os.Stderr, "loadtest: warming up for %s...\n", cfg.warmup)
	if !sleepCtx(ctx, cfg.warmup) {
		// Cancelled during warmup: still close and let workers drain.
		close(warmupDone)
		wg.Wait()
		return ctx.Err()
	}
	measureStart := time.Now()
	close(warmupDone)
	fmt.Fprintf(os.Stderr, "loadtest: measuring for %s (ws-clients=%d api-clients=%d)...\n", cfg.duration, cfg.wsClients, cfg.apiClients)

	wg.Wait()
	elapsed := time.Since(measureStart)

	report := runReport{
		StartedAt:  startedAt,
		DurationMS: elapsed.Milliseconds(),
		WSClients:  cfg.wsClients,
		APIClients: cfg.apiClients,
		BaseURL:    cfg.baseURL,
		WSURL:      cfg.wsURL,
	}
	if cfg.wsClients > 0 {
		report.Channels = append(report.Channels, summarizeChannel("ws", elapsed, wsAll.samples, wsAll.names))
	}
	if cfg.apiClients > 0 {
		report.Channels = append(report.Channels, summarizeChannel("api", elapsed, apiAll.samples, apiAll.names))
	}

	printReport(report)
	if cfg.out != "" {
		if err := writeJSON(cfg.out, report); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "loadtest: wrote summary to %s\n", cfg.out)
	}
	return nil
}

func printReport(report runReport) {
	fmt.Printf("\n=== NostrMash load test ===\n")
	fmt.Printf("target:   %s (ws: %s)\n", report.BaseURL, report.WSURL)
	fmt.Printf("window:   %d ms   ws-clients=%d api-clients=%d\n\n", report.DurationMS, report.WSClients, report.APIClients)
	for _, ch := range report.Channels {
		fmt.Printf("[%s] total=%d ok=%d errors=%d error_rate=%.2f%% throughput=%.1f req/s\n",
			ch.Name, ch.Total, ch.OK, ch.Errors, ch.ErrorRate*100, ch.Throughput)
		fmt.Printf("      latency  p50=%s  p95=%s  p99=%s  max=%s\n",
			ch.Latency.P50.Round(time.Microsecond),
			ch.Latency.P95.Round(time.Microsecond),
			ch.Latency.P99.Round(time.Microsecond),
			ch.Latency.Max.Round(time.Microsecond))
		if len(ch.ByClass) > 0 {
			fmt.Printf("      classes  %s\n", formatCounts(ch.ByClass))
		}
	}
	fmt.Println()
}

func formatCounts(counts map[string]int) string {
	parts := make([]string, 0, len(counts))
	for k, v := range counts {
		parts = append(parts, fmt.Sprintf("%s=%d", k, v))
	}
	return strings.Join(parts, " ")
}

func writeJSON(path string, report runReport) error {
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644)
}

func deriveWSURL(baseURL string) string {
	ws := baseURL
	switch {
	case strings.HasPrefix(ws, "https://"):
		ws = "wss://" + strings.TrimPrefix(ws, "https://")
	case strings.HasPrefix(ws, "http://"):
		ws = "ws://" + strings.TrimPrefix(ws, "http://")
	}
	return strings.TrimRight(ws, "/") + "/primal/ws"
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}
