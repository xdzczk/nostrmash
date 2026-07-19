package main

import (
	"sort"
	"time"
)

// sample is a single recorded request outcome for one load channel.
type sample struct {
	latency time.Duration
	ok      bool
	// class is a coarse outcome bucket used for the human-readable summary
	// ("2xx", "4xx", "5xx", "notice", "timeout", "dial", "transport", "eose").
	class string
}

// latencySummary holds the percentile breakdown for a set of latencies.
type latencySummary struct {
	Count int           `json:"count"`
	Min   time.Duration `json:"min_ns"`
	Max   time.Duration `json:"max_ns"`
	Mean  time.Duration `json:"mean_ns"`
	P50   time.Duration `json:"p50_ns"`
	P95   time.Duration `json:"p95_ns"`
	P99   time.Duration `json:"p99_ns"`
}

// channelSummary aggregates one load channel (WS or API) over a run window.
type channelSummary struct {
	Name       string         `json:"name"`
	Total      int            `json:"total"`
	OK         int            `json:"ok"`
	Errors     int            `json:"errors"`
	ErrorRate  float64        `json:"error_rate"`
	Throughput float64        `json:"throughput_rps"`
	Latency    latencySummary `json:"latency"`
	ByClass    map[string]int `json:"by_class"`
	ByRequest  map[string]int `json:"by_request,omitempty"`
}

// percentile returns the value at the given percentile (0..100) of a sorted
// slice using the nearest-rank method. sorted must be ascending and non-empty.
func percentile(sorted []time.Duration, p float64) time.Duration {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[n-1]
	}
	// Nearest-rank: rank = ceil(p/100 * n), 1-based, clamped to [1, n].
	rank := int(p/100.0*float64(n) + 0.9999999999)
	if rank < 1 {
		rank = 1
	}
	if rank > n {
		rank = n
	}
	return sorted[rank-1]
}

// summarizeLatencies computes the percentile breakdown of the given latencies.
// The input slice is copied before sorting so callers keep their ordering.
func summarizeLatencies(latencies []time.Duration) latencySummary {
	n := len(latencies)
	if n == 0 {
		return latencySummary{}
	}
	sorted := make([]time.Duration, n)
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var total time.Duration
	for _, d := range sorted {
		total += d
	}
	return latencySummary{
		Count: n,
		Min:   sorted[0],
		Max:   sorted[n-1],
		Mean:  total / time.Duration(n),
		P50:   percentile(sorted, 50),
		P95:   percentile(sorted, 95),
		P99:   percentile(sorted, 99),
	}
}

// summarizeChannel folds a batch of samples into a channelSummary. requestNames
// is an optional parallel slice recording which request shape produced each
// sample; when non-nil it must have the same length as samples.
func summarizeChannel(name string, elapsed time.Duration, samples []sample, requestNames []string) channelSummary {
	summary := channelSummary{
		Name:    name,
		ByClass: map[string]int{},
	}
	latencies := make([]time.Duration, 0, len(samples))
	for i, s := range samples {
		summary.Total++
		if s.ok {
			summary.OK++
		} else {
			summary.Errors++
		}
		if s.class != "" {
			summary.ByClass[s.class]++
		}
		latencies = append(latencies, s.latency)
		if requestNames != nil && i < len(requestNames) {
			if summary.ByRequest == nil {
				summary.ByRequest = map[string]int{}
			}
			summary.ByRequest[requestNames[i]]++
		}
	}
	summary.Latency = summarizeLatencies(latencies)
	if summary.Total > 0 {
		summary.ErrorRate = float64(summary.Errors) / float64(summary.Total)
	}
	if elapsed > 0 {
		summary.Throughput = float64(summary.Total) / elapsed.Seconds()
	}
	return summary
}
