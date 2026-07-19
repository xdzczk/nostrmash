package main

import (
	"testing"
	"time"
)

func ms(v int) time.Duration { return time.Duration(v) * time.Millisecond }

func TestPercentileNearestRank(t *testing.T) {
	sorted := []time.Duration{ms(1), ms(2), ms(3), ms(4), ms(5), ms(6), ms(7), ms(8), ms(9), ms(10)}
	cases := []struct {
		p    float64
		want time.Duration
	}{
		{0, ms(1)},
		{10, ms(1)},
		{50, ms(5)},
		{95, ms(10)},
		{99, ms(10)},
		{100, ms(10)},
	}
	for _, tc := range cases {
		if got := percentile(sorted, tc.p); got != tc.want {
			t.Errorf("percentile(%.0f) = %v, want %v", tc.p, got, tc.want)
		}
	}
}

func TestPercentileEmptyAndSingle(t *testing.T) {
	if got := percentile(nil, 95); got != 0 {
		t.Errorf("percentile(nil) = %v, want 0", got)
	}
	if got := percentile([]time.Duration{ms(42)}, 99); got != ms(42) {
		t.Errorf("percentile(single, 99) = %v, want %v", got, ms(42))
	}
}

func TestSummarizeLatencies(t *testing.T) {
	// Deliberately unsorted input to confirm we sort a copy.
	in := []time.Duration{ms(5), ms(1), ms(3), ms(2), ms(4)}
	got := summarizeLatencies(in)
	if got.Count != 5 {
		t.Fatalf("count = %d, want 5", got.Count)
	}
	if got.Min != ms(1) || got.Max != ms(5) {
		t.Fatalf("min/max = %v/%v, want 1ms/5ms", got.Min, got.Max)
	}
	if got.Mean != ms(3) {
		t.Fatalf("mean = %v, want 3ms", got.Mean)
	}
	if got.P50 != ms(3) {
		t.Fatalf("p50 = %v, want 3ms", got.P50)
	}
	// Confirm the caller's slice was not mutated.
	if in[0] != ms(5) {
		t.Fatalf("input slice mutated: %v", in)
	}
}

func TestSummarizeChannel(t *testing.T) {
	samples := []sample{
		{latency: ms(10), ok: true, class: "2xx"},
		{latency: ms(20), ok: true, class: "2xx"},
		{latency: ms(30), ok: false, class: "5xx"},
	}
	names := []string{"a", "a", "b"}
	got := summarizeChannel("api", time.Second, samples, names)
	if got.Total != 3 || got.OK != 2 || got.Errors != 1 {
		t.Fatalf("totals wrong: %+v", got)
	}
	if got.ErrorRate < 0.33 || got.ErrorRate > 0.34 {
		t.Fatalf("error rate = %v, want ~0.333", got.ErrorRate)
	}
	if got.Throughput != 3 {
		t.Fatalf("throughput = %v, want 3", got.Throughput)
	}
	if got.ByClass["2xx"] != 2 || got.ByClass["5xx"] != 1 {
		t.Fatalf("by class wrong: %v", got.ByClass)
	}
	if got.ByRequest["a"] != 2 || got.ByRequest["b"] != 1 {
		t.Fatalf("by request wrong: %v", got.ByRequest)
	}
}
