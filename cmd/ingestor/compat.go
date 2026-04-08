package main

import (
	"github.com/xdzczk/nostrmash/internal/config"
	ingestorruntime "github.com/xdzczk/nostrmash/internal/ingestor/runtime"
)

func resolveLiveKinds(cfg config.RelayConfig) ([]int, error) {
	return ingestorruntime.ResolveLiveKinds(cfg)
}

func sortRelaysByWeights(normalized []string, baseOrder map[string]int, weights map[string]float64) []string {
	return ingestorruntime.SortRelaysByWeights(normalized, baseOrder, weights)
}

func resolveBuildVersion(appVersion string) string {
	return ingestorruntime.ResolveBuildVersion(appVersion, buildVersion)
}
