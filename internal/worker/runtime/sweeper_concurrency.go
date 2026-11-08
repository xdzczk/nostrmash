package runtime

import "github.com/xdzczk/nostrmash/internal/config"

// poolAncillaryReserve is the spare connections kept for retention,
// metrics, and one-off queries when the configured sweepers fit.
const poolAncillaryReserve = 4

// fitWorkerSweeperConcurrency lowers author-analytics, profile-stats, and
// Meilisearch sweeper goroutines until bundle workers, sweepers, and a
// spare connection fit in the pool. Bundle concurrency is left as
// configured. When the generous reserve of 4 does not leave a slot for
// every enabled sweeper, the reserve shrinks to 1 so each sweeper still
// runs. A sweeper whose configured concurrency is already inside the
// budget is unchanged.
func fitWorkerSweeperConcurrency(cfg config.WorkerConfig, maxConns int) config.WorkerConfig {
	if maxConns <= 0 {
		return cfg
	}
	bundle := cfg.Concurrency + cfg.LiveConcurrency + cfg.BackfillConcurrency
	if bundle < 0 {
		bundle = 0
	}
	type slot struct {
		requested int
		got       *int
	}
	out := cfg
	var slots []slot
	if cfg.MeilisearchSweeper.Enabled && cfg.MeilisearchSweeper.Concurrency > 0 {
		slots = append(slots, slot{cfg.MeilisearchSweeper.Concurrency, &out.MeilisearchSweeper.Concurrency})
	}
	if cfg.AuthorAnalyticsSweeper.Enabled && cfg.AuthorAnalyticsSweeper.Concurrency > 0 {
		slots = append(slots, slot{cfg.AuthorAnalyticsSweeper.Concurrency, &out.AuthorAnalyticsSweeper.Concurrency})
	}
	if cfg.ProfileStatsSweeper.Enabled && cfg.ProfileStatsSweeper.Concurrency > 0 {
		slots = append(slots, slot{cfg.ProfileStatsSweeper.Concurrency, &out.ProfileStatsSweeper.Concurrency})
	}
	if len(slots) == 0 {
		return out
	}
	requested := 0
	for _, s := range slots {
		requested += s.requested
	}
	budget := maxConns - bundle - poolAncillaryReserve
	if budget < len(slots) {
		budget = maxConns - bundle - 1
	}
	if budget < 0 {
		budget = 0
	}
	if budget >= requested {
		return out
	}
	for i := range slots {
		*slots[i].got = 0
	}
	for i := range slots {
		if budget == 0 {
			break
		}
		*slots[i].got = 1
		budget--
	}
	for budget > 0 {
		progressed := false
		for i := range slots {
			if budget == 0 {
				break
			}
			if *slots[i].got < slots[i].requested {
				*slots[i].got++
				budget--
				progressed = true
			}
		}
		if !progressed {
			break
		}
	}
	return out
}
