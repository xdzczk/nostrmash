package relayadmission

import (
	"context"
	"fmt"
	"log/slog"
	"sort"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/relayregistry"
)

// Controller computes scores, applies state transitions, enforces caps,
// and publishes the desired active relay set.
type Controller struct {
	log   *slog.Logger
	store *relayregistry.Store
	cfg   config.RelayRegistryAdmissionConfig
}

func NewController(
	log *slog.Logger,
	store *relayregistry.Store,
	cfg config.RelayRegistryAdmissionConfig,
) *Controller {
	return &Controller{log: log, store: store, cfg: cfg}
}

// Run executes one full admission cycle: score all relays, apply transitions, publish set.
func (c *Controller) Run(ctx context.Context) error {
	relays, err := c.store.ListRelays(ctx, relayregistry.ListFilter{Limit: 1000})
	if err != nil {
		return fmt.Errorf("list relays for admission: %w", err)
	}

	var promotions, demotions int

	for _, rec := range relays {
		sc := ComputeScore(rec)
		newState := c.computeTransition(rec, sc)

		if newState != rec.AdmissionState || sc.TotalScore != rec.Score {
			if err := c.store.SetAdmissionState(
				ctx, rec.URLKey, newState, sc.TotalScore, MarshalComponents(sc),
			); err != nil {
				c.log.Warn("relay_admission_state_update_failed",
					"relay", rec.NormalizedURL,
					"error", err,
				)
				continue
			}
			if newState != rec.AdmissionState {
				c.log.Info("relay_admission_state_transition",
					"relay", rec.NormalizedURL,
					"from", string(rec.AdmissionState),
					"to", string(newState),
					"score", sc.TotalScore,
				)
				if isPromotion(rec.AdmissionState, newState) {
					promotions++
					metrics.IncRelayAdmissionChange("promotion")
				} else {
					demotions++
					metrics.IncRelayAdmissionChange("demotion")
				}
			}
		}
	}

	c.enforceCaps(ctx)

	c.emitStateMetrics(ctx)

	c.log.Info("relay_admission_cycle_completed",
		"total_relays", len(relays),
		"promotions", promotions,
		"demotions", demotions,
	)
	return nil
}

// RunDryRun returns what the admission controller would do without persisting changes.
func (c *Controller) RunDryRun(ctx context.Context) ([]AdmissionProposal, error) {
	relays, err := c.store.ListRelays(ctx, relayregistry.ListFilter{Limit: 1000})
	if err != nil {
		return nil, fmt.Errorf("list relays for dry-run: %w", err)
	}

	proposals := make([]AdmissionProposal, 0, len(relays))
	for _, rec := range relays {
		sc := ComputeScore(rec)
		newState := c.computeTransition(rec, sc)
		proposals = append(proposals, AdmissionProposal{
			URLKey:        rec.URLKey,
			NormalizedURL: rec.NormalizedURL,
			CurrentState:  rec.AdmissionState,
			ProposedState: newState,
			Score:         sc,
			Changed:       newState != rec.AdmissionState,
		})
	}
	return proposals, nil
}

// AdmissionProposal describes a proposed state change for diagnostics.
type AdmissionProposal struct {
	URLKey        string                       `json:"url_key"`
	NormalizedURL string                       `json:"normalized_url"`
	CurrentState  relayregistry.AdmissionState `json:"current_state"`
	ProposedState relayregistry.AdmissionState `json:"proposed_state"`
	Score         ScoreComponents              `json:"score"`
	Changed       bool                         `json:"changed"`
}

func (c *Controller) computeTransition(rec relayregistry.RelayRecord, sc ScoreComponents) relayregistry.AdmissionState {
	// Manual policy always takes precedence.
	switch rec.ManualPolicy {
	case relayregistry.ManualPolicyBlocked:
		return relayregistry.AdmissionBlocked
	case relayregistry.ManualPolicyDrained:
		return relayregistry.AdmissionDraining
	case relayregistry.ManualPolicyPinned:
		return relayregistry.AdmissionPinned
	}

	score := sc.TotalScore
	current := rec.AdmissionState

	switch current {
	case relayregistry.AdmissionCandidate:
		if score >= c.cfg.MinScoreForProbation {
			return relayregistry.AdmissionProbation
		}
		return relayregistry.AdmissionCandidate

	case relayregistry.AdmissionProbation:
		if score >= c.cfg.MinScoreForActive {
			return relayregistry.AdmissionActive
		}
		if score < c.cfg.MinScoreForProbation {
			return relayregistry.AdmissionCandidate
		}
		return relayregistry.AdmissionProbation

	case relayregistry.AdmissionActive:
		if rec.ProbeFailRate > c.cfg.DemoteFailureThreshold {
			return relayregistry.AdmissionProbation
		}
		if score < c.cfg.MinScoreForProbation {
			return relayregistry.AdmissionInactive
		}
		return relayregistry.AdmissionActive

	case relayregistry.AdmissionInactive:
		if score >= c.cfg.MinScoreForProbation {
			return relayregistry.AdmissionProbation
		}
		return relayregistry.AdmissionInactive

	case relayregistry.AdmissionPinned:
		return relayregistry.AdmissionPinned

	case relayregistry.AdmissionBlocked:
		return relayregistry.AdmissionBlocked

	case relayregistry.AdmissionDraining:
		return relayregistry.AdmissionDraining
	}

	return current
}

func (c *Controller) enforceCaps(ctx context.Context) {
	relays, err := c.store.ListRelays(ctx, relayregistry.ListFilter{
		AdmissionStates: []relayregistry.AdmissionState{
			relayregistry.AdmissionActive,
			relayregistry.AdmissionPinned,
			relayregistry.AdmissionProbation,
		},
		Limit: 500,
	})
	if err != nil {
		c.log.Error("relay_admission_enforce_caps_list_failed", "error", err)
		return
	}

	var pinnedCount int
	var dynamicActiveRelays, probationRelays []relayregistry.RelayRecord
	for _, r := range relays {
		switch r.AdmissionState {
		case relayregistry.AdmissionPinned:
			pinnedCount++
		case relayregistry.AdmissionActive:
			if r.SourceSeed || r.ManualPolicy == relayregistry.ManualPolicyPinned {
				pinnedCount++
			} else {
				dynamicActiveRelays = append(dynamicActiveRelays, r)
			}
		case relayregistry.AdmissionProbation:
			probationRelays = append(probationRelays, r)
		}
	}

	totalActive := pinnedCount + len(dynamicActiveRelays)

	// Enforce total active cap (pinned + dynamic): demote lowest-scored dynamic relays.
	if totalActive > c.cfg.MaxTotalActive {
		sort.Slice(dynamicActiveRelays, func(i, j int) bool {
			return dynamicActiveRelays[i].Score < dynamicActiveRelays[j].Score
		})
		excess := totalActive - c.cfg.MaxTotalActive
		for i := 0; i < excess && i < len(dynamicActiveRelays); i++ {
			c.demote(ctx, dynamicActiveRelays[i], relayregistry.AdmissionProbation, "total_active_cap_exceeded")
		}
		// Recompute after total cap enforcement.
		dynamicActiveRelays = dynamicActiveRelays[min(excess, len(dynamicActiveRelays)):]
	}

	// Enforce dynamic active cap.
	if len(dynamicActiveRelays) > c.cfg.MaxDynamicActive {
		sort.Slice(dynamicActiveRelays, func(i, j int) bool {
			return dynamicActiveRelays[i].Score < dynamicActiveRelays[j].Score
		})
		excess := len(dynamicActiveRelays) - c.cfg.MaxDynamicActive
		for i := 0; i < excess; i++ {
			c.demote(ctx, dynamicActiveRelays[i], relayregistry.AdmissionProbation, "dynamic_cap_exceeded")
		}
	}

	// Enforce probation cap.
	if len(probationRelays) > c.cfg.MaxProbation {
		sort.Slice(probationRelays, func(i, j int) bool {
			return probationRelays[i].Score < probationRelays[j].Score
		})
		excess := len(probationRelays) - c.cfg.MaxProbation
		for i := 0; i < excess; i++ {
			c.demote(ctx, probationRelays[i], relayregistry.AdmissionInactive, "probation_cap_exceeded")
		}
	}
}

func (c *Controller) demote(ctx context.Context, rec relayregistry.RelayRecord, target relayregistry.AdmissionState, reason string) {
	if err := c.store.SetAdmissionState(ctx, rec.URLKey, target, rec.Score, nil); err != nil {
		c.log.Warn("relay_admission_cap_demote_failed",
			"relay", rec.NormalizedURL,
			"error", err,
		)
		return
	}
	c.log.Info("relay_admission_cap_demoted",
		"relay", rec.NormalizedURL,
		"from", string(rec.AdmissionState),
		"to", string(target),
		"reason", reason,
	)
}

func (c *Controller) emitStateMetrics(ctx context.Context) {
	if c.store == nil {
		return
	}
	allRelays, err := c.store.ListRelays(ctx, relayregistry.ListFilter{Limit: 1000})
	if err != nil {
		return
	}
	counts := make(map[string]float64)
	for _, r := range allRelays {
		counts[string(r.AdmissionState)]++
	}
	for _, state := range []string{
		"candidate", "probation", "active", "inactive",
		"blocked", "draining", "pinned",
	} {
		metrics.SetRelayRegistryStateCount(state, counts[state])
	}
}

func isPromotion(from, to relayregistry.AdmissionState) bool {
	order := map[relayregistry.AdmissionState]int{
		relayregistry.AdmissionBlocked:   0,
		relayregistry.AdmissionInactive:  1,
		relayregistry.AdmissionCandidate: 2,
		relayregistry.AdmissionDraining:  2,
		relayregistry.AdmissionProbation: 3,
		relayregistry.AdmissionActive:    4,
		relayregistry.AdmissionPinned:    5,
	}
	return order[to] > order[from]
}
