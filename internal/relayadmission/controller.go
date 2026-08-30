package relayadmission

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/relayregistry"
)

// registryStore is the relay-registry surface admission needs. Concrete
// *relayregistry.Store implements it; tests use a fake.
type registryStore interface {
	ListRelays(ctx context.Context, filter relayregistry.ListFilter) ([]relayregistry.RelayRecord, error)
	SetAdmissionState(
		ctx context.Context,
		urlKey string,
		state relayregistry.AdmissionState,
		score float64,
		scoreComponents json.RawMessage,
	) error
}

// Controller computes scores, applies state transitions, enforces caps,
// and publishes the desired active relay set.
type Controller struct {
	log   *slog.Logger
	store registryStore
	cfg   config.RelayRegistryAdmissionConfig
}

func NewController(
	log *slog.Logger,
	store registryStore,
	cfg config.RelayRegistryAdmissionConfig,
) *Controller {
	return &Controller{log: log, store: store, cfg: cfg}
}

// enforcementListLimit bounds the listings used for tier occupancy and cap
// enforcement. Correct enforcement requires seeing EVERY active/pinned/
// probation row: production ran with a 500-row limit here while the
// mid-cycle active+probation set exceeded it, so the lowest-scored ~300
// active relays were silently truncated out of enforceCaps and escaped the
// MaxTotalActive cap forever (337 relays ingesting against a cap of 20).
// The capped tiers hold ~40 rows in steady state; this limit exists only as
// a pathological-degenerate guard, not as a working assumption.
const enforcementListLimit = 20000

// Run executes one full admission cycle: score all relays, apply transitions, publish set.
func (c *Controller) Run(ctx context.Context) error {
	relays, err := c.store.ListRelays(ctx, relayregistry.ListFilter{Limit: 1000})
	if err != nil {
		return fmt.Errorf("list relays for admission: %w", err)
	}

	// Snapshot tier occupancy before applying transitions so promotions can
	// be gated by remaining capacity. Without this, every cycle promoted
	// hundreds of relays into tiers that were already full and enforceCaps
	// demoted them straight back — ~20k pointless state flips (and log
	// lines, and index churn) per day, plus a window in which the desired
	// active set briefly contained the whole flood.
	occupancy, err := c.loadTierOccupancy(ctx)
	if err != nil {
		return fmt.Errorf("load tier occupancy for admission: %w", err)
	}

	var promotions, demotions int

	for _, rec := range relays {
		sc := ComputeScore(rec)
		newState := c.computeTransition(rec, sc)
		// relays is ordered by score DESC, so when capacity is scarce the
		// highest-scored contenders win the open slots.
		newState = occupancy.gate(c.cfg, rec.AdmissionState, newState)

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

// tierOccupancy tracks how many relays currently occupy the capped tiers so
// the transition loop can refuse promotions into full tiers instead of
// relying on enforceCaps to churn them back out.
type tierOccupancy struct {
	pinned        int
	dynamicActive int
	probation     int
}

// loadTierOccupancy counts the current occupants of the capped tiers.
func (c *Controller) loadTierOccupancy(ctx context.Context) (*tierOccupancy, error) {
	relays, err := c.store.ListRelays(ctx, relayregistry.ListFilter{
		AdmissionStates: []relayregistry.AdmissionState{
			relayregistry.AdmissionActive,
			relayregistry.AdmissionPinned,
			relayregistry.AdmissionProbation,
		},
		Limit: enforcementListLimit,
	})
	if err != nil {
		return nil, err
	}
	occ := &tierOccupancy{}
	for _, r := range relays {
		switch r.AdmissionState {
		case relayregistry.AdmissionPinned:
			occ.pinned++
		case relayregistry.AdmissionActive:
			if r.ManualPolicy == relayregistry.ManualPolicyPinned {
				occ.pinned++
			} else {
				occ.dynamicActive++
			}
		case relayregistry.AdmissionProbation:
			occ.probation++
		}
	}
	return occ, nil
}

// gate applies tier capacity to a proposed transition: promotions into a
// full tier are refused (the relay keeps its current state and competes
// again next cycle), while demotions and lateral moves always pass and
// update the occupancy so later gating decisions in the same cycle see
// them. Manual states (pinned/blocked/draining) are never gated — they are
// operator decisions, and pinned capacity is accounted via cfg instead.
func (o *tierOccupancy) gate(
	cfg config.RelayRegistryAdmissionConfig,
	current, proposed relayregistry.AdmissionState,
) relayregistry.AdmissionState {
	if proposed == current {
		return proposed
	}

	// Free the slot the relay is leaving (only matters if the move goes
	// through; probation→active both frees and consumes, handled below).
	switch {
	case current == relayregistry.AdmissionProbation && proposed == relayregistry.AdmissionActive:
		totalActive := o.pinned + o.dynamicActive
		if totalActive >= cfg.MaxTotalActive || o.dynamicActive >= cfg.MaxDynamicActive {
			return current
		}
		o.probation--
		o.dynamicActive++
		return proposed

	case proposed == relayregistry.AdmissionProbation &&
		(current == relayregistry.AdmissionCandidate || current == relayregistry.AdmissionInactive):
		if o.probation >= cfg.MaxProbation {
			return current
		}
		o.probation++
		return proposed

	case current == relayregistry.AdmissionActive && proposed == relayregistry.AdmissionProbation:
		// Health demotion: always allowed — a failing relay must leave
		// active even if probation is full (enforceCaps trims overflow).
		o.dynamicActive--
		o.probation++
		return proposed

	case current == relayregistry.AdmissionActive:
		o.dynamicActive--
		return proposed

	case current == relayregistry.AdmissionProbation:
		o.probation--
		return proposed
	}

	return proposed
}

func (c *Controller) enforceCaps(ctx context.Context) {
	relays, err := c.store.ListRelays(ctx, relayregistry.ListFilter{
		AdmissionStates: []relayregistry.AdmissionState{
			relayregistry.AdmissionActive,
			relayregistry.AdmissionPinned,
			relayregistry.AdmissionProbation,
		},
		Limit: enforcementListLimit,
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
			// Only real ops pins are protected from cap demotion.
			// source_seed is a bootstrap flag, not a pin — seeds compete.
			if r.ManualPolicy == relayregistry.ManualPolicyPinned {
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
	// Spills into probation and must be counted before MaxProbation is applied.
	if totalActive > c.cfg.MaxTotalActive {
		sort.Slice(dynamicActiveRelays, func(i, j int) bool {
			return dynamicActiveRelays[i].Score < dynamicActiveRelays[j].Score
		})
		excess := totalActive - c.cfg.MaxTotalActive
		for i := 0; i < excess && i < len(dynamicActiveRelays); i++ {
			c.demote(ctx, dynamicActiveRelays[i], relayregistry.AdmissionProbation, "total_active_cap_exceeded")
			probationRelays = append(probationRelays, dynamicActiveRelays[i])
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
			probationRelays = append(probationRelays, dynamicActiveRelays[i])
		}
	}

	// Enforce probation cap (includes relays just spilled from active caps).
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
