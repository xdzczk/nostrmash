package query

import (
	"context"
	"time"
)

type fallbackLookupReason string

const (
	fallbackLookupDirect           fallbackLookupReason = "direct"
	fallbackLookupDiscoveryMiss    fallbackLookupReason = "discovery_miss"
	fallbackLookupSearchMiss       fallbackLookupReason = "search_miss"
	fallbackLookupThreadCompletion fallbackLookupReason = "thread_completion"
)

type fallbackPolicyRuntime struct {
	mode               string
	trustPolicy        TrustQualificationPolicy
	maxAttempts        int
	maxTimeBudget      time.Duration
	allowDirectLookups bool
	stateReader        trustStateCapability
	qualifier          trustQualificationCapability
}

func (s Service) fallbackPolicy() fallbackPolicyRuntime {
	return fallbackPolicyRuntime{
		mode:               s.fallbackFetchMode,
		trustPolicy:        s.fallbackFetchPolicy,
		maxAttempts:        s.fallbackMaxAttempts,
		maxTimeBudget:      s.fallbackMaxTimeBudget,
		allowDirectLookups: s.fallbackDirectLookups,
		stateReader:        s.capabilities.trust.state,
		qualifier:          s.capabilities.trust.qualification,
	}
}

func (p fallbackPolicyRuntime) eventAdmission(reason fallbackLookupReason) (allowed bool, strict bool) {
	switch p.mode {
	case trustModeTrustedOnly:
		if reason == fallbackLookupDirect && p.allowDirectLookups {
			return true, true
		}
		return false, true
	case trustModePreferTrusted:
		return true, true
	default:
		return true, false
	}
}

func (p fallbackPolicyRuntime) admitProfiles(
	ctx context.Context,
	pubkeys []string,
	reason fallbackLookupReason,
) ([]string, bool) {
	if len(pubkeys) == 0 {
		return nil, false
	}
	if p.mode == trustModeOpen {
		return pubkeys, true
	}
	rows, trustedByState := p.lookupTrust(ctx, pubkeys)
	if rows == nil {
		return p.admitWithoutTrust(pubkeys, reason)
	}
	allowed := make([]string, 0, len(pubkeys))
	allTrusted := true
	for _, pubkey := range pubkeys {
		trusted := trustedByState[pubkey]
		switch p.mode {
		case trustModeTrustedOnly:
			if trusted {
				allowed = append(allowed, pubkey)
				continue
			}
			if reason == fallbackLookupDirect && p.allowDirectLookups {
				allowed = append(allowed, pubkey)
			}
			allTrusted = false
		case trustModePreferTrusted:
			allowed = append(allowed, pubkey)
			if !trusted {
				allTrusted = false
			}
		default:
			allowed = append(allowed, pubkey)
		}
	}
	return allowed, allTrusted
}

func (p fallbackPolicyRuntime) lookupTrust(ctx context.Context, pubkeys []string) (map[string]TrustQualification, map[string]bool) {
	if p.stateReader != nil {
		states, err := p.stateReader.GetTrustStates(ctx, pubkeys)
		if err == nil {
			rows := make(map[string]TrustQualification, len(states))
			trusted := make(map[string]bool, len(states))
			for pubkey, state := range states {
				row := trustQualificationFromState(trustStateFromStore(state), p.trustPolicy)
				rows[pubkey] = row
				trusted[pubkey] = row.Trusted
			}
			return rows, trusted
		}
	}
	if p.qualifier != nil {
		storeRows, err := p.qualifier.GetTrustQualifications(ctx, pubkeys, trustQualificationPolicyToStore(p.trustPolicy))
		if err == nil {
			rows := make(map[string]TrustQualification, len(storeRows))
			trusted := make(map[string]bool, len(storeRows))
			for pubkey, row := range storeRows {
				mapped := trustQualificationFromStore(row)
				rows[pubkey] = mapped
				trusted[pubkey] = mapped.Trusted
			}
			return rows, trusted
		}
	}
	return nil, nil
}

func (p fallbackPolicyRuntime) admitWithoutTrust(pubkeys []string, reason fallbackLookupReason) ([]string, bool) {
	switch p.mode {
	case trustModeTrustedOnly:
		if reason == fallbackLookupDirect && p.allowDirectLookups {
			return pubkeys, false
		}
		return nil, false
	case trustModePreferTrusted:
		return pubkeys, false
	default:
		return pubkeys, true
	}
}

func (p fallbackPolicyRuntime) executionBounds(strict bool) (int, time.Duration) {
	attempts := p.maxAttempts
	if attempts <= 0 {
		attempts = 1
	}
	budget := p.maxTimeBudget
	if budget <= 0 {
		budget = 2 * time.Second
	}
	if !strict {
		return attempts, budget
	}
	if attempts > 1 {
		attempts = 1
	}
	if budget > 750*time.Millisecond {
		budget = 750 * time.Millisecond
	}
	return attempts, budget
}

func withFallbackTimeBudget(ctx context.Context, budget time.Duration) (context.Context, context.CancelFunc) {
	if budget <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, budget)
}
