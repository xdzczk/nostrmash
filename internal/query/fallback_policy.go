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
	qualifier          trustQualificationCapability
}

func (s Service) fallbackPolicy() fallbackPolicyRuntime {
	return fallbackPolicyRuntime{
		mode:               s.fallbackFetchMode,
		trustPolicy:        s.fallbackFetchPolicy,
		maxAttempts:        s.fallbackMaxAttempts,
		maxTimeBudget:      s.fallbackMaxTimeBudget,
		allowDirectLookups: s.fallbackDirectLookups,
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
	if p.qualifier == nil {
		return p.admitWithoutTrust(pubkeys, reason)
	}

	rows, err := p.qualifier.GetTrustQualifications(ctx, pubkeys, p.trustPolicy)
	if err != nil {
		return p.admitWithoutTrust(pubkeys, reason)
	}
	allowed := make([]string, 0, len(pubkeys))
	allTrusted := true
	for _, pubkey := range pubkeys {
		row, ok := rows[pubkey]
		trusted := ok && row.Trusted
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
