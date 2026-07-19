package store

import storetrust "github.com/xdzczk/nostrmash/internal/store/trust"

// The trust bounded context now lives in internal/store/trust. These aliases
// re-export its exported types so existing callers that reference
// store.Trust* keep compiling; the trust methods are promoted onto
// PostgresStore via the embedded *storetrust.Trust.
type (
	TrustGlobalScore                  = storetrust.TrustGlobalScore
	TrustRun                          = storetrust.TrustRun
	TrustState                        = storetrust.TrustState
	TrustQualification                = storetrust.TrustQualification
	TrustQualificationPolicy          = storetrust.TrustQualificationPolicy
	TrustGraphSnapshotRefreshResult   = storetrust.TrustGraphSnapshotRefreshResult
	TrustPubkeyCandidate              = storetrust.TrustPubkeyCandidate
	TrustPubkeyFrontierEntry          = storetrust.TrustPubkeyFrontierEntry
	TrustPubkeyFrontierRefreshResult  = storetrust.TrustPubkeyFrontierRefreshResult
	TrustRelayCandidate               = storetrust.TrustRelayCandidate
	TrustRelayCandidateQuery          = storetrust.TrustRelayCandidateQuery
	TrustRelaySuggestion              = storetrust.TrustRelaySuggestion
	TrustRelaySuggestionRefreshResult = storetrust.TrustRelaySuggestionRefreshResult
)

// Package-level trust helpers re-exported so existing store.* callers keep
// working after the move to the trust bounded-context package.
var (
	NormalizeRelayURLs  = storetrust.NormalizeRelayURLs
	SortRelaysByWeights = storetrust.SortRelaysByWeights
)
