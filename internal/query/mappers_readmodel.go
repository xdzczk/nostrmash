package query

import (
	"context"
	"time"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

// mapSlice applies a readmodel→query mapper across a slice, preserving nil vs.
// empty semantics (a nil input yields a nil result).
func mapSlice[T any, U any](in []T, f func(T) U) []U {
	if in == nil {
		return nil
	}
	out := make([]U, 0, len(in))
	for _, v := range in {
		out = append(out, f(v))
	}
	return out
}

// queryTrendingNotesFetch wraps a readmodel-shaped trending-notes fetch as the
// query-shaped plainTrendingFetch consumed by the trust-aware discovery
// pipeline. The trust pipeline operates on query DTOs, so mapping happens as
// rows are pulled.
func queryTrendingNotesFetch(
	fetch func(ctx context.Context, window time.Duration, limit int, offset int) ([]readmodel.TrendingNote, error),
) plainTrendingFetch {
	if fetch == nil {
		return nil
	}
	return func(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingNote, error) {
		rows, err := fetch(ctx, window, limit, offset)
		if err != nil {
			return nil, err
		}
		return mapSlice(rows, trendingNoteFromStore), nil
	}
}

// queryTrustQualifiedNotesFetch wraps a readmodel-shaped trust-qualified fetch
// as the query-shaped trustQualifiedTrendingFetch, mapping the qualification
// policy into its readmodel form and the qualified rows into candidates.
func queryTrustQualifiedNotesFetch(cap trustQualifiedTrendingNotesCapability) trustQualifiedTrendingFetch {
	if cap == nil {
		return nil
	}
	return func(
		ctx context.Context,
		window time.Duration,
		limit int,
		offset int,
		mode string,
		policy TrustQualificationPolicy,
		maxStaleness time.Duration,
	) ([]trustedNoteCandidate, bool, error) {
		rows, ready, err := cap.GetTrustQualifiedTrendingNotes(
			ctx, window, limit, offset, mode, trustQualificationPolicyToStore(policy), maxStaleness,
		)
		if err != nil {
			return nil, false, err
		}
		out := make([]trustedNoteCandidate, 0, len(rows))
		for _, row := range rows {
			out = append(out, trustedNoteCandidate{
				note:    trendingNoteFromStore(row.Note),
				trusted: row.Trusted,
			})
		}
		return out, ready, nil
	}
}

// queryTrendingProfilesFetch wraps a readmodel-shaped profile fetch as the
// query-shaped callback the profile discovery pipeline expects.
func queryTrendingProfilesFetch(
	fetch func(ctx context.Context, window time.Duration, limit int, offset int) ([]readmodel.TrendingProfile, error),
) func(context.Context, time.Duration, int, int) ([]TrendingProfile, error) {
	if fetch == nil {
		return nil
	}
	return func(ctx context.Context, window time.Duration, limit int, offset int) ([]TrendingProfile, error) {
		rows, err := fetch(ctx, window, limit, offset)
		if err != nil {
			return nil, err
		}
		return mapSlice(rows, trendingProfileFromStore), nil
	}
}

func trustQualificationPolicyToStore(policy TrustQualificationPolicy) readmodel.TrustQualificationPolicy {
	return readmodel.TrustQualificationPolicy{
		MaxHops:      policy.MaxHops,
		MinimumScore: policy.MinimumScore,
	}
}
