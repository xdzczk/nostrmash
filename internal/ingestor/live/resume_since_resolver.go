package live

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/ingestor/relay"
	"github.com/xdzczk/nostrmash/internal/model"
)

// ResumeSinceResolver computes live subscription since cursors from checkpoints.
type ResumeSinceResolver struct {
	store                    CheckpointStore
	filterGroup              string
	bootstrapLookbackSeconds int64
	overlapSeconds           int64
	nowFn                    func() time.Time
}

func NewResumeSinceResolver(
	store CheckpointStore,
	filterGroup string,
	bootstrapLookbackSeconds int64,
	overlapSeconds int64,
) (*ResumeSinceResolver, error) {
	if store == nil {
		return nil, fmt.Errorf("checkpoint store is required")
	}
	filterGroup = strings.TrimSpace(filterGroup)
	if filterGroup == "" {
		return nil, fmt.Errorf("filter group is required")
	}
	if bootstrapLookbackSeconds <= 0 {
		return nil, fmt.Errorf("bootstrap lookback must be > 0")
	}
	if overlapSeconds < 0 {
		return nil, fmt.Errorf("resume overlap must be >= 0")
	}
	return &ResumeSinceResolver{
		store:                    store,
		filterGroup:              filterGroup,
		bootstrapLookbackSeconds: bootstrapLookbackSeconds,
		overlapSeconds:           overlapSeconds,
		nowFn:                    time.Now,
	}, nil
}

func (r *ResumeSinceResolver) ResolveSince(
	ctx context.Context,
	relayURL string,
) (relay.SinceResolution, error) {
	checkpoint, err := r.store.GetIngestCheckpoint(ctx, relayURL, model.ModeLive, r.filterGroup)
	if err != nil {
		return relay.SinceResolution{}, fmt.Errorf("load live checkpoint: %w", err)
	}
	if checkpoint != nil && checkpoint.Since != nil {
		since := *checkpoint.Since - r.overlapSeconds
		if since < 0 {
			since = 0
		}
		return relay.SinceResolution{
			Since:                    since,
			Strategy:                 "checkpoint",
			CheckpointSince:          checkpoint.Since,
			BootstrapLookbackSeconds: r.bootstrapLookbackSeconds,
			OverlapSeconds:           r.overlapSeconds,
		}, nil
	}

	since := r.nowFn().UTC().Unix() - r.bootstrapLookbackSeconds
	if since < 0 {
		since = 0
	}
	return relay.SinceResolution{
		Since:                    since,
		Strategy:                 "bootstrap_lookback",
		BootstrapLookbackSeconds: r.bootstrapLookbackSeconds,
		OverlapSeconds:           r.overlapSeconds,
	}, nil
}
