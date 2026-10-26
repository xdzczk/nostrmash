package query

import (
	"context"
	"encoding/json"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

// FeedEventsRequest selects a page of stored events for a Primal feed.
type FeedEventsRequest struct {
	Authors        []string
	Kinds          []int
	Since          int64
	Until          int64
	Limit          int
	Offset         int
	IncludeReplies bool
	OnlyReplies    bool
	HasImage       bool
}

type primalV1Reader interface {
	GetFeedEvents(context.Context, []string, []int, int64, int64, int, int, bool, bool, bool) ([]json.RawMessage, error)
	GetFollowerCountsByPubkeys(context.Context, []string) ([]readmodel.ProfileFollowerCount, error)
	GetZapTotalsByReceiver(context.Context, string) (readmodel.ZapTotals, error)
	GetOldestEventCreatedAt(context.Context, string) (int64, bool, error)
	CountEventsByKind(context.Context, string, int) (int64, error)
	GetEventRelayHints(context.Context, []string) ([]readmodel.EventRelayHint, error)
	GetViewerEventActions(context.Context, string, []string) ([]readmodel.ViewerEventAction, error)
	GetTopZapsByEvents(context.Context, []string, int) ([]readmodel.EventZap, error)
	ListDeletedEventIDs(context.Context, []string) ([]string, error)
	GetAppSettings(context.Context, string) (json.RawMessage, string, error)
	PutAppSettings(context.Context, string, string, json.RawMessage) error
	GetAppSubsettings(context.Context, string, string) (json.RawMessage, error)
	PutAppSubsettings(context.Context, string, string, json.RawMessage) error
	ListPrimalNotifications(context.Context, string, []int, int64, int64, int, int) ([]readmodel.PrimalNotification, error)
	GetNotificationsSeen(context.Context, string) (int64, error)
	SetNotificationsSeen(context.Context, string, int64) error
	ListNotificationCounts(context.Context, string) ([]readmodel.NotificationCount, error)
	GetMediaMetadataByURLs(context.Context, []string) ([]readmodel.MediaMetadata, error)
	PutMediaMetadata(context.Context, readmodel.MediaMetadata) error
	GetLinkMetadataByURLs(context.Context, []string) ([]readmodel.LinkMetadata, error)
	PutLinkMetadata(context.Context, readmodel.LinkMetadata) error
}

func (s Service) primalV1() (primalV1Reader, error) {
	adapter, ok := s.reader.(readModelReaderAdapter)
	if !ok {
		return nil, unsupportedCapabilityError("primal v1")
	}
	reader, ok := adapter.readModel.(primalV1Reader)
	if !ok {
		return nil, unsupportedCapabilityError("primal v1")
	}
	return reader, nil
}

// GetEventCountsBatch loads action counts for many events.
func (s Service) GetEventCountsBatch(ctx context.Context, ids []string) (map[string]EventCounts, error) {
	adapter, ok := s.reader.(readModelReaderAdapter)
	if !ok {
		return nil, unsupportedCapabilityError("event counts")
	}
	return adapter.GetEventCountsByIDs(ctx, ids)
}

// GetFeedEvents returns a raw-event page for a Primal feed.
func (s Service) GetFeedEvents(ctx context.Context, req FeedEventsRequest) ([]json.RawMessage, error) {
	reader, err := s.primalV1()
	if err != nil {
		return nil, err
	}
	return reader.GetFeedEvents(ctx, req.Authors, req.Kinds, req.Since, req.Until, req.Limit, req.Offset, req.IncludeReplies, req.OnlyReplies, req.HasImage)
}

// GetFollowerCounts returns follower totals keyed by pubkey.
func (s Service) GetFollowerCounts(ctx context.Context, pubkeys []string) (map[string]int64, error) {
	reader, err := s.primalV1()
	if err != nil {
		return nil, err
	}
	rows, err := reader.GetFollowerCountsByPubkeys(ctx, pubkeys)
	if err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(rows))
	for _, row := range rows {
		out[row.Pubkey] = row.FollowerCount
	}
	return out, nil
}

// GetZapTotals returns received zap count and sats for pubkey.
func (s Service) GetZapTotals(ctx context.Context, pubkey string) (readmodel.ZapTotals, error) {
	reader, err := s.primalV1()
	if err != nil {
		return readmodel.ZapTotals{}, err
	}
	return reader.GetZapTotalsByReceiver(ctx, pubkey)
}

// GetOldestCreatedAt returns the earliest stored created_at for pubkey.
func (s Service) GetOldestCreatedAt(ctx context.Context, pubkey string) (int64, bool, error) {
	reader, err := s.primalV1()
	if err != nil {
		return 0, false, err
	}
	return reader.GetOldestEventCreatedAt(ctx, pubkey)
}

// CountAuthorKind counts stored events of one kind for pubkey.
func (s Service) CountAuthorKind(ctx context.Context, pubkey string, kind int) (int64, error) {
	reader, err := s.primalV1()
	if err != nil {
		return 0, err
	}
	return reader.CountEventsByKind(ctx, pubkey, kind)
}

// GetRelayHints returns one relay URL per event id.
func (s Service) GetRelayHints(ctx context.Context, ids []string) (map[string]string, error) {
	reader, err := s.primalV1()
	if err != nil {
		return nil, err
	}
	rows, err := reader.GetEventRelayHints(ctx, ids)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(rows))
	for _, row := range rows {
		out[row.EventID] = row.RelayURL
	}
	return out, nil
}

// GetViewerActions reports the viewer's engagement with the given events.
func (s Service) GetViewerActions(ctx context.Context, viewer string, ids []string) ([]readmodel.ViewerEventAction, error) {
	reader, err := s.primalV1()
	if err != nil {
		return nil, err
	}
	return reader.GetViewerEventActions(ctx, viewer, ids)
}

// GetTopEventZaps returns the highest-sats zaps for the given events.
func (s Service) GetTopEventZaps(ctx context.Context, ids []string, limitPerEvent int) ([]readmodel.EventZap, error) {
	reader, err := s.primalV1()
	if err != nil {
		return nil, err
	}
	return reader.GetTopZapsByEvents(ctx, ids, limitPerEvent)
}

// DeletedEventIDs returns the subset of ids that have a deletion record.
func (s Service) DeletedEventIDs(ctx context.Context, ids []string) ([]string, error) {
	reader, err := s.primalV1()
	if err != nil {
		return nil, err
	}
	return reader.ListDeletedEventIDs(ctx, ids)
}

// GetAppSettings returns stored client settings JSON.
func (s Service) GetAppSettings(ctx context.Context, pubkey string) (json.RawMessage, string, error) {
	reader, err := s.primalV1()
	if err != nil {
		return nil, "", err
	}
	return reader.GetAppSettings(ctx, pubkey)
}

// PutAppSettings stores client settings JSON.
func (s Service) PutAppSettings(ctx context.Context, pubkey string, client string, settings json.RawMessage) error {
	reader, err := s.primalV1()
	if err != nil {
		return err
	}
	return reader.PutAppSettings(ctx, pubkey, client, settings)
}

// GetAppSubsettings returns stored subsettings JSON.
func (s Service) GetAppSubsettings(ctx context.Context, pubkey string, subkey string) (json.RawMessage, error) {
	reader, err := s.primalV1()
	if err != nil {
		return nil, err
	}
	return reader.GetAppSubsettings(ctx, pubkey, subkey)
}

// PutAppSubsettings stores subsettings JSON.
func (s Service) PutAppSubsettings(ctx context.Context, pubkey string, subkey string, settings json.RawMessage) error {
	reader, err := s.primalV1()
	if err != nil {
		return err
	}
	return reader.PutAppSubsettings(ctx, pubkey, subkey, settings)
}

// ListNotifications returns a page of stored notifications.
func (s Service) ListNotifications(ctx context.Context, pubkey string, types []int, since, until int64, limit, offset int) ([]readmodel.PrimalNotification, error) {
	reader, err := s.primalV1()
	if err != nil {
		return nil, err
	}
	return reader.ListPrimalNotifications(ctx, pubkey, types, since, until, limit, offset)
}

// NotificationsSeen returns the viewer's seen-until timestamp.
func (s Service) NotificationsSeen(ctx context.Context, pubkey string) (int64, error) {
	reader, err := s.primalV1()
	if err != nil {
		return 0, err
	}
	return reader.GetNotificationsSeen(ctx, pubkey)
}

// MarkNotificationsSeen records seen-until and clears unread counts.
func (s Service) MarkNotificationsSeen(ctx context.Context, pubkey string, seenUntil int64) error {
	reader, err := s.primalV1()
	if err != nil {
		return err
	}
	return reader.SetNotificationsSeen(ctx, pubkey, seenUntil)
}

// NotificationCounts returns nonzero unread counters.
func (s Service) NotificationCounts(ctx context.Context, pubkey string) ([]readmodel.NotificationCount, error) {
	reader, err := s.primalV1()
	if err != nil {
		return nil, err
	}
	return reader.ListNotificationCounts(ctx, pubkey)
}

// MediaMetadataByURLs loads stored media rows.
func (s Service) MediaMetadataByURLs(ctx context.Context, urls []string) ([]readmodel.MediaMetadata, error) {
	reader, err := s.primalV1()
	if err != nil {
		return nil, err
	}
	return reader.GetMediaMetadataByURLs(ctx, urls)
}

// SaveMediaMetadata upserts one media row.
func (s Service) SaveMediaMetadata(ctx context.Context, row readmodel.MediaMetadata) error {
	reader, err := s.primalV1()
	if err != nil {
		return err
	}
	return reader.PutMediaMetadata(ctx, row)
}

// LinkMetadataByURLs loads stored link previews.
func (s Service) LinkMetadataByURLs(ctx context.Context, urls []string) ([]readmodel.LinkMetadata, error) {
	reader, err := s.primalV1()
	if err != nil {
		return nil, err
	}
	return reader.GetLinkMetadataByURLs(ctx, urls)
}

// SaveLinkMetadata upserts one link preview.
func (s Service) SaveLinkMetadata(ctx context.Context, row readmodel.LinkMetadata) error {
	reader, err := s.primalV1()
	if err != nil {
		return err
	}
	return reader.PutLinkMetadata(ctx, row)
}
