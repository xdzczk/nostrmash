package api_primal

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
)

var update = flag.Bool("update", false, "update golden contract fixtures")

type fakeEventReader struct {
	getEventRawByIDFn            func(context.Context, string) (json.RawMessage, error)
	getEventRawsByIDs            func(context.Context, []string) (map[string]json.RawMessage, error)
	getProfileByPubkey           func(context.Context, string) (store.ProfileProjection, error)
	getProfilesByBatch           func(context.Context, []string) (map[string]store.ProfileProjection, error)
	getAuthorEventsFn            func(context.Context, string, int) ([]json.RawMessage, error)
	getAuthorRepliesFn           func(context.Context, string, int) ([]json.RawMessage, error)
	getEventCountsFn             func(context.Context, string) (store.EventCounts, error)
	getEventRepliesFn            func(context.Context, string, int, *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error)
	getEventAncestors            func(context.Context, string, int) ([]json.RawMessage, []string, error)
	getContactListFn             func(context.Context, string) (store.ContactListProjection, error)
	getRelayListFn               func(context.Context, string) (store.RelayListProjection, error)
	searchEventsFn               func(context.Context, string, int) ([]json.RawMessage, error)
	searchProfilesFn             func(context.Context, string, int) ([]store.ProfileProjection, error)
	getByKindPubkeyFn            func(context.Context, int, string, int) ([]json.RawMessage, error)
	getRefsPubkeyFn              func(context.Context, string, int) ([]json.RawMessage, error)
	getFollowersFn               func(context.Context, string, int) ([]json.RawMessage, error)
	getUserZapsFn                func(context.Context, string, int, bool) ([]json.RawMessage, error)
	getEventZapsFn               func(context.Context, string, int) ([]json.RawMessage, error)
	isFollowingFn                func(context.Context, string, string) (bool, error)
	getMutualFollowsFn           func(context.Context, string, string, int) ([]string, error)
	getDMContactsFn              func(context.Context, string, int) ([]string, error)
	getDMContactsDetailedFn      func(context.Context, string, int, int, int64, int64) ([]json.RawMessage, error)
	getDirectMsgsFn              func(context.Context, string, string, int) ([]json.RawMessage, error)
	getDirectMsgsRangeFn         func(context.Context, string, string, int64, int64, int, int) ([]json.RawMessage, error)
	getDMUnreadFn                func(context.Context, string, int) ([]json.RawMessage, error)
	resetDMUnreadFn              func(context.Context, string, string) error
	getDMCountFn                 func(context.Context, string, string) (int64, error)
	resetDMCountFn               func(context.Context, string, string) error
	resetDMCountsFn              func(context.Context, string) error
	getModerationFn              func(context.Context, string, int) ([]string, error)
	getModerationByIdentifierFn  func(context.Context, string, string) ([]string, error)
	isHiddenFn                   func(context.Context, string, string) (bool, string, error)
	getParamListFn               func(context.Context, string, int, int) ([]json.RawMessage, error)
	getParamListByIdentifierFn   func(context.Context, string, int, string, int) ([]json.RawMessage, error)
	getParamEventFn              func(context.Context, string, int, string) (json.RawMessage, error)
	getParamEventsFn             func(context.Context, int, string, int) ([]json.RawMessage, error)
	getHighlightsByEventFn       func(context.Context, string, int) ([]json.RawMessage, error)
	getHighlightsByATargetFn     func(context.Context, int, string, string, int) ([]json.RawMessage, error)
	getEventsByATagAndKindFn     func(context.Context, int, string, int) ([]json.RawMessage, error)
	getNetworkStatsFn            func(context.Context) (store.NetworkStats, error)
	getCuratedValuesFn           func(context.Context, string, string, int) ([]string, error)
	getCuratedRecommendedReadsFn func(context.Context, int) ([]store.CuratedRecommendedRead, error)
	getCuratedReadsTopicsFn      func(context.Context, int) ([]store.CuratedReadsTopic, error)
	getCuratedFeaturedAuthorsFn  func(context.Context, int) ([]store.CuratedFeaturedAuthor, error)
	getCreatorPaidTiersFn        func(context.Context, string) ([]json.RawMessage, error)
	getPubkeyByLNAddressFn       func(context.Context, string) (string, error)
}

func (f fakeEventReader) GetEventRawByID(ctx context.Context, id string) (json.RawMessage, error) {
	if f.getEventRawByIDFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getEventRawByIDFn(ctx, id)
}

func (f fakeEventReader) GetEventRawsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	if f.getEventRawsByIDs == nil {
		return nil, errors.New("not implemented")
	}
	return f.getEventRawsByIDs(ctx, ids)
}

func (f fakeEventReader) GetEventWithProvenance(context.Context, string) (store.EventWithProvenance, error) {
	return store.EventWithProvenance{}, errors.New("not implemented")
}

func (f fakeEventReader) GetEventSeenOn(context.Context, string) ([]model.EventRelay, error) {
	return nil, errors.New("not implemented")
}

func (f fakeEventReader) GetProfileByPubkey(ctx context.Context, pubkey string) (store.ProfileProjection, error) {
	if f.getProfileByPubkey == nil {
		return store.ProfileProjection{}, errors.New("not implemented")
	}
	return f.getProfileByPubkey(ctx, pubkey)
}

func (f fakeEventReader) GetProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
	if f.getProfilesByBatch == nil {
		return nil, errors.New("not implemented")
	}
	return f.getProfilesByBatch(ctx, pubkeys)
}

func (f fakeEventReader) GetAuthorRecentEvents(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if f.getAuthorEventsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getAuthorEventsFn(ctx, pubkey, limit)
}

func (f fakeEventReader) GetAuthorReplies(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if f.getAuthorRepliesFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getAuthorRepliesFn(ctx, pubkey, limit)
}

func (f fakeEventReader) GetEventCounts(ctx context.Context, eventID string) (store.EventCounts, error) {
	if f.getEventCountsFn == nil {
		return store.EventCounts{}, errors.New("not implemented")
	}
	return f.getEventCountsFn(ctx, eventID)
}

func (f fakeEventReader) GetEventReplies(
	ctx context.Context,
	eventID string,
	limit int,
	cursor *store.EventOrderCursor,
) ([]json.RawMessage, *store.EventOrderCursor, error) {
	if f.getEventRepliesFn == nil {
		return nil, nil, errors.New("not implemented")
	}
	return f.getEventRepliesFn(ctx, eventID, limit, cursor)
}

func (f fakeEventReader) GetEventAncestors(ctx context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error) {
	if f.getEventAncestors == nil {
		return nil, nil, errors.New("not implemented")
	}
	return f.getEventAncestors(ctx, eventID, maxDepth)
}

func (f fakeEventReader) ListRelayHealth(context.Context) ([]model.IngestCheckpoint, error) {
	return nil, errors.New("not implemented")
}

func (f fakeEventReader) GetContactListByPubkey(ctx context.Context, pubkey string) (store.ContactListProjection, error) {
	if f.getContactListFn == nil {
		return store.ContactListProjection{}, errors.New("not implemented")
	}
	return f.getContactListFn(ctx, pubkey)
}

func (f fakeEventReader) GetRelayListByPubkey(ctx context.Context, pubkey string) (store.RelayListProjection, error) {
	if f.getRelayListFn == nil {
		return store.RelayListProjection{}, errors.New("not implemented")
	}
	return f.getRelayListFn(ctx, pubkey)
}

func (f fakeEventReader) SearchEventsByContent(ctx context.Context, query string, limit int) ([]json.RawMessage, error) {
	if f.searchEventsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.searchEventsFn(ctx, query, limit)
}

func (f fakeEventReader) SearchProfiles(ctx context.Context, query string, limit int) ([]store.ProfileProjection, error) {
	if f.searchProfilesFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.searchProfilesFn(ctx, query, limit)
}

func (f fakeEventReader) GetRecentEventsByKindAndPubkey(ctx context.Context, kind int, pubkey string, limit int) ([]json.RawMessage, error) {
	if f.getByKindPubkeyFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getByKindPubkeyFn(ctx, kind, pubkey, limit)
}

func (f fakeEventReader) GetEventsReferencingPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error) {
	if f.getRefsPubkeyFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getRefsPubkeyFn(ctx, targetPubkey, limit)
}

func (f fakeEventReader) GetFollowersByPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error) {
	if f.getFollowersFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getFollowersFn(ctx, targetPubkey, limit)
}

func (f fakeEventReader) GetUserZaps(ctx context.Context, pubkey string, limit int, sortBySats bool) ([]json.RawMessage, error) {
	if f.getUserZapsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getUserZapsFn(ctx, pubkey, limit, sortBySats)
}

func (f fakeEventReader) GetEventZapsBySats(ctx context.Context, eventID string, limit int) ([]json.RawMessage, error) {
	if f.getEventZapsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getEventZapsFn(ctx, eventID, limit)
}

func (f fakeEventReader) IsUserFollowing(ctx context.Context, followerPubkey string, followedPubkey string) (bool, error) {
	if f.isFollowingFn == nil {
		return false, errors.New("not implemented")
	}
	return f.isFollowingFn(ctx, followerPubkey, followedPubkey)
}

func (f fakeEventReader) GetMutualFollows(ctx context.Context, leftPubkey string, rightPubkey string, limit int) ([]string, error) {
	if f.getMutualFollowsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getMutualFollowsFn(ctx, leftPubkey, rightPubkey, limit)
}

func (f fakeEventReader) GetDirectMessageContacts(ctx context.Context, pubkey string, limit int) ([]string, error) {
	if f.getDMContactsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getDMContactsFn(ctx, pubkey, limit)
}

func (f fakeEventReader) GetDirectMessages(ctx context.Context, pubkey string, peer string, limit int) ([]json.RawMessage, error) {
	if f.getDirectMsgsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getDirectMsgsFn(ctx, pubkey, peer, limit)
}

func (f fakeEventReader) GetDirectMessagesWithRange(ctx context.Context, pubkey string, peer string, since int64, until int64, limit int, offset int) ([]json.RawMessage, error) {
	if f.getDirectMsgsRangeFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getDirectMsgsRangeFn(ctx, pubkey, peer, since, until, limit, offset)
}

func (f fakeEventReader) GetDirectMessageUnreadCounts(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if f.getDMUnreadFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getDMUnreadFn(ctx, pubkey, limit)
}

func (f fakeEventReader) GetDirectMessageContactsDetailed(ctx context.Context, receiver string, limit int, offset int, since int64, until int64) ([]json.RawMessage, error) {
	if f.getDMContactsDetailedFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getDMContactsDetailedFn(ctx, receiver, limit, offset, since, until)
}

func (f fakeEventReader) GetDirectMessageCount(ctx context.Context, receiver string, sender string) (int64, error) {
	if f.getDMCountFn == nil {
		return 0, errors.New("not implemented")
	}
	return f.getDMCountFn(ctx, receiver, sender)
}

func (f fakeEventReader) ResetDirectMessageUnread(ctx context.Context, pubkey string, peer string) error {
	if f.resetDMUnreadFn == nil {
		return errors.New("not implemented")
	}
	return f.resetDMUnreadFn(ctx, pubkey, peer)
}

func (f fakeEventReader) ResetDirectMessageCount(ctx context.Context, receiver string, sender string) error {
	if f.resetDMCountFn == nil {
		return errors.New("not implemented")
	}
	return f.resetDMCountFn(ctx, receiver, sender)
}

func (f fakeEventReader) ResetDirectMessageCounts(ctx context.Context, receiver string) error {
	if f.resetDMCountsFn == nil {
		return errors.New("not implemented")
	}
	return f.resetDMCountsFn(ctx, receiver)
}

func (f fakeEventReader) GetModerationList(ctx context.Context, pubkey string, kind int) ([]string, error) {
	if f.getModerationFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getModerationFn(ctx, pubkey, kind)
}

func (f fakeEventReader) GetModerationListByIdentifier(ctx context.Context, pubkey string, identifier string) ([]string, error) {
	if f.getModerationByIdentifierFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getModerationByIdentifierFn(ctx, pubkey, identifier)
}

func (f fakeEventReader) IsHiddenByContentModeration(ctx context.Context, viewerPubkey string, eventID string) (bool, string, error) {
	if f.isHiddenFn == nil {
		return false, "", errors.New("not implemented")
	}
	return f.isHiddenFn(ctx, viewerPubkey, eventID)
}

func (f fakeEventReader) GetParameterizedReplaceableList(ctx context.Context, pubkey string, kind int, limit int) ([]json.RawMessage, error) {
	if f.getParamListFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getParamListFn(ctx, pubkey, kind, limit)
}

func (f fakeEventReader) GetParameterizedReplaceableListByIdentifier(ctx context.Context, pubkey string, kind int, identifier string, limit int) ([]json.RawMessage, error) {
	if f.getParamListByIdentifierFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getParamListByIdentifierFn(ctx, pubkey, kind, identifier, limit)
}

func (f fakeEventReader) GetParameterizedReplaceableEvent(ctx context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error) {
	if f.getParamEventFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getParamEventFn(ctx, pubkey, kind, dTag)
}

func (f fakeEventReader) GetParameterizedReplaceableEvents(ctx context.Context, kind int, dTag string, limit int) ([]json.RawMessage, error) {
	if f.getParamEventsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getParamEventsFn(ctx, kind, dTag, limit)
}

func (f fakeEventReader) GetHighlightsByEventID(ctx context.Context, eventID string, limit int) ([]json.RawMessage, error) {
	if f.getHighlightsByEventFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getHighlightsByEventFn(ctx, eventID, limit)
}

func (f fakeEventReader) GetHighlightsByATarget(ctx context.Context, kind int, pubkey string, identifier string, limit int) ([]json.RawMessage, error) {
	if f.getHighlightsByATargetFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getHighlightsByATargetFn(ctx, kind, pubkey, identifier, limit)
}

func (f fakeEventReader) GetEventsByATagAndKind(ctx context.Context, kind int, aTagValue string, limit int) ([]json.RawMessage, error) {
	if f.getEventsByATagAndKindFn == nil {
		return []json.RawMessage{}, nil
	}
	return f.getEventsByATagAndKindFn(ctx, kind, aTagValue, limit)
}

func (f fakeEventReader) GetNetworkStats(ctx context.Context) (store.NetworkStats, error) {
	if f.getNetworkStatsFn == nil {
		return store.NetworkStats{}, errors.New("not implemented")
	}
	return f.getNetworkStatsFn(ctx)
}

func (f fakeEventReader) GetCuratedValues(ctx context.Context, tableName string, valueColumn string, limit int) ([]string, error) {
	if f.getCuratedValuesFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getCuratedValuesFn(ctx, tableName, valueColumn, limit)
}

func (f fakeEventReader) GetCreatorPaidTiers(ctx context.Context, pubkey string) ([]json.RawMessage, error) {
	if f.getCreatorPaidTiersFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getCreatorPaidTiersFn(ctx, pubkey)
}

func (f fakeEventReader) GetCuratedRecommendedReads(ctx context.Context, limit int) ([]store.CuratedRecommendedRead, error) {
	if f.getCuratedRecommendedReadsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getCuratedRecommendedReadsFn(ctx, limit)
}

func (f fakeEventReader) GetCuratedReadsTopics(ctx context.Context, limit int) ([]store.CuratedReadsTopic, error) {
	if f.getCuratedReadsTopicsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getCuratedReadsTopicsFn(ctx, limit)
}

func (f fakeEventReader) GetCuratedFeaturedAuthors(ctx context.Context, limit int) ([]store.CuratedFeaturedAuthor, error) {
	if f.getCuratedFeaturedAuthorsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getCuratedFeaturedAuthorsFn(ctx, limit)
}

func (f fakeEventReader) GetPubkeyByLNAddress(ctx context.Context, lnAddress string) (string, error) {
	if f.getPubkeyByLNAddressFn == nil {
		return "", errors.New("not implemented")
	}
	return f.getPubkeyByLNAddressFn(ctx, lnAddress)
}

func readJSONFile[T any](t *testing.T, path string) T {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var out T
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return out
}

func mustReadJSON(t *testing.T, relativePath string) json.RawMessage {
	t.Helper()
	path := filepath.Join("testdata", relativePath)
	return mustReadJSONPath(t, path)
}

func mustReadJSONPath(t *testing.T, path string) json.RawMessage {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return json.RawMessage(raw)
}

func writeFormattedJSON(t *testing.T, path string, value []byte) {
	t.Helper()
	var decoded any
	if err := json.Unmarshal(value, &decoded); err != nil {
		t.Fatalf("unmarshal generated json for %s: %v", path, err)
	}
	pretty, err := json.MarshalIndent(decoded, "", "  ")
	if err != nil {
		t.Fatalf("marshal pretty json for %s: %v", path, err)
	}
	pretty = append(pretty, '\n')
	if err := os.WriteFile(path, pretty, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func assertJSONEqual(t *testing.T, got []byte, want []byte) {
	t.Helper()
	var gotValue any
	var wantValue any
	if err := json.Unmarshal(got, &gotValue); err != nil {
		t.Fatalf("unmarshal got json: %v\nraw=%s", err, string(got))
	}
	if err := json.Unmarshal(want, &wantValue); err != nil {
		t.Fatalf("unmarshal want json: %v\nraw=%s", err, string(want))
	}
	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Fatalf("json mismatch\n%s", formatJSONDiff(gotValue, wantValue))
	}
}

func formatJSONDiff(got any, want any) string {
	gotPretty, _ := json.MarshalIndent(got, "", "  ")
	wantPretty, _ := json.MarshalIndent(want, "", "  ")
	return fmt.Sprintf("got=%s\nwant=%s", gotPretty, wantPretty)
}
