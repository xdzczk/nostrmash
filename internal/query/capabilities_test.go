package query

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/xdzczk/nostrmash/internal/store"
)

func TestServiceCapabilities_FullCapabilityReader(t *testing.T) {
	t.Parallel()
	svc := mustNewService(t, fullCapabilityReader{
		fakeReader: fakeReader{
			getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
				return nil, errors.New("unused")
			},
		},
	})
	ctx := context.Background()

	dms, err := svc.GetDirectMessagesByPeer(ctx, "pk-1", "peer-1", 5)
	if err != nil {
		t.Fatalf("GetDirectMessagesByPeer returned error: %v", err)
	}
	if len(dms) != 1 || string(dms[0]) != `{"id":"dm-capability"}` {
		t.Fatalf("expected capability-backed direct messages, got %#v", dms)
	}

	muteList, err := svc.GetMuteList(ctx, "pk-1")
	if err != nil {
		t.Fatalf("GetMuteList returned error: %v", err)
	}
	if len(muteList) != 1 || muteList[0] != "pk-muted" {
		t.Fatalf("expected capability-backed mute list, got %#v", muteList)
	}

	reads, err := svc.GetCuratedRecommendedReads(ctx, 10)
	if err != nil {
		t.Fatalf("GetCuratedRecommendedReads returned error: %v", err)
	}
	if len(reads) != 1 || reads[0].EventID != "evt-curated" {
		t.Fatalf("expected capability-backed curated reads, got %#v", reads)
	}

	score, err := svc.GetTrustScore(ctx, "pk-1")
	if err != nil {
		t.Fatalf("GetTrustScore returned error: %v", err)
	}
	if score.Pubkey != "pk-1" || score.Score != 0.91 {
		t.Fatalf("expected capability-backed trust score, got %#v", score)
	}

	bookmarks, err := svc.GetBookmarks(ctx, "pk-1", 20)
	if err != nil {
		t.Fatalf("GetBookmarks returned error: %v", err)
	}
	if len(bookmarks) != 1 || string(bookmarks[0]) != `{"id":"bookmark-capability"}` {
		t.Fatalf("expected capability-backed bookmarks, got %#v", bookmarks)
	}

	following, err := svc.IsUserFollowing(ctx, "pk-1", "pk-2")
	if err != nil {
		t.Fatalf("IsUserFollowing returned error: %v", err)
	}
	if !following {
		t.Fatalf("expected capability-backed following=true")
	}

	zaps, err := svc.GetZaps(ctx, "pk-1", 5)
	if err != nil {
		t.Fatalf("GetZaps returned error: %v", err)
	}
	if len(zaps) != 1 || string(zaps[0]) != `{"id":"zap-capability"}` {
		t.Fatalf("expected capability-backed zaps, got %#v", zaps)
	}
}

func TestServiceCapabilities_PartialCapabilityReader(t *testing.T) {
	t.Parallel()
	svc := mustNewService(t, partialCapabilityReader{
		fakeReader: fakeReader{
			getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
				return nil, errors.New("unused")
			},
		},
		getCuratedRecommendedReadsFn: func(context.Context, int) ([]CuratedRecommendedRead, error) {
			return []CuratedRecommendedRead{{EventID: "evt-partial"}}, nil
		},
		getRecentEventsByKindAndPubkeyFn: func(_ context.Context, kind int, _ string, _ int) ([]json.RawMessage, error) {
			return []json.RawMessage{json.RawMessage(`{"kind":9735}`)}, nil
		},
	})
	ctx := context.Background()

	reads, err := svc.GetCuratedRecommendedReads(ctx, 5)
	if err != nil {
		t.Fatalf("GetCuratedRecommendedReads returned error: %v", err)
	}
	if len(reads) != 1 || reads[0].EventID != "evt-partial" {
		t.Fatalf("expected partial capability curated reads, got %#v", reads)
	}

	zaps, err := svc.GetZaps(ctx, "pk-1", 5)
	if err != nil {
		t.Fatalf("GetZaps returned error: %v", err)
	}
	if len(zaps) != 1 || string(zaps[0]) != `{"kind":9735}` {
		t.Fatalf("expected zaps fallback via base reader, got %#v", zaps)
	}

	if _, err := svc.GetTrustScore(ctx, "pk-1"); err == nil {
		t.Fatalf("expected missing trust capability error")
	}
}

func TestServiceCapabilities_MissingCapabilityReader(t *testing.T) {
	t.Parallel()
	svc := mustNewService(t, fakeReader{
		getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
			return nil, errors.New("unused")
		},
	})
	ctx := context.Background()

	contacts, err := svc.GetDirectMessageContacts(ctx, "pk-1", 10)
	if err == nil {
		t.Fatalf("expected missing direct message contacts capability error")
	}
	if !IsUnsupportedCapability(err) {
		t.Fatalf("expected unsupported capability error, got: %v", err)
	}
	if contacts != nil {
		t.Fatalf("expected nil contacts on unsupported capability, got %#v", contacts)
	}

	highlights, err := svc.GetHighlightsByEventID(ctx, "evt-1", 10)
	if err == nil {
		t.Fatalf("expected missing highlights capability error")
	}
	if !IsUnsupportedCapability(err) {
		t.Fatalf("expected unsupported capability error, got: %v", err)
	}
	if highlights != nil {
		t.Fatalf("expected nil highlights on unsupported capability, got %#v", highlights)
	}

	if _, err := svc.GetTrustScore(ctx, "pk-1"); err == nil {
		t.Fatalf("expected missing trust capability error")
	}
}

func TestNewServiceWithOptions_ReturnsErrorWithoutRequiredReaderCapabilities(t *testing.T) {
	t.Parallel()
	_, err := NewServiceWithOptions(struct{}{}, ServiceOptions{})
	if err == nil {
		t.Fatalf("expected constructor error when required reader capability is missing")
	}
	if got, want := err.Error(), "query: unsupported reader type struct {}"; got != want {
		t.Fatalf("unexpected constructor error: got %q want %q", got, want)
	}
}

func TestNewServiceWithOptions_SucceedsWithMixedCapabilities(t *testing.T) {
	t.Parallel()
	svc, err := NewServiceWithOptions(partialCapabilityReader{
		fakeReader: fakeReader{
			getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
				return nil, errors.New("unused")
			},
		},
		getCuratedRecommendedReadsFn: func(context.Context, int) ([]CuratedRecommendedRead, error) {
			return []CuratedRecommendedRead{{EventID: "evt-partial"}}, nil
		},
		getRecentEventsByKindAndPubkeyFn: func(_ context.Context, kind int, _ string, _ int) ([]json.RawMessage, error) {
			return []json.RawMessage{json.RawMessage(`{"kind":9735}`)}, nil
		},
	}, ServiceOptions{})
	if err != nil {
		t.Fatalf("NewServiceWithOptions returned error: %v", err)
	}

	ctx := context.Background()
	reads, err := svc.GetCuratedRecommendedReads(ctx, 5)
	if err != nil {
		t.Fatalf("GetCuratedRecommendedReads returned error: %v", err)
	}
	if len(reads) != 1 || reads[0].EventID != "evt-partial" {
		t.Fatalf("expected partial capability curated reads, got %#v", reads)
	}
	zaps, err := svc.GetZaps(ctx, "pk-1", 5)
	if err != nil {
		t.Fatalf("GetZaps returned error: %v", err)
	}
	if len(zaps) != 1 || string(zaps[0]) != `{"kind":9735}` {
		t.Fatalf("expected zaps fallback via base reader, got %#v", zaps)
	}
}

func TestGetMuteList_DistinguishesSupportedEmptyUnsupportedAndBackendFailure(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	supportedEmpty := mustNewService(t, moderationCapabilityReader{
		fakeReader: fakeReader{
			getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
				return nil, errors.New("unused")
			},
		},
		getModerationListFn: func(context.Context, string, int) ([]string, error) {
			return nil, store.ErrNotFound
		},
	})
	values, err := supportedEmpty.GetMuteList(ctx, "pk-1")
	if err != nil {
		t.Fatalf("expected supported empty mute list, got err=%v", err)
	}
	if len(values) != 0 {
		t.Fatalf("expected supported empty mute list, got %#v", values)
	}

	unsupported := mustNewService(t, fakeReader{
		getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
			return nil, errors.New("unused")
		},
	})
	_, err = unsupported.GetMuteList(ctx, "pk-1")
	if err == nil || !IsUnsupportedCapability(err) {
		t.Fatalf("expected unsupported capability error, got %v", err)
	}

	backendFailure := mustNewService(t, moderationCapabilityReader{
		fakeReader: fakeReader{
			getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
				return nil, errors.New("unused")
			},
		},
		getModerationListFn: func(context.Context, string, int) ([]string, error) {
			return nil, errors.New("backend failure")
		},
	})
	_, err = backendFailure.GetMuteList(ctx, "pk-1")
	if err == nil || IsUnsupportedCapability(err) {
		t.Fatalf("expected backend failure error, got %v", err)
	}
}

type fullCapabilityReader struct {
	fakeReader
}

func (r fullCapabilityReader) GetDirectMessages(context.Context, string, string, int) ([]json.RawMessage, error) {
	return []json.RawMessage{json.RawMessage(`{"id":"dm-capability"}`)}, nil
}

func (r fullCapabilityReader) GetModerationList(context.Context, string, int) ([]string, error) {
	return []string{"pk-muted"}, nil
}

func (r fullCapabilityReader) GetCuratedRecommendedReads(context.Context, int) ([]CuratedRecommendedRead, error) {
	return []CuratedRecommendedRead{{EventID: "evt-curated"}}, nil
}

func (r fullCapabilityReader) GetTrustScore(context.Context, string) (TrustScore, error) {
	return TrustScore{Pubkey: "pk-1", Score: 0.91}, nil
}

func (r fullCapabilityReader) GetParameterizedReplaceableEvent(context.Context, string, int, string) (json.RawMessage, error) {
	return json.RawMessage(`{"id":"bookmark-capability"}`), nil
}

func (r fullCapabilityReader) IsUserFollowing(context.Context, string, string) (bool, error) {
	return true, nil
}

func (r fullCapabilityReader) GetUserZaps(context.Context, string, int, bool) ([]json.RawMessage, error) {
	return []json.RawMessage{json.RawMessage(`{"id":"zap-capability"}`)}, nil
}

type partialCapabilityReader struct {
	fakeReader
	getCuratedRecommendedReadsFn     func(context.Context, int) ([]CuratedRecommendedRead, error)
	getRecentEventsByKindAndPubkeyFn func(context.Context, int, string, int) ([]json.RawMessage, error)
}

type moderationCapabilityReader struct {
	fakeReader
	getModerationListFn func(context.Context, string, int) ([]string, error)
}

func (r moderationCapabilityReader) GetModerationList(ctx context.Context, pubkey string, kind int) ([]string, error) {
	if r.getModerationListFn == nil {
		return nil, store.ErrNotFound
	}
	return r.getModerationListFn(ctx, pubkey, kind)
}

func (r partialCapabilityReader) GetCuratedRecommendedReads(ctx context.Context, limit int) ([]CuratedRecommendedRead, error) {
	if r.getCuratedRecommendedReadsFn == nil {
		return []CuratedRecommendedRead{}, nil
	}
	return r.getCuratedRecommendedReadsFn(ctx, limit)
}

func (r partialCapabilityReader) GetRecentEventsByKindAndPubkey(ctx context.Context, kind int, pubkey string, limit int) ([]json.RawMessage, error) {
	if r.getRecentEventsByKindAndPubkeyFn == nil {
		return r.fakeReader.GetRecentEventsByKindAndPubkey(ctx, kind, pubkey, limit)
	}
	return r.getRecentEventsByKindAndPubkeyFn(ctx, kind, pubkey, limit)
}
