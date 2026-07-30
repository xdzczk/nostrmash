package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/query"
	"github.com/xdzczk/nostrmash/internal/store"
	storeread "github.com/xdzczk/nostrmash/internal/store/read"
	storetrust "github.com/xdzczk/nostrmash/internal/store/trust"
)

func TestDiscoveryTrendingRoutes_ReturnSuccess(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{
		getTrendingNotesFn: func(_ context.Context, window time.Duration, limit int, offset int) ([]storeread.TrendingNote, error) {
			if window != 24*time.Hour {
				t.Fatalf("unexpected notes window: %s", window)
			}
			if limit != 2 {
				t.Fatalf("unexpected notes limit: %d", limit)
			}
			if offset != 0 {
				t.Fatalf("unexpected notes offset: %d", offset)
			}
			return []storeread.TrendingNote{
				{EventID: "note_1", AuthorPubkey: "pk_1", CreatedAt: 1700000000, Content: "hello", ReplyCount: 4, RepostCount: 2, ReactionCount: 3, ZapCount: 1, ZapMSats: 20000, Score: 12.5},
				{EventID: "note_2", AuthorPubkey: "pk_2", CreatedAt: 1700000010, Content: "world", ReplyCount: 1, RepostCount: 0, ReactionCount: 2, ZapCount: 0, ZapMSats: 0, Score: 4.25},
			}, nil
		},
		getHotConversationsFn: func(_ context.Context, window time.Duration, limit int, offset int) ([]store.HotConversation, error) {
			if window != 24*time.Hour {
				t.Fatalf("unexpected conversations window: %s", window)
			}
			if limit != 2 {
				t.Fatalf("unexpected conversations limit: %d", limit)
			}
			if offset != 1 {
				t.Fatalf("unexpected conversations offset: %d", offset)
			}
			return []store.HotConversation{
				{RootEventID: "root_1", AuthorPubkey: "pk_1", CreatedAt: 1700000000, Content: "hot one", ReplyCount: 4, ParticipantCount: 3, LastActivityAt: 1700000100, Replies24h: 4, Replies7d: 5, VelocityScore: 4.3, Consistency: "eventual"},
			}, nil
		},
		getTrendingTagsFn: func(_ context.Context, window time.Duration, limit int, offset int) ([]storeread.TrendingHashtag, error) {
			if window != 7*24*time.Hour {
				t.Fatalf("unexpected hashtag window: %s", window)
			}
			if limit != 3 {
				t.Fatalf("unexpected hashtag limit: %d", limit)
			}
			if offset != 1 {
				t.Fatalf("unexpected hashtag offset: %d", offset)
			}
			return []storeread.TrendingHashtag{{Hashtag: "nostr", EventCount: 11, UniqueAuthors: 6}, {Hashtag: "bitcoin", EventCount: 8, UniqueAuthors: 5}}, nil
		},
		getTrendingProfilesFn: func(_ context.Context, window time.Duration, limit int, offset int) ([]storeread.TrendingProfile, error) {
			if window != 24*time.Hour {
				t.Fatalf("unexpected trending profiles window: %s", window)
			}
			if limit != 4 {
				t.Fatalf("unexpected trending profiles limit: %d", limit)
			}
			if offset != 2 {
				t.Fatalf("unexpected trending profiles offset: %d", offset)
			}
			return []storeread.TrendingProfile{{Pubkey: "pk_a", Score: 9.5}, {Pubkey: "pk_b", Score: 6.25}}, nil
		},
		getRisingProfilesFn: func(_ context.Context, window time.Duration, limit int, offset int) ([]storeread.TrendingProfile, error) {
			if window != 7*24*time.Hour {
				t.Fatalf("unexpected rising profiles window: %s", window)
			}
			if limit != 2 {
				t.Fatalf("unexpected rising profiles limit: %d", limit)
			}
			if offset != 1 {
				t.Fatalf("unexpected rising profiles offset: %d", offset)
			}
			return []storeread.TrendingProfile{{Pubkey: "pk_c", Score: 5.75}}, nil
		},
		getRelatedProfilesFn: func(_ context.Context, pubkey string, limit int) ([]storeread.RelatedProfile, error) {
			if pubkey != "pk_a" {
				t.Fatalf("unexpected related profiles pubkey: %s", pubkey)
			}
			if limit != 2 {
				t.Fatalf("unexpected related profiles limit: %d", limit)
			}
			return []storeread.RelatedProfile{
				{
					Pubkey:               "pk_related_1",
					TopicOverlap:         3,
					ReplyAdjacency:       2,
					InteractionAdjacency: 1,
					QuoteRepostAdjacency: 1,
					Reasons:              []string{"topic_overlap", "reply_adjacency"},
					Score:                92,
				},
			}, nil
		},
	}, 200)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/notes/trending", h.GetTrendingNotes)
	mux.HandleFunc("GET /api/v1/discovery/conversations/hot", h.GetHotConversations)
	mux.HandleFunc("GET /api/v1/discovery/hashtags/trending", h.GetTrendingHashtags)
	mux.HandleFunc("GET /api/v1/discovery/profiles/trending", h.GetTrendingProfiles)
	mux.HandleFunc("GET /api/v1/discovery/profiles/rising", h.GetRisingProfiles)
	mux.HandleFunc("GET /api/v1/discovery/profiles/{pubkey}/related", h.GetRelatedProfiles)

	paths := []string{
		"/api/v1/discovery/notes/trending?window=24h&limit=2",
		"/api/v1/discovery/conversations/hot?window=24h&limit=2&offset=1",
		"/api/v1/discovery/hashtags/trending?window=7d&limit=3&offset=1",
		"/api/v1/discovery/profiles/trending?window=24h&limit=4&offset=2",
		"/api/v1/discovery/profiles/rising?window=7d&limit=2&offset=1",
		"/api/v1/discovery/profiles/pk_a/related?limit=2",
	}
	for _, path := range paths {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("unexpected status for %s: got %d want %d", path, rec.Code, http.StatusOK)
		}
	}
}

func TestDiscoveryProfileRoutes_InlineIdentityHydration(t *testing.T) {
	const (
		pubkeyA = "0f92c4a4aab613ff051f2a6e9cde7d0d131faa576a11ffe175ab82b4715c501b"
		pubkeyB = "1111111111111111111111111111111111111111111111111111111111111111"
	)
	h := mustNewHandlers(t, fakeEventReader{
		getTrendingProfilesFn: func(_ context.Context, _ time.Duration, _ int, _ int) ([]storeread.TrendingProfile, error) {
			return []storeread.TrendingProfile{{Pubkey: pubkeyA, Score: 9.5, RecentNewFollowers: 3}}, nil
		},
		getRisingProfilesFn: func(_ context.Context, _ time.Duration, _ int, _ int) ([]storeread.TrendingProfile, error) {
			return []storeread.TrendingProfile{{Pubkey: pubkeyB, Score: 7.25, RecentNewFollowers: 12}}, nil
		},
		getRelatedProfilesFn: func(_ context.Context, pubkey string, _ int) ([]storeread.RelatedProfile, error) {
			if pubkey != pubkeyA {
				t.Fatalf("unexpected related pubkey: %s", pubkey)
			}
			return []storeread.RelatedProfile{{Pubkey: pubkeyB, Score: 42}}, nil
		},
		getTrendingNotesFn: func(_ context.Context, _ time.Duration, _ int, _ int) ([]storeread.TrendingNote, error) {
			return []storeread.TrendingNote{{EventID: "note_a", AuthorPubkey: pubkeyA, CreatedAt: 123, Content: "hello", Score: 5.5}}, nil
		},
		getTrendingTagsFn: func(_ context.Context, _ time.Duration, _ int, _ int) ([]storeread.TrendingHashtag, error) {
			return []storeread.TrendingHashtag{}, nil
		},
		getPublicNetworkStatsFn: func(_ context.Context, _ int) (storeread.PublicDiscoveryNetworkStats, error) {
			return storeread.PublicDiscoveryNetworkStats{}, nil
		},
		getProfilesByBatch: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			out := make(map[string]store.ProfileProjection, len(pubkeys))
			for _, pubkey := range pubkeys {
				switch pubkey {
				case pubkeyA:
					out[pubkey] = store.ProfileProjection{
						Pubkey:            pubkey,
						MetadataEventID:   "meta_a",
						MetadataCreatedAt: 100,
						ProfileJSON: json.RawMessage(`{
							"name":"alice",
							"display_name":"Alice",
							"picture":"https://cdn.example/alice.png",
							"about":"hello",
							"nip05":"alice@example.com",
							"lud16":"alice@getalby.com",
							"website":"https://alice.example"
						}`),
					}
				case pubkeyB:
					out[pubkey] = store.ProfileProjection{
						Pubkey:            pubkey,
						MetadataEventID:   "meta_b",
						MetadataCreatedAt: 101,
						ProfileJSON:       json.RawMessage(`{"name":"bob","display_name":"Bob","picture":"https://cdn.example/bob.png"}`),
					}
				}
			}
			return out, nil
		},
	}, 200)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/notes/trending", h.GetTrendingNotes)
	mux.HandleFunc("GET /api/v1/discovery/profiles/trending", h.GetTrendingProfiles)
	mux.HandleFunc("GET /api/v1/discovery/profiles/rising", h.GetRisingProfiles)
	mux.HandleFunc("GET /api/v1/discovery/profiles/{pubkey}/related", h.GetRelatedProfiles)
	mux.HandleFunc("GET /api/v1/discovery/home", h.GetDiscoveryHome)

	trendingReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/profiles/trending", nil)
	trendingRec := httptest.NewRecorder()
	mux.ServeHTTP(trendingRec, trendingReq)
	if trendingRec.Code != http.StatusOK {
		t.Fatalf("unexpected trending status: got %d want %d", trendingRec.Code, http.StatusOK)
	}
	var trendingBody map[string]any
	if err := json.Unmarshal(trendingRec.Body.Bytes(), &trendingBody); err != nil {
		t.Fatalf("decode trending response: %v", err)
	}
	trendingProfiles, ok := trendingBody["profiles"].([]any)
	if !ok || len(trendingProfiles) != 1 {
		t.Fatalf("unexpected trending profiles payload: %#v", trendingBody["profiles"])
	}
	firstTrending, ok := trendingProfiles[0].(map[string]any)
	if !ok {
		t.Fatalf("unexpected first trending profile payload: %#v", trendingProfiles[0])
	}
	if firstTrending["display_name"] != "Alice" || firstTrending["picture"] != "https://cdn.example/alice.png" {
		t.Fatalf("expected inline identity fields on trending payload, got %#v", firstTrending)
	}
	if firstTrending["npub"] != encodeNpub(pubkeyA) {
		t.Fatalf("expected npub on trending payload, got %#v", firstTrending)
	}
	if firstTrending["nip05"] != "alice@example.com" || firstTrending["lud16"] != "alice@getalby.com" || firstTrending["website"] != "https://alice.example" {
		t.Fatalf("expected extended identity fields on trending payload, got %#v", firstTrending)
	}
	if got := int64(firstTrending["recent_new_followers"].(float64)); got != 3 {
		t.Fatalf("expected recent_new_followers on trending payload, got %#v", firstTrending)
	}

	relatedReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/profiles/"+pubkeyA+"/related", nil)
	relatedRec := httptest.NewRecorder()
	mux.ServeHTTP(relatedRec, relatedReq)
	if relatedRec.Code != http.StatusOK {
		t.Fatalf("unexpected related status: got %d want %d", relatedRec.Code, http.StatusOK)
	}
	var relatedBody map[string]any
	if err := json.Unmarshal(relatedRec.Body.Bytes(), &relatedBody); err != nil {
		t.Fatalf("decode related response: %v", err)
	}
	relatedProfiles, ok := relatedBody["related"].([]any)
	if !ok || len(relatedProfiles) != 1 {
		t.Fatalf("unexpected related payload: %#v", relatedBody["related"])
	}
	firstRelated, ok := relatedProfiles[0].(map[string]any)
	if !ok || firstRelated["display_name"] != "Bob" {
		t.Fatalf("expected inline identity on related payload, got %#v", relatedProfiles[0])
	}
	if firstRelated["npub"] != encodeNpub(pubkeyB) {
		t.Fatalf("expected npub on related payload, got %#v", relatedProfiles[0])
	}

	notesReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/notes/trending", nil)
	notesRec := httptest.NewRecorder()
	mux.ServeHTTP(notesRec, notesReq)
	if notesRec.Code != http.StatusOK {
		t.Fatalf("unexpected notes status: got %d want %d", notesRec.Code, http.StatusOK)
	}
	var notesBody map[string]any
	if err := json.Unmarshal(notesRec.Body.Bytes(), &notesBody); err != nil {
		t.Fatalf("decode notes response: %v", err)
	}
	trendingNotes, ok := notesBody["notes"].([]any)
	if !ok || len(trendingNotes) != 1 {
		t.Fatalf("unexpected notes payload: %#v", notesBody["notes"])
	}
	firstNote, ok := trendingNotes[0].(map[string]any)
	if !ok {
		t.Fatalf("unexpected first note payload: %#v", trendingNotes[0])
	}
	if _, ok := firstNote["preview"].(map[string]any); !ok {
		t.Fatalf("expected preview payload on note item, got %#v", firstNote)
	}
	noteAuthor, ok := firstNote["author"].(map[string]any)
	if !ok {
		t.Fatalf("expected inline author payload on note, got %#v", firstNote)
	}
	if noteAuthor["display_name"] != "Alice" || noteAuthor["picture"] != "https://cdn.example/alice.png" {
		t.Fatalf("expected inline author identity on note payload, got %#v", noteAuthor)
	}
	if noteAuthor["npub"] != encodeNpub(pubkeyA) {
		t.Fatalf("expected npub on inline note author payload, got %#v", noteAuthor)
	}

	homeReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/home", nil)
	homeRec := httptest.NewRecorder()
	mux.ServeHTTP(homeRec, homeReq)
	if homeRec.Code != http.StatusOK {
		t.Fatalf("unexpected home status: got %d want %d", homeRec.Code, http.StatusOK)
	}
	var homeBody map[string]any
	if err := json.Unmarshal(homeRec.Body.Bytes(), &homeBody); err != nil {
		t.Fatalf("decode home response: %v", err)
	}
	sections, ok := homeBody["sections"].(map[string]any)
	if !ok {
		t.Fatalf("missing home sections payload: %#v", homeBody)
	}
	profilesSection, ok := sections["profiles"].(map[string]any)
	if !ok {
		t.Fatalf("missing home profiles section: %#v", sections)
	}
	homeTrending, ok := profilesSection["trending"].([]any)
	if !ok || len(homeTrending) != 1 {
		t.Fatalf("unexpected home trending profiles payload: %#v", profilesSection["trending"])
	}
	firstHomeTrending, ok := homeTrending[0].(map[string]any)
	if !ok || firstHomeTrending["display_name"] != "Alice" || firstHomeTrending["picture"] != "https://cdn.example/alice.png" {
		t.Fatalf("expected inline identity on home trending payload, got %#v", homeTrending[0])
	}
	if got := int64(firstHomeTrending["recent_new_followers"].(float64)); got != 3 {
		t.Fatalf("expected recent_new_followers on home trending payload, got %#v", firstHomeTrending)
	}
	if firstHomeTrending["npub"] != encodeNpub(pubkeyA) {
		t.Fatalf("expected npub on home trending payload, got %#v", homeTrending[0])
	}
	homeRising, ok := profilesSection["rising"].([]any)
	if !ok || len(homeRising) != 1 {
		t.Fatalf("unexpected home rising profiles payload: %#v", profilesSection["rising"])
	}
	firstHomeRising, ok := homeRising[0].(map[string]any)
	if !ok || firstHomeRising["display_name"] != "Bob" {
		t.Fatalf("expected inline identity on home rising payload, got %#v", homeRising[0])
	}
	if got := int64(firstHomeRising["recent_new_followers"].(float64)); got != 12 {
		t.Fatalf("expected recent_new_followers on home rising payload, got %#v", firstHomeRising)
	}
	if firstHomeRising["npub"] != encodeNpub(pubkeyB) {
		t.Fatalf("expected npub on home rising payload, got %#v", homeRising[0])
	}
	homeNotes, ok := sections["trending_notes"].([]any)
	if !ok || len(homeNotes) != 1 {
		t.Fatalf("unexpected home notes payload: %#v", sections["trending_notes"])
	}
	firstHomeNote, ok := homeNotes[0].(map[string]any)
	if !ok {
		t.Fatalf("unexpected home first note payload: %#v", homeNotes[0])
	}
	if _, ok := firstHomeNote["preview"].(map[string]any); !ok {
		t.Fatalf("expected preview payload on home note item, got %#v", firstHomeNote)
	}
	homeNoteAuthor, ok := firstHomeNote["author"].(map[string]any)
	if !ok || homeNoteAuthor["display_name"] != "Alice" || homeNoteAuthor["npub"] != encodeNpub(pubkeyA) {
		t.Fatalf("expected inline author identity on home note payload, got %#v", firstHomeNote)
	}
}

func TestDiscoveryHomeRoute_ComposesBoundedSections(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{
		getTrendingNotesFn: func(_ context.Context, window time.Duration, limit int, offset int) ([]storeread.TrendingNote, error) {
			if window != 24*time.Hour || limit != 3 || offset != 0 {
				t.Fatalf("unexpected notes args: window=%s limit=%d offset=%d", window, limit, offset)
			}
			return []storeread.TrendingNote{
				{EventID: "note_1", AuthorPubkey: "pk_1", CreatedAt: 1700000000, Content: "hello", Score: 2.5},
			}, nil
		},
		getTrendingTagsFn: func(_ context.Context, window time.Duration, limit int, offset int) ([]storeread.TrendingHashtag, error) {
			if window != 24*time.Hour || limit != 2 || offset != 0 {
				t.Fatalf("unexpected hashtags args: window=%s limit=%d offset=%d", window, limit, offset)
			}
			return []storeread.TrendingHashtag{
				{Hashtag: "nostr", EventCount: 5, UniqueAuthors: 4},
			}, nil
		},
		getTrendingProfilesFn: func(_ context.Context, window time.Duration, limit int, offset int) ([]storeread.TrendingProfile, error) {
			if window != 24*time.Hour || limit != 2 || offset != 0 {
				t.Fatalf("unexpected trending profiles args: window=%s limit=%d offset=%d", window, limit, offset)
			}
			return []storeread.TrendingProfile{{Pubkey: "pk_a", Score: 9.1}}, nil
		},
		getRisingProfilesFn: func(_ context.Context, window time.Duration, limit int, offset int) ([]storeread.TrendingProfile, error) {
			if window != 24*time.Hour || limit != 2 || offset != 0 {
				t.Fatalf("unexpected rising profiles args: window=%s limit=%d offset=%d", window, limit, offset)
			}
			return []storeread.TrendingProfile{{Pubkey: "pk_b", Score: 7.4}}, nil
		},
		getTrendingDomainsFn: func(_ context.Context, window time.Duration, limit int, offset int) ([]store.DomainSummaryProjection, error) {
			if window != 24*time.Hour || limit != 2 || offset != 0 {
				t.Fatalf("unexpected domains args: window=%s limit=%d offset=%d", window, limit, offset)
			}
			return []store.DomainSummaryProjection{{
				Domain: "example.com",
				Activity: store.DomainActivityStatsProjection{
					Last24h: store.DomainActivityProjection{LinkCount: 3, NoteCount: 2, UniqueAuthors: 2},
					Last7d:  store.DomainActivityProjection{LinkCount: 7, NoteCount: 5, UniqueAuthors: 4},
				},
			}}, nil
		},
		getPublicNetworkStatsFn: func(_ context.Context, hashtagLimit int) (storeread.PublicDiscoveryNetworkStats, error) {
			if hashtagLimit != 7 {
				t.Fatalf("unexpected hashtag stat limit: %d", hashtagLimit)
			}
			return storeread.PublicDiscoveryNetworkStats{
				EventsIngested:    11,
				ProjectedProfiles: 6,
				Relays:            3,
				ActiveAuthors:     storeread.WindowedCount{Last24h: 4, Last7d: 8},
				NoteVolume:        storeread.WindowedCount{Last24h: 12, Last7d: 40},
			}, nil
		},
	}, 200)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/home", h.GetDiscoveryHome)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/home?window=24h&notes_limit=3&hashtags_limit=2&profiles_limit=2&domains_limit=2&hashtag_stat_limit=7", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["surface"] != "home" {
		t.Fatalf("unexpected surface: %#v", body["surface"])
	}
	sections, ok := body["sections"].(map[string]any)
	if !ok {
		t.Fatalf("missing sections payload: %#v", body)
	}
	if _, ok := sections["trending_notes"].([]any); !ok {
		t.Fatalf("missing trending_notes section: %#v", sections)
	}
	if _, ok := sections["trending_hashtags"].([]any); !ok {
		t.Fatalf("missing trending_hashtags section: %#v", sections)
	}
	domains, ok := sections["trending_domains"].([]any)
	if !ok || len(domains) != 1 {
		t.Fatalf("missing trending_domains section: %#v", sections)
	}
	profiles, ok := sections["profiles"].(map[string]any)
	if !ok {
		t.Fatalf("missing profiles section: %#v", sections)
	}
	if _, ok := profiles["trending"].([]any); !ok {
		t.Fatalf("missing profiles.trending section: %#v", profiles)
	}
	if _, ok := profiles["rising"].([]any); !ok {
		t.Fatalf("missing profiles.rising section: %#v", profiles)
	}
	if _, ok := sections["network_summary"].(map[string]any); !ok {
		t.Fatalf("missing network_summary section: %#v", sections)
	}
	meta, ok := body["meta"].(map[string]any)
	if !ok || meta["ranking_version"] != discoveryRankingVersion {
		t.Fatalf("missing discovery metadata: %#v", body)
	}
	notes := sections["trending_notes"].([]any)
	firstNote := notes[0].(map[string]any)
	if _, ok := firstNote["ranking"].(map[string]any); !ok {
		t.Fatalf("missing note ranking metadata: %#v", firstNote)
	}
}

func TestDiscoveryHomeRoute_RendersSparseSectionWithoutDroppingBundle(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{
		getTrendingNotesFn: func(_ context.Context, _ time.Duration, _ int, _ int) ([]storeread.TrendingNote, error) {
			return []storeread.TrendingNote{
				{EventID: "note_1", AuthorPubkey: "pk_1", CreatedAt: 1700000000, Content: "hello"},
			}, nil
		},
		getTrendingTagsFn: func(_ context.Context, _ time.Duration, _ int, _ int) ([]storeread.TrendingHashtag, error) {
			return []storeread.TrendingHashtag{}, nil
		},
		getTrendingProfilesFn: func(_ context.Context, _ time.Duration, _ int, _ int) ([]storeread.TrendingProfile, error) {
			return []storeread.TrendingProfile{{Pubkey: "pk_a", Score: 9}}, nil
		},
		getRisingProfilesFn: func(_ context.Context, _ time.Duration, _ int, _ int) ([]storeread.TrendingProfile, error) {
			return []storeread.TrendingProfile{}, nil
		},
		getPublicNetworkStatsFn: func(_ context.Context, _ int) (storeread.PublicDiscoveryNetworkStats, error) {
			return storeread.PublicDiscoveryNetworkStats{
				EventsIngested:    1,
				ProjectedProfiles: 1,
				Relays:            1,
				ActiveAuthors:     storeread.WindowedCount{},
				NoteVolume:        storeread.WindowedCount{},
			}, nil
		},
	}, 200)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/home", h.GetDiscoveryHome)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/home", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	sections, ok := body["sections"].(map[string]any)
	if !ok {
		t.Fatalf("missing sections payload: %#v", body)
	}
	hashtags, ok := sections["trending_hashtags"].([]any)
	if !ok {
		t.Fatalf("missing trending_hashtags section: %#v", sections)
	}
	if len(hashtags) != 0 {
		t.Fatalf("expected sparse hashtag section to be empty, got %#v", hashtags)
	}
}

func TestDiscoveryStatsRoutes_ReturnSuccess(t *testing.T) {
	computedAt := time.Date(2026, time.July, 28, 12, 0, 0, 0, time.UTC)
	h := mustNewHandlers(t, fakeEventReader{
		getPublicNetworkStatsFn: func(_ context.Context, hashtagLimit int) (storeread.PublicDiscoveryNetworkStats, error) {
			if hashtagLimit != 10 && hashtagLimit != 1 {
				t.Fatalf("unexpected hashtag limit: got=%d want one of [10,1]", hashtagLimit)
			}
			return storeread.PublicDiscoveryNetworkStats{
				EventsIngested:    11,
				ProjectedProfiles: 7,
				ComputedAt:        &computedAt,
				Relays:            3,
				RelaySummary: storeread.RelaySummaryStats{
					Total:         3,
					Active24h:     2,
					Active7d:      3,
					EventVolume:   storeread.WindowedCount{Last24h: 7, Last7d: 18},
					UniqueAuthors: storeread.WindowedCount{Last24h: 5, Last7d: 9},
				},
				TopRelays: []storeread.RelayUsageSummary{
					{RelayURL: "wss://relay.one", EventCount: 11, UniqueAuthors: 7},
					{RelayURL: "wss://relay.two", EventCount: 6, UniqueAuthors: 4},
				},
				ActiveAuthors: storeread.WindowedCount{Last24h: 5, Last7d: 9},
				NoteVolume:    storeread.WindowedCount{Last24h: 12, Last7d: 44},
				TopHashtags: &storeread.TrendingHashtagWindows{
					Last24h: []storeread.TrendingHashtag{{Hashtag: "nostr", EventCount: 6, UniqueAuthors: 4}},
					Last7d:  []storeread.TrendingHashtag{{Hashtag: "nostr", EventCount: 10, UniqueAuthors: 8}},
				},
				TopLanguages24h: []storeread.LanguageSummary{{Language: "en", Count: 5}},
				TopLanguages7d:  []storeread.LanguageSummary{{Language: "en", Count: 9}},
			}, nil
		},
	}, 200)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/stats/network", h.GetNetworkStats)
	mux.HandleFunc("GET /api/v1/discovery/stats/content", h.GetContentStats)
	mux.HandleFunc("GET /api/v1/discovery/stats/relays", h.GetRelayStats)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/stats/network", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status for network stats: got %d want %d", rec.Code, http.StatusOK)
	}
	var networkResp struct {
		ComputedAt string `json:"computed_at"`
		Network    struct {
			Totals struct {
				EventsIngested    int64 `json:"events_ingested"`
				ProjectedProfiles int64 `json:"projected_profiles"`
			} `json:"totals"`
			Activity struct {
				ActiveAuthors storeread.WindowedCount `json:"active_authors"`
				NoteVolume    storeread.WindowedCount `json:"note_volume"`
			} `json:"activity"`
			Relays struct {
				Total       int64 `json:"total"`
				Active24h   int64 `json:"active_24h"`
				Active7d    int64 `json:"active_7d"`
				EventVolume struct {
					Last24h int64 `json:"24h"`
					Last7d  int64 `json:"7d"`
				} `json:"event_volume"`
				Top []struct {
					RelayURL   string `json:"relay_url"`
					EventCount int64  `json:"event_count"`
				} `json:"top"`
			} `json:"relays"`
			TopLanguages map[string][]storeread.LanguageSummary `json:"top_languages"`
		} `json:"network"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &networkResp); err != nil {
		t.Fatalf("decode network response: %v", err)
	}
	if networkResp.Network.Totals.EventsIngested != 11 || networkResp.Network.Totals.ProjectedProfiles != 7 {
		t.Fatalf("unexpected network totals payload: %#v", networkResp.Network.Totals)
	}
	if networkResp.ComputedAt == "" {
		t.Fatal("expected network stats computed_at")
	}
	if networkResp.Network.Activity.ActiveAuthors.Last24h != 5 || networkResp.Network.Activity.NoteVolume.Last7d != 44 {
		t.Fatalf("unexpected activity payload: %#v", networkResp.Network.Activity)
	}
	if networkResp.Network.Relays.Total != 3 {
		t.Fatalf("unexpected relay total payload: %#v", networkResp.Network.Relays)
	}
	if networkResp.Network.Relays.Active24h != 2 || networkResp.Network.Relays.EventVolume.Last7d != 18 {
		t.Fatalf("unexpected relay summary payload: %#v", networkResp.Network.Relays)
	}
	if len(networkResp.Network.Relays.Top) != 2 || networkResp.Network.Relays.Top[0].RelayURL != "wss://relay.one" {
		t.Fatalf("unexpected relay top payload: %#v", networkResp.Network.Relays.Top)
	}
	if len(networkResp.Network.TopLanguages["24h"]) == 0 || networkResp.Network.TopLanguages["24h"][0].Language != "en" {
		t.Fatalf("unexpected top_languages payload: %#v", networkResp.Network.TopLanguages)
	}

	contentReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/stats/content", nil)
	contentRec := httptest.NewRecorder()
	mux.ServeHTTP(contentRec, contentReq)
	if contentRec.Code != http.StatusOK {
		t.Fatalf("unexpected status for content stats: got %d want %d", contentRec.Code, http.StatusOK)
	}
	var contentResp struct {
		ComputedAt string `json:"computed_at"`
	}
	if err := json.Unmarshal(contentRec.Body.Bytes(), &contentResp); err != nil {
		t.Fatalf("decode content response: %v", err)
	}
	if contentResp.ComputedAt == "" {
		t.Fatal("expected content stats computed_at")
	}

	relayReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/stats/relays", nil)
	relayRec := httptest.NewRecorder()
	mux.ServeHTTP(relayRec, relayReq)
	if relayRec.Code != http.StatusOK {
		t.Fatalf("unexpected status for relays stats: got %d want %d", relayRec.Code, http.StatusOK)
	}
	var relayResp struct {
		ComputedAt string `json:"computed_at"`
		Relays     struct {
			Total    int64 `json:"total"`
			Active7d int64 `json:"active_7d"`
			Top      []struct {
				RelayURL string `json:"relay_url"`
			} `json:"top"`
		} `json:"relays"`
	}
	if err := json.Unmarshal(relayRec.Body.Bytes(), &relayResp); err != nil {
		t.Fatalf("decode relay response: %v", err)
	}
	if relayResp.Relays.Total != 3 || relayResp.Relays.Active7d != 3 || len(relayResp.Relays.Top) != 2 {
		t.Fatalf("unexpected relay stats payload: %#v", relayResp.Relays)
	}
	if relayResp.ComputedAt == "" {
		t.Fatal("expected relay stats computed_at")
	}
}

func TestDiscoveryStatsSeriesRoute_ReturnsHourlyPoints(t *testing.T) {
	computedAt := time.Date(2026, time.July, 28, 12, 5, 0, 0, time.UTC)
	firstBucket := computedAt.Add(-time.Hour)
	h := mustNewHandlers(t, fakeEventReader{
		getDiscoveryStatsSeriesFn: func(_ context.Context, metric string, window time.Duration) (storeread.DiscoveryStatsSeries, error) {
			if metric != "note_volume" {
				t.Fatalf("unexpected metric: %q", metric)
			}
			if window != 7*24*time.Hour {
				t.Fatalf("unexpected window: %s", window)
			}
			return storeread.DiscoveryStatsSeries{
				Metric:     metric,
				ComputedAt: &computedAt,
				Points: []storeread.DiscoveryStatsSeriesPoint{
					{T: firstBucket, V: 12},
					{T: computedAt, V: 18},
				},
			}, nil
		},
	}, 200)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/stats/series", h.GetDiscoveryStatsSeries)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/discovery/stats/series?metric=note_volume", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var response struct {
		Metric     string `json:"metric"`
		Window     string `json:"window"`
		ComputedAt string `json:"computed_at"`
		Points     []struct {
			T int64 `json:"t"`
			V int64 `json:"v"`
		} `json:"points"`
		Consistency string `json:"consistency"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Metric != "note_volume" || response.Window != "7d" || response.ComputedAt == "" || response.Consistency != "eventual" {
		t.Fatalf("unexpected response metadata: %#v", response)
	}
	if len(response.Points) != 2 || response.Points[0].T != firstBucket.Unix() || response.Points[1].V != 18 {
		t.Fatalf("unexpected response points: %#v", response.Points)
	}

	for _, rawURL := range []string{
		"/api/v1/discovery/stats/series",
		"/api/v1/discovery/stats/series?metric=unknown",
		"/api/v1/discovery/stats/series?metric=relay_events&window=24h",
	} {
		badRec := httptest.NewRecorder()
		mux.ServeHTTP(badRec, httptest.NewRequest(http.MethodGet, rawURL, nil))
		if badRec.Code != http.StatusBadRequest {
			t.Fatalf("%s: got %d want %d", rawURL, badRec.Code, http.StatusBadRequest)
		}
	}
}

func TestDiscoveryRoutes_BadLimitAndUnsupportedCapability(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{
		getTrendingNotesFn: func(_ context.Context, _ time.Duration, _ int, _ int) ([]storeread.TrendingNote, error) {
			return nil, errors.Join(query.ErrUnsupportedCapability, errors.New("query: curated recommended reads unsupported"))
		},
		getHotConversationsFn: func(_ context.Context, _ time.Duration, _ int, _ int) ([]store.HotConversation, error) {
			return nil, errors.Join(query.ErrUnsupportedCapability, errors.New("query: hot conversations unsupported"))
		},
		getTrendingProfilesFn: func(_ context.Context, _ time.Duration, _ int, _ int) ([]storeread.TrendingProfile, error) {
			return nil, errors.Join(query.ErrUnsupportedCapability, errors.New("query: trending profiles unsupported"))
		},
		getPublicNetworkStatsFn: func(_ context.Context, _ int) (storeread.PublicDiscoveryNetworkStats, error) {
			return storeread.PublicDiscoveryNetworkStats{}, errors.Join(query.ErrUnsupportedCapability, errors.New("query: network stats unsupported"))
		},
		getRelatedProfilesFn: func(_ context.Context, _ string, _ int) ([]storeread.RelatedProfile, error) {
			return nil, errors.Join(query.ErrUnsupportedCapability, errors.New("query: related profiles unsupported"))
		},
	}, 200)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/notes/trending", h.GetTrendingNotes)
	mux.HandleFunc("GET /api/v1/discovery/conversations/hot", h.GetHotConversations)
	mux.HandleFunc("GET /api/v1/discovery/hashtags/trending", h.GetTrendingHashtags)
	mux.HandleFunc("GET /api/v1/discovery/profiles/trending", h.GetTrendingProfiles)
	mux.HandleFunc("GET /api/v1/discovery/profiles/{pubkey}/related", h.GetRelatedProfiles)
	mux.HandleFunc("GET /api/v1/discovery/stats/network", h.GetNetworkStats)

	badReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/notes/trending?limit=1000", nil)
	badRec := httptest.NewRecorder()
	mux.ServeHTTP(badRec, badReq)
	if badRec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status for bad limit: got %d want %d", badRec.Code, http.StatusBadRequest)
	}

	badNotesWindowReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/notes/trending?window=48h", nil)
	badNotesWindowRec := httptest.NewRecorder()
	mux.ServeHTTP(badNotesWindowRec, badNotesWindowReq)
	if badNotesWindowRec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status for bad notes window: got %d want %d", badNotesWindowRec.Code, http.StatusBadRequest)
	}

	badConversationsWindowReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/conversations/hot?window=48h", nil)
	badConversationsWindowRec := httptest.NewRecorder()
	mux.ServeHTTP(badConversationsWindowRec, badConversationsWindowReq)
	if badConversationsWindowRec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status for bad conversations window: got %d want %d", badConversationsWindowRec.Code, http.StatusBadRequest)
	}

	badWindowReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/hashtags/trending?window=48h", nil)
	badWindowRec := httptest.NewRecorder()
	mux.ServeHTTP(badWindowRec, badWindowReq)
	if badWindowRec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status for bad window: got %d want %d", badWindowRec.Code, http.StatusBadRequest)
	}

	badProfileWindowReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/profiles/trending?window=48h", nil)
	badProfileWindowRec := httptest.NewRecorder()
	mux.ServeHTTP(badProfileWindowRec, badProfileWindowReq)
	if badProfileWindowRec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status for bad profile window: got %d want %d", badProfileWindowRec.Code, http.StatusBadRequest)
	}

	unsupportedNotesReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/notes/trending?window=24h&limit=2", nil)
	unsupportedNotesRec := httptest.NewRecorder()
	mux.ServeHTTP(unsupportedNotesRec, unsupportedNotesReq)
	if unsupportedNotesRec.Code != http.StatusNotImplemented {
		t.Fatalf("unexpected status for unsupported notes: got %d want %d", unsupportedNotesRec.Code, http.StatusNotImplemented)
	}

	unsupportedProfilesReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/profiles/trending?window=24h&limit=2", nil)
	unsupportedProfilesRec := httptest.NewRecorder()
	mux.ServeHTTP(unsupportedProfilesRec, unsupportedProfilesReq)
	if unsupportedProfilesRec.Code != http.StatusNotImplemented {
		t.Fatalf("unexpected status for unsupported profiles: got %d want %d", unsupportedProfilesRec.Code, http.StatusNotImplemented)
	}

	unsupportedRelatedReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/profiles/pk_1/related?limit=2", nil)
	unsupportedRelatedRec := httptest.NewRecorder()
	mux.ServeHTTP(unsupportedRelatedRec, unsupportedRelatedReq)
	if unsupportedRelatedRec.Code != http.StatusNotImplemented {
		t.Fatalf("unexpected status for unsupported related profiles: got %d want %d", unsupportedRelatedRec.Code, http.StatusNotImplemented)
	}

	unsupportedConversationsReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/conversations/hot?window=24h&limit=2", nil)
	unsupportedConversationsRec := httptest.NewRecorder()
	mux.ServeHTTP(unsupportedConversationsRec, unsupportedConversationsReq)
	if unsupportedConversationsRec.Code != http.StatusNotImplemented {
		t.Fatalf("unexpected status for unsupported conversations: got %d want %d", unsupportedConversationsRec.Code, http.StatusNotImplemented)
	}

	unsupportedStatsReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/stats/network", nil)
	unsupportedStatsRec := httptest.NewRecorder()
	mux.ServeHTTP(unsupportedStatsRec, unsupportedStatsReq)
	if unsupportedStatsRec.Code != http.StatusNotImplemented {
		t.Fatalf("unexpected status for unsupported stats: got %d want %d", unsupportedStatsRec.Code, http.StatusNotImplemented)
	}
}

func TestDiscoveryRelatedProfilesRoute_RankingBoundedAndSparse(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{
		getRelatedProfilesFn: func(_ context.Context, pubkey string, limit int) ([]storeread.RelatedProfile, error) {
			switch pubkey {
			case "target_pk":
				if limit != 2 {
					t.Fatalf("unexpected limit: got %d want %d", limit, 2)
				}
				return []storeread.RelatedProfile{
					{
						Pubkey:               "rank_1",
						TopicOverlap:         4,
						ReplyAdjacency:       2,
						InteractionAdjacency: 1,
						QuoteRepostAdjacency: 1,
						Reasons:              []string{"topic_overlap", "reply_adjacency", "quote_repost_adjacency"},
						Score:                101,
					},
					{
						Pubkey:               "rank_2",
						TopicOverlap:         2,
						ReplyAdjacency:       0,
						InteractionAdjacency: 1,
						QuoteRepostAdjacency: 0,
						Reasons:              []string{"topic_overlap", "interaction_adjacency"},
						Score:                44,
					},
				}, nil
			case "sparse_pk":
				return []storeread.RelatedProfile{}, nil
			case "missing_pk":
				return nil, store.ErrNotFound
			default:
				t.Fatalf("unexpected pubkey %q", pubkey)
				return nil, nil
			}
		},
	}, 200)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/profiles/{pubkey}/related", h.GetRelatedProfiles)

	okReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/profiles/target_pk/related?limit=2", nil)
	okRec := httptest.NewRecorder()
	mux.ServeHTTP(okRec, okReq)
	if okRec.Code != http.StatusOK {
		t.Fatalf("unexpected status for related profiles: got %d want %d", okRec.Code, http.StatusOK)
	}
	var okBody map[string]any
	if err := json.Unmarshal(okRec.Body.Bytes(), &okBody); err != nil {
		t.Fatalf("decode related response: %v", err)
	}
	if okBody["pubkey"] != "target_pk" {
		t.Fatalf("unexpected pubkey payload: %#v", okBody["pubkey"])
	}
	related, ok := okBody["related"].([]any)
	if !ok {
		t.Fatalf("missing related payload: %#v", okBody)
	}
	if len(related) != 2 {
		t.Fatalf("expected related payload length 2, got %#v", related)
	}
	first, ok := related[0].(map[string]any)
	if !ok || first["pubkey"] != "rank_1" {
		t.Fatalf("expected rank_1 first, got %#v", related[0])
	}

	sparseReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/profiles/sparse_pk/related", nil)
	sparseRec := httptest.NewRecorder()
	mux.ServeHTTP(sparseRec, sparseReq)
	if sparseRec.Code != http.StatusOK {
		t.Fatalf("unexpected status for sparse profile: got %d want %d", sparseRec.Code, http.StatusOK)
	}
	var sparseBody map[string]any
	if err := json.Unmarshal(sparseRec.Body.Bytes(), &sparseBody); err != nil {
		t.Fatalf("decode sparse response: %v", err)
	}
	sparseRelated, ok := sparseBody["related"].([]any)
	if !ok || len(sparseRelated) != 0 {
		t.Fatalf("expected empty related payload for sparse profile: %#v", sparseBody)
	}

	missingReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/profiles/missing_pk/related", nil)
	missingRec := httptest.NewRecorder()
	mux.ServeHTTP(missingRec, missingReq)
	if missingRec.Code != http.StatusNotFound {
		t.Fatalf("unexpected status for missing profile: got %d want %d", missingRec.Code, http.StatusNotFound)
	}
}

func TestDiscoveryStatsRoutes_MissingDataEdgeCases(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{
		getPublicNetworkStatsFn: func(_ context.Context, _ int) (storeread.PublicDiscoveryNetworkStats, error) {
			return storeread.PublicDiscoveryNetworkStats{
				EventsIngested:    0,
				ProjectedProfiles: 0,
				Relays:            0,
				ActiveAuthors:     storeread.WindowedCount{Last24h: 0, Last7d: 0},
				NoteVolume:        storeread.WindowedCount{Last24h: 0, Last7d: 0},
				TopHashtags:       nil,
			}, nil
		},
	}, 200)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/stats/network", h.GetNetworkStats)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/stats/network", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var decoded map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	networkValue, ok := decoded["network"].(map[string]any)
	if !ok {
		t.Fatalf("missing network payload: %#v", decoded)
	}
	if _, hasTopHashtags := networkValue["top_hashtags"]; hasTopHashtags {
		t.Fatalf("top_hashtags should be omitted when unavailable: %#v", networkValue)
	}
}

func TestDiscoveryCache_HitAndMissForTrendingNotes(t *testing.T) {
	calls := 0
	cacheEnabled := true
	h := mustNewHandlersWithOptions(t, fakeEventReader{
		getTrendingNotesFn: func(_ context.Context, _ time.Duration, _ int, _ int) ([]storeread.TrendingNote, error) {
			calls++
			return []storeread.TrendingNote{
				{EventID: "note_1", AuthorPubkey: "pk_1", CreatedAt: 1700000000, Content: "cached", Score: 1.0},
			}, nil
		},
	}, HandlersOptions{
		MaxBatchSize: 200,
		DiscoveryCache: &DiscoveryCacheOptions{
			Enabled:     &cacheEnabled,
			MaxEntries:  8,
			TrendingTTL: time.Minute,
		},
	})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/notes/trending", h.GetTrendingNotes)

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/notes/trending?window=24h&limit=1&offset=0", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("unexpected status on request %d: got %d want %d", i+1, rec.Code, http.StatusOK)
		}
	}
	if calls != 1 {
		t.Fatalf("expected one backend call for cache hit path, got %d", calls)
	}
}

func TestDiscoveryCache_SeparatesKeysByParams(t *testing.T) {
	calls := 0
	cacheEnabled := true
	h := mustNewHandlersWithOptions(t, fakeEventReader{
		getTrendingNotesFn: func(_ context.Context, _ time.Duration, _ int, _ int) ([]storeread.TrendingNote, error) {
			calls++
			return []storeread.TrendingNote{{EventID: "note_1", AuthorPubkey: "pk_1", CreatedAt: 1700000000, Content: "ok", Score: 1.0}}, nil
		},
	}, HandlersOptions{
		MaxBatchSize: 200,
		DiscoveryCache: &DiscoveryCacheOptions{
			Enabled:     &cacheEnabled,
			MaxEntries:  8,
			TrendingTTL: time.Minute,
		},
	})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/notes/trending", h.GetTrendingNotes)

	reqA := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/notes/trending?window=24h&limit=1&offset=0", nil)
	recA := httptest.NewRecorder()
	mux.ServeHTTP(recA, reqA)
	if recA.Code != http.StatusOK {
		t.Fatalf("unexpected status for first key: got %d want %d", recA.Code, http.StatusOK)
	}

	reqB := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/notes/trending?window=24h&limit=2&offset=0", nil)
	recB := httptest.NewRecorder()
	mux.ServeHTTP(recB, reqB)
	if recB.Code != http.StatusOK {
		t.Fatalf("unexpected status for second key: got %d want %d", recB.Code, http.StatusOK)
	}

	if calls != 2 {
		t.Fatalf("expected separate backend calls for different params, got %d", calls)
	}
}

func TestDiscoveryCache_TTLExpiry(t *testing.T) {
	calls := 0
	cacheEnabled := true
	h := mustNewHandlersWithOptions(t, fakeEventReader{
		getPublicNetworkStatsFn: func(_ context.Context, _ int) (storeread.PublicDiscoveryNetworkStats, error) {
			calls++
			return storeread.PublicDiscoveryNetworkStats{
				EventsIngested: 12,
				Relays:         4,
			}, nil
		},
	}, HandlersOptions{
		MaxBatchSize: 200,
		DiscoveryCache: &DiscoveryCacheOptions{
			Enabled:        &cacheEnabled,
			MaxEntries:     8,
			PublicStatsTTL: 10 * time.Millisecond,
		},
	})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/stats/network", h.GetNetworkStats)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/stats/network", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status before expiry: got %d want %d", rec.Code, http.StatusOK)
	}

	time.Sleep(20 * time.Millisecond)

	req2 := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/stats/network", nil)
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("unexpected status after expiry: got %d want %d", rec2.Code, http.StatusOK)
	}
	if calls != 2 {
		t.Fatalf("expected cache expiry to trigger second backend call, got %d", calls)
	}
}

func TestDiscoveryCache_DisabledFallsBackToQueryPath(t *testing.T) {
	calls := 0
	cacheEnabled := false
	h := mustNewHandlersWithOptions(t, fakeEventReader{
		getPublicNetworkStatsFn: func(_ context.Context, _ int) (storeread.PublicDiscoveryNetworkStats, error) {
			calls++
			return storeread.PublicDiscoveryNetworkStats{Relays: 2}, nil
		},
	}, HandlersOptions{
		MaxBatchSize: 200,
		DiscoveryCache: &DiscoveryCacheOptions{
			Enabled:        &cacheEnabled,
			MaxEntries:     8,
			PublicStatsTTL: time.Minute,
		},
	})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/stats/relays", h.GetRelayStats)

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/stats/relays", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("unexpected status on request %d: got %d want %d", i+1, rec.Code, http.StatusOK)
		}
	}

	if calls != 2 {
		t.Fatalf("expected cache-disabled path to call backend twice, got %d", calls)
	}
}

func TestDiscoveryTrendingRoutes_TrustMetadataByMode(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name                string
		mode                string
		expectedApplied     bool
		expectedResultScope string
	}{
		{name: "open", mode: "open", expectedApplied: false, expectedResultScope: "open"},
		{name: "prefer_trusted", mode: "prefer_trusted", expectedApplied: true, expectedResultScope: "prefer_trusted"},
		{name: "trusted_only", mode: "trusted_only", expectedApplied: true, expectedResultScope: "trusted_only"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			h := mustNewHandlersWithOptions(t, trustQualifiedFakeReader{
				fakeEventReader: fakeEventReader{
					getTrendingNotesFn: func(_ context.Context, _ time.Duration, _ int, _ int) ([]storeread.TrendingNote, error) {
						return []storeread.TrendingNote{
							{EventID: "note_1", AuthorPubkey: "pk_trusted", CreatedAt: 1700000000, Content: "hello"},
							{EventID: "note_2", AuthorPubkey: "pk_open", CreatedAt: 1700000001, Content: "world"},
						}, nil
					},
				},
				getTrustQualificationsFn: func(_ context.Context, pubkeys []string, _ storetrust.TrustQualificationPolicy) (map[string]storetrust.TrustQualification, error) {
					out := make(map[string]storetrust.TrustQualification, len(pubkeys))
					for _, pubkey := range pubkeys {
						out[pubkey] = storetrust.TrustQualification{Pubkey: pubkey, Trusted: pubkey == "pk_trusted"}
					}
					return out, nil
				},
				isTrustedAuthorFn: func(_ context.Context, pubkey string, _ storetrust.TrustQualificationPolicy) (bool, error) {
					return pubkey == "pk_trusted", nil
				},
			}, HandlersOptions{
				MaxBatchSize: 200,
				QueryOptions: query.ServiceOptions{
					DiscoveryCandidateTrustMode: tc.mode,
				},
			})

			req := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/notes/trending?window=24h&limit=2", nil)
			rec := httptest.NewRecorder()
			http.HandlerFunc(h.GetTrendingNotes).ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
			}

			var body struct {
				TrustMode    string `json:"trust_mode"`
				TrustApplied bool   `json:"trust_applied"`
				ResultScope  string `json:"result_scope"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if body.TrustMode != tc.mode {
				t.Fatalf("unexpected trust_mode: got %q want %q", body.TrustMode, tc.mode)
			}
			if body.TrustApplied != tc.expectedApplied {
				t.Fatalf("unexpected trust_applied: got %v want %v", body.TrustApplied, tc.expectedApplied)
			}
			if body.ResultScope != tc.expectedResultScope {
				t.Fatalf("unexpected result_scope: got %q want %q", body.ResultScope, tc.expectedResultScope)
			}
		})
	}
}

func TestHashtagPageRoutes_SummaryNotesAndRelated(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{
		getHashtagSummaryFn: func(_ context.Context, hashtag string) (storeread.HashtagSummary, error) {
			if hashtag != "nostr" {
				t.Fatalf("unexpected hashtag summary key: %s", hashtag)
			}
			latest := int64(1700000010)
			return storeread.HashtagSummary{
				Hashtag:       "nostr",
				LatestEventAt: &latest,
				Activity: storeread.HashtagActivityStats{
					Last24h: storeread.HashtagActivity{EventCount: 3, UniqueAuthors: 2},
					Last7d:  storeread.HashtagActivity{EventCount: 7, UniqueAuthors: 4},
					Last30d: storeread.HashtagActivity{EventCount: 9, UniqueAuthors: 5},
					All:     storeread.HashtagActivity{EventCount: 11, UniqueAuthors: 6},
				},
			}, nil
		},
		getHashtagNotesFn: func(_ context.Context, hashtag string, sort string, window string, limit int, offset int) ([]storeread.TrendingNote, error) {
			if hashtag != "nostr" || sort != "top" || window != "7d" || limit != 2 || offset != 1 {
				t.Fatalf("unexpected hashtag notes args: %s %s %s %d %d", hashtag, sort, window, limit, offset)
			}
			return []storeread.TrendingNote{
				{EventID: "note_1", AuthorPubkey: "pk_1", CreatedAt: 1700000000, Content: "hello", ReplyCount: 1, RepostCount: 2, ReactionCount: 3, ZapCount: 1, ZapMSats: 2000, Score: 9.2},
			}, nil
		},
		getRelatedHashtagsFn: func(_ context.Context, hashtag string, limit int) ([]storeread.RelatedHashtag, error) {
			if hashtag != "nostr" || limit != 3 {
				t.Fatalf("unexpected related hashtag args: %s %d", hashtag, limit)
			}
			return []storeread.RelatedHashtag{
				{Hashtag: "bitcoin", CoOccurrenceCount: 4, CoOccurrenceAuthors: 3},
			}, nil
		},
	}, 200)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/hashtags/{hashtag}", h.GetHashtagSummary)
	mux.HandleFunc("GET /api/v1/discovery/hashtags/{hashtag}/notes", h.GetHashtagNotes)
	mux.HandleFunc("GET /api/v1/discovery/hashtags/{hashtag}/related", h.GetRelatedHashtags)

	for _, path := range []string{
		"/api/v1/discovery/hashtags/nostr",
		"/api/v1/discovery/hashtags/nostr/notes?sort=top&window=7d&limit=2&offset=1",
		"/api/v1/discovery/hashtags/nostr/related?limit=3",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("unexpected status for %s: got %d want %d", path, rec.Code, http.StatusOK)
		}
	}
}

func TestHashtagPageRoutes_NormalizationMissingAndValidation(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{
		getHashtagSummaryFn: func(_ context.Context, hashtag string) (storeread.HashtagSummary, error) {
			if hashtag == "nostr" {
				return storeread.HashtagSummary{Hashtag: "nostr", Activity: storeread.HashtagActivityStats{All: storeread.HashtagActivity{EventCount: 1, UniqueAuthors: 1}}}, nil
			}
			return storeread.HashtagSummary{}, store.ErrNotFound
		},
		getHashtagNotesFn: func(_ context.Context, hashtag string, _, _ string, _, _ int) ([]storeread.TrendingNote, error) {
			if hashtag == "missing" {
				return nil, store.ErrNotFound
			}
			return []storeread.TrendingNote{}, nil
		},
		getRelatedHashtagsFn: func(_ context.Context, hashtag string, _ int) ([]storeread.RelatedHashtag, error) {
			if hashtag == "missing" {
				return nil, store.ErrNotFound
			}
			return []storeread.RelatedHashtag{}, nil
		},
	}, 200)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/hashtags/{hashtag}", h.GetHashtagSummary)
	mux.HandleFunc("GET /api/v1/discovery/hashtags/{hashtag}/notes", h.GetHashtagNotes)
	mux.HandleFunc("GET /api/v1/discovery/hashtags/{hashtag}/related", h.GetRelatedHashtags)

	okReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/hashtags/%23Nostr", nil)
	okRec := httptest.NewRecorder()
	mux.ServeHTTP(okRec, okReq)
	if okRec.Code != http.StatusOK {
		t.Fatalf("unexpected status for normalized hashtag: got %d want %d", okRec.Code, http.StatusOK)
	}

	missingReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/hashtags/missing", nil)
	missingRec := httptest.NewRecorder()
	mux.ServeHTTP(missingRec, missingReq)
	if missingRec.Code != http.StatusNotFound {
		t.Fatalf("unexpected status for missing hashtag summary: got %d want %d", missingRec.Code, http.StatusNotFound)
	}

	badReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/hashtags/###bad/notes?sort=wat&window=99d", nil)
	badRec := httptest.NewRecorder()
	mux.ServeHTTP(badRec, badReq)
	if badRec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status for invalid notes params: got %d want %d", badRec.Code, http.StatusBadRequest)
	}
}

func TestDomainPageRoutes_TrendingSummaryAndNotes(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{
		getTrendingDomainsFn: func(_ context.Context, window time.Duration, limit int, offset int) ([]store.DomainSummaryProjection, error) {
			if window != 7*24*time.Hour || limit != 2 || offset != 1 {
				t.Fatalf("unexpected trending domains args: window=%s limit=%d offset=%d", window, limit, offset)
			}
			return []store.DomainSummaryProjection{
				{
					Domain: "example.com",
					Activity: store.DomainActivityStatsProjection{
						Last24h: store.DomainActivityProjection{LinkCount: 3, NoteCount: 2, UniqueAuthors: 2},
						Last7d:  store.DomainActivityProjection{LinkCount: 7, NoteCount: 5, UniqueAuthors: 4},
					},
				},
			}, nil
		},
		getDomainSummaryFn: func(_ context.Context, domain string, recentLimit int, topLimit int) (store.DomainSummaryProjection, error) {
			if domain != "example.com" || recentLimit != 5 || topLimit != 5 {
				t.Fatalf("unexpected domain summary args: %s %d %d", domain, recentLimit, topLimit)
			}
			latest := int64(1700000011)
			return store.DomainSummaryProjection{
				Domain:        "example.com",
				LatestEventAt: &latest,
				Activity: store.DomainActivityStatsProjection{
					Last24h: store.DomainActivityProjection{LinkCount: 2, NoteCount: 2, UniqueAuthors: 2},
					Last7d:  store.DomainActivityProjection{LinkCount: 8, NoteCount: 6, UniqueAuthors: 5},
					Last30d: store.DomainActivityProjection{LinkCount: 11, NoteCount: 8, UniqueAuthors: 6},
					All:     store.DomainActivityProjection{LinkCount: 13, NoteCount: 10, UniqueAuthors: 7},
				},
				RecentNotes: []storeread.TrendingNote{
					{EventID: "note_recent", AuthorPubkey: "pk_1", CreatedAt: 1700000000, Content: "recent"},
				},
				TopNotes: []storeread.TrendingNote{
					{EventID: "note_top", AuthorPubkey: "pk_2", CreatedAt: 1699999999, Content: "top", Score: 12.2},
				},
			}, nil
		},
		getDomainNotesFn: func(_ context.Context, domain string, sort string, window string, limit int, offset int) ([]storeread.TrendingNote, error) {
			if domain != "example.com" || sort != "top" || window != "30d" || limit != 2 || offset != 1 {
				t.Fatalf("unexpected domain notes args: %s %s %s %d %d", domain, sort, window, limit, offset)
			}
			return []storeread.TrendingNote{
				{EventID: "note_1", AuthorPubkey: "pk_1", CreatedAt: 1700000000, Content: "hello", Score: 9.2},
			}, nil
		},
	}, 200)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/domains/trending", h.GetTrendingDomains)
	mux.HandleFunc("GET /api/v1/discovery/domains/{domain}", h.GetDomainSummary)
	mux.HandleFunc("GET /api/v1/discovery/domains/{domain}/notes", h.GetDomainNotes)

	for _, path := range []string{
		"/api/v1/discovery/domains/trending?window=7d&limit=2&offset=1",
		"/api/v1/discovery/domains/example.com",
		"/api/v1/discovery/domains/example.com/notes?sort=top&window=30d&limit=2&offset=1",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("unexpected status for %s: got %d want %d", path, rec.Code, http.StatusOK)
		}
	}
}

func TestDomainPageRoutes_NormalizationMissingAndValidation(t *testing.T) {
	h := mustNewHandlers(t, fakeEventReader{
		getDomainSummaryFn: func(_ context.Context, domain string, _, _ int) (store.DomainSummaryProjection, error) {
			if domain == "example.com" || domain == "youtube.com" {
				return store.DomainSummaryProjection{
					Domain:   domain,
					Activity: store.DomainActivityStatsProjection{All: store.DomainActivityProjection{LinkCount: 1, NoteCount: 1, UniqueAuthors: 1}},
				}, nil
			}
			return store.DomainSummaryProjection{}, store.ErrNotFound
		},
		getDomainNotesFn: func(_ context.Context, domain string, _, _ string, _, _ int) ([]storeread.TrendingNote, error) {
			if domain == "missing.example" {
				return nil, store.ErrNotFound
			}
			return []storeread.TrendingNote{}, nil
		},
	}, 200)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/discovery/domains/{domain}", h.GetDomainSummary)
	mux.HandleFunc("GET /api/v1/discovery/domains/{domain}/notes", h.GetDomainNotes)

	okReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/domains/HTTPS:%2F%2FExample.com", nil)
	okRec := httptest.NewRecorder()
	mux.ServeHTTP(okRec, okReq)
	if okRec.Code != http.StatusOK {
		t.Fatalf("unexpected status for normalized domain: got %d want %d", okRec.Code, http.StatusOK)
	}

	aliasReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/domains/www.youtu.be", nil)
	aliasRec := httptest.NewRecorder()
	mux.ServeHTTP(aliasRec, aliasReq)
	if aliasRec.Code != http.StatusOK {
		t.Fatalf("unexpected status for canonical domain alias: got %d want %d", aliasRec.Code, http.StatusOK)
	}
	var aliasBody struct {
		Domain string `json:"domain"`
	}
	if err := json.Unmarshal(aliasRec.Body.Bytes(), &aliasBody); err != nil {
		t.Fatalf("decode canonical domain alias response: %v", err)
	}
	if aliasBody.Domain != "youtube.com" {
		t.Fatalf("unexpected canonical domain alias response: got=%q want=youtube.com", aliasBody.Domain)
	}

	missingReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/domains/missing.example", nil)
	missingRec := httptest.NewRecorder()
	mux.ServeHTTP(missingRec, missingReq)
	if missingRec.Code != http.StatusNotFound {
		t.Fatalf("unexpected status for missing domain summary: got %d want %d", missingRec.Code, http.StatusNotFound)
	}

	badReq := httptest.NewRequest(http.MethodGet, "/api/v1/discovery/domains/###bad/notes?sort=wat&window=99d", nil)
	badRec := httptest.NewRecorder()
	mux.ServeHTTP(badRec, badReq)
	if badRec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status for invalid domain notes params: got %d want %d", badRec.Code, http.StatusBadRequest)
	}
}
