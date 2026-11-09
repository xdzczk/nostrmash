package api

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/xdzczk/nostrmash/internal/nostr"
	"github.com/xdzczk/nostrmash/internal/query"
)

// discoveryHomeBuildTimeout bounds the total time a cache-miss /home build
// may take, independent of how many sub-calls it fans out to. Every
// individual aggregate behind this handler is now either a sub-millisecond
// snapshot lookup or a normal indexed query, so 5s is generous headroom for
// worst-case DB latency while still failing fast (and freeing the
// singleflight lock for the next attempt) instead of hanging for the full
// client-side fetch timeout the way the pre-snapshot live aggregates did.
const discoveryHomeBuildTimeout = 5 * time.Second

func (h Handlers) GetTrendingNotes(w http.ResponseWriter, r *http.Request) {
	window, windowLabel, err := parseTrendingWindow(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	viewer, ok := h.parseDiscoveryViewer(w, r)
	if !ok {
		return
	}
	lang, err := parseOptionalDiscoveryLanguage(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	offset, err := parseBoundedNonNegativeInt(r, "offset", 0, discoveryOffsetCap(viewer))
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	scope := discoveryScopeWithViewer(lang, viewer)
	cachePolicy := h.rankingCachePolicy("trending_notes", map[string]any{
		"window": windowLabel,
		"limit":  limit,
		"offset": offset,
	}, lang, viewer)
	if err := h.servePublicCached(r.Context(), w, cachePolicy, func(ctx context.Context) (map[string]any, error) {
		var notes []query.TrendingNote
		var meta query.PersonalizationMeta
		var notesErr error
		if viewer == "" {
			notes, notesErr = h.service.GetTrendingNotes(ctx, window, scope, limit, offset)
		} else {
			notes, meta, notesErr = h.service.GetPersonalizedTrendingNotes(ctx, window, scope, limit, offset)
		}
		degraded := false
		if notesErr != nil {
			if query.IsUnsupportedCapability(notesErr) {
				return nil, notesErr
			}
			recordDiscoveryDegrade(ctx, "trending_notes", "backend", notesErr, nil)
			notes = nil
			degraded = true
		}
		noteAuthorPubkeys := make([]string, 0, len(notes))
		for _, note := range notes {
			noteAuthorPubkeys = append(noteAuthorPubkeys, note.AuthorPubkey)
		}
		noteAuthorIdentities, identitiesErr := h.resolveProfileIdentities(ctx, noteAuthorPubkeys)
		if identitiesErr != nil {
			recordDiscoveryDegrade(ctx, "trending_notes", "profile_identities", identitiesErr, nil)
			noteAuthorIdentities = map[string]profileIdentityFields{}
			degraded = true
		}
		payload := buildDiscoveryNoteItems(notes, noteAuthorIdentities, windowLabel)
		h.attachTopZaps(ctx, payload)
		payloadResponse := map[string]any{
			"surface":     "trending",
			"window":      windowLabel,
			"notes":       payload,
			"consistency": "eventual",
		}
		if degraded {
			payloadResponse["degraded"] = true
		}
		computedAt := time.Now().UTC()
		addDiscoveryListMeta(payloadResponse, windowLabel, &computedAt, len(payload))
		h.addDiscoveryTrustMetadata(payloadResponse)
		h.attachDiscoveryLanguage(ctx, payloadResponse, lang)
		attachPersonalization(payloadResponse, meta)
		personalizeRankingItems(payload, meta, noteCohortEvidence(notes), nil, "followed_engagers", "cohort_engagers", "accounts")
		return payloadResponse, nil
	}); err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "trending notes are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
}

func (h Handlers) GetTrendingLongForm(w http.ResponseWriter, r *http.Request) {
	window, windowLabel, err := parseTrendingWindow(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	viewer, ok := h.parseDiscoveryViewer(w, r)
	if !ok {
		return
	}
	lang, err := parseOptionalDiscoveryLanguage(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	offset, err := parseBoundedNonNegativeInt(r, "offset", 0, discoveryOffsetCap(viewer))
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	scope := discoveryScopeWithViewer(lang, viewer)
	cachePolicy := h.rankingCachePolicy("trending_long_form", map[string]any{
		"window": windowLabel,
		"limit":  limit,
		"offset": offset,
	}, lang, viewer)
	if err := h.servePublicCached(r.Context(), w, cachePolicy, func(ctx context.Context) (map[string]any, error) {
		var articles []query.TrendingNote
		var meta query.PersonalizationMeta
		var articlesErr error
		if viewer == "" {
			articles, articlesErr = h.service.GetTrendingLongForm(ctx, window, scope, limit, offset)
		} else {
			articles, meta, articlesErr = h.service.GetPersonalizedTrendingLongForm(ctx, window, scope, limit, offset)
		}
		degraded := false
		if articlesErr != nil {
			if query.IsUnsupportedCapability(articlesErr) {
				return nil, articlesErr
			}
			recordDiscoveryDegrade(ctx, "trending_long_form", "backend", articlesErr, nil)
			articles = nil
			degraded = true
		}
		authorPubkeys := make([]string, 0, len(articles))
		for _, article := range articles {
			authorPubkeys = append(authorPubkeys, article.AuthorPubkey)
		}
		authorIdentities, identitiesErr := h.resolveProfileIdentities(ctx, authorPubkeys)
		if identitiesErr != nil {
			recordDiscoveryDegrade(ctx, "trending_long_form", "profile_identities", identitiesErr, nil)
			authorIdentities = map[string]profileIdentityFields{}
			degraded = true
		}
		payload := buildDiscoveryNoteItems(articles, authorIdentities, windowLabel)
		h.attachTopZaps(ctx, payload)
		payloadResponse := map[string]any{
			"surface":     "trending_long_form",
			"window":      windowLabel,
			"articles":    payload,
			"consistency": "eventual",
		}
		if degraded {
			payloadResponse["degraded"] = true
		}
		computedAt := time.Now().UTC()
		addDiscoveryListMeta(payloadResponse, windowLabel, &computedAt, len(payload))
		h.addDiscoveryTrustMetadata(payloadResponse)
		h.attachDiscoveryLanguage(ctx, payloadResponse, lang)
		attachPersonalization(payloadResponse, meta)
		personalizeRankingItems(payload, meta, noteCohortEvidence(articles), nil, "followed_engagers", "cohort_engagers", "accounts")
		return payloadResponse, nil
	}); err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "trending long-form is not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
}

func (h Handlers) GetTrendingMedia(w http.ResponseWriter, r *http.Request) {
	format := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("format")))
	if _, ok := nostr.TrendingMediaKinds(format); !ok {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "format must be pictures, videos, or shorts")
		return
	}
	window, windowLabel, err := parseTrendingWindow(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	viewer, ok := h.parseDiscoveryViewer(w, r)
	if !ok {
		return
	}
	lang, err := parseOptionalDiscoveryLanguage(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	offset, err := parseBoundedNonNegativeInt(r, "offset", 0, discoveryOffsetCap(viewer))
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	scope := discoveryScopeWithViewer(lang, viewer)
	cachePolicy := h.rankingCachePolicy("trending_media", map[string]any{
		"format": format,
		"window": windowLabel,
		"limit":  limit,
		"offset": offset,
	}, lang, viewer)
	if err := h.servePublicCached(r.Context(), w, cachePolicy, func(ctx context.Context) (map[string]any, error) {
		var events []query.TrendingNote
		var meta query.PersonalizationMeta
		var eventsErr error
		if viewer == "" {
			events, eventsErr = h.service.GetTrendingMedia(ctx, format, window, scope, limit, offset)
		} else {
			events, meta, eventsErr = h.service.GetPersonalizedTrendingMedia(ctx, format, window, scope, limit, offset)
		}
		degraded := false
		if eventsErr != nil {
			if query.IsUnsupportedCapability(eventsErr) {
				return nil, eventsErr
			}
			recordDiscoveryDegrade(ctx, "trending_media", "backend", eventsErr, nil)
			events = nil
			degraded = true
		}
		authorPubkeys := make([]string, 0, len(events))
		for _, event := range events {
			authorPubkeys = append(authorPubkeys, event.AuthorPubkey)
		}
		authorIdentities, identitiesErr := h.resolveProfileIdentities(ctx, authorPubkeys)
		if identitiesErr != nil {
			recordDiscoveryDegrade(ctx, "trending_media", "profile_identities", identitiesErr, nil)
			authorIdentities = map[string]profileIdentityFields{}
			degraded = true
		}
		payload := buildDiscoveryNoteItems(events, authorIdentities, windowLabel)
		h.attachTopZaps(ctx, payload)
		payloadResponse := map[string]any{
			"surface":     "trending_media",
			"format":      format,
			"window":      windowLabel,
			"events":      payload,
			"consistency": "eventual",
		}
		if degraded {
			payloadResponse["degraded"] = true
		}
		computedAt := time.Now().UTC()
		addDiscoveryListMeta(payloadResponse, windowLabel, &computedAt, len(payload))
		h.addDiscoveryTrustMetadata(payloadResponse)
		h.attachDiscoveryLanguage(ctx, payloadResponse, lang)
		attachPersonalization(payloadResponse, meta)
		personalizeRankingItems(payload, meta, noteCohortEvidence(events), nil, "followed_engagers", "cohort_engagers", "accounts")
		return payloadResponse, nil
	}); err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "trending media is not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
}

func (h Handlers) GetHotConversations(w http.ResponseWriter, r *http.Request) {
	window, windowLabel, err := parseTrendingWindow(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	viewer, ok := h.parseDiscoveryViewer(w, r)
	if !ok {
		return
	}
	lang, err := parseOptionalDiscoveryLanguage(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	offset, err := parseBoundedNonNegativeInt(r, "offset", 0, discoveryOffsetCap(viewer))
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	scope := discoveryScopeWithViewer(lang, viewer)
	cachePolicy := h.rankingCachePolicy("hot_conversations", map[string]any{
		"window": windowLabel,
		"limit":  limit,
		"offset": offset,
	}, lang, viewer)
	if err := h.servePublicCached(r.Context(), w, cachePolicy, func(ctx context.Context) (map[string]any, error) {
		var conversations []query.HotConversation
		var meta query.PersonalizationMeta
		var conversationsErr error
		if viewer == "" {
			conversations, conversationsErr = h.service.GetHotConversations(ctx, window, scope, limit, offset)
		} else {
			conversations, meta, conversationsErr = h.service.GetPersonalizedHotConversations(ctx, window, scope, limit, offset)
		}
		degraded := false
		if conversationsErr != nil {
			if query.IsUnsupportedCapability(conversationsErr) {
				return nil, conversationsErr
			}
			recordDiscoveryDegrade(ctx, "hot_conversations", "backend", conversationsErr, nil)
			conversations = nil
			degraded = true
		}
		items := make([]map[string]any, 0, len(conversations))
		for index, conversation := range conversations {
			item := map[string]any{
				"root_event_id":     conversation.RootEventID,
				"author_pubkey":     conversation.AuthorPubkey,
				"created_at":        conversation.CreatedAt,
				"content":           conversation.Content,
				"reply_count":       conversation.ReplyCount,
				"repost_count":      conversation.RepostCount,
				"reaction_count":    conversation.ReactionCount,
				"zap_count":         conversation.ZapCount,
				"zap_msats":         conversation.ZapMSats,
				"participant_count": conversation.ParticipantCount,
				"last_activity_at":  conversation.LastActivityAt,
				"activity": map[string]any{
					"replies_24h": conversation.Replies24h,
					"replies_7d":  conversation.Replies7d,
					"replies_30d": conversation.Replies30d,
				},
				"velocity_score": conversation.VelocityScore,
				"ranking":        buildConversationRanking(conversation, index+1),
			}
			item["preview"] = buildNotePreviewPayload(conversation.RootEventID, conversation.Content)
			items = append(items, item)
		}
		h.attachTopZaps(ctx, items)
		payloadResponse := map[string]any{
			"surface":       "hot_conversations",
			"window":        windowLabel,
			"conversations": items,
			"consistency":   "eventual",
		}
		if degraded {
			payloadResponse["degraded"] = true
		}
		computedAt := time.Now().UTC()
		addDiscoveryListMeta(payloadResponse, windowLabel, &computedAt, len(items))
		h.addDiscoveryTrustMetadata(payloadResponse)
		h.attachDiscoveryLanguage(ctx, payloadResponse, lang)
		evidence, scores := conversationBlend(conversations)
		attachPersonalization(payloadResponse, meta)
		personalizeRankingItems(items, meta, evidence, scores, "followed_replies", "cohort_replies", "replies")
		return payloadResponse, nil
	}); err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "hot conversations are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
}

// discoveryHomeRequest is one homepage bundle. The zero-value limits are the
// handler defaults; the warmer refreshes exactly those keys so an unscoped
// GET /api/v1/discovery/home is usually a cache hit.
type discoveryHomeRequest struct {
	window           time.Duration
	windowLabel      string
	notesLimit       int
	hashtagsLimit    int
	profilesLimit    int
	domainsLimit     int
	hashtagStatLimit int
	language         string
	viewer           string
}

func defaultDiscoveryHomeRequest(window time.Duration, label string) discoveryHomeRequest {
	return discoveryHomeRequest{
		window:           window,
		windowLabel:      label,
		notesLimit:       10,
		hashtagsLimit:    10,
		profilesLimit:    10,
		domainsLimit:     10,
		hashtagStatLimit: 10,
	}
}

func (h Handlers) discoveryHomePolicy(req discoveryHomeRequest) publicResponseCachePolicy {
	family := publicCacheFamilyBundle
	params := map[string]any{
		"window":             req.windowLabel,
		"notes_limit":        req.notesLimit,
		"hashtags_limit":     req.hashtagsLimit,
		"profiles_limit":     req.profilesLimit,
		"domains_limit":      req.domainsLimit,
		"hashtag_stat_limit": req.hashtagStatLimit,
	}
	if req.viewer != "" {
		family = publicCacheFamilyPersonalized
		params["viewer"] = req.viewer
	}
	return h.newPublicCachePolicy(family, "discovery_home", discoveryCacheParams(params, req.language))
}

func (h Handlers) GetDiscoveryHome(w http.ResponseWriter, r *http.Request) {
	window, windowLabel, err := parseTrendingWindow(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	viewer, ok := h.parseDiscoveryViewer(w, r)
	if !ok {
		return
	}
	lang, err := parseOptionalDiscoveryLanguage(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	notesLimit, err := parseBoundedPositiveInt(r, "notes_limit", 10, 20)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	hashtagsLimit, err := parseBoundedPositiveInt(r, "hashtags_limit", 10, 20)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	profilesLimit, err := parseBoundedPositiveInt(r, "profiles_limit", 10, 20)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	domainsLimit, err := parseBoundedPositiveInt(r, "domains_limit", 10, 20)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	hashtagStatLimit, err := parseBoundedPositiveInt(r, "hashtag_stat_limit", 10, 50)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	homeReq := discoveryHomeRequest{
		window:           window,
		windowLabel:      windowLabel,
		notesLimit:       notesLimit,
		hashtagsLimit:    hashtagsLimit,
		profilesLimit:    profilesLimit,
		domainsLimit:     domainsLimit,
		hashtagStatLimit: hashtagStatLimit,
		language:         lang,
		viewer:           viewer,
	}
	if err := h.servePublicCached(r.Context(), w, h.discoveryHomePolicy(homeReq), func(reqCtx context.Context) (map[string]any, error) {
		// Bound the whole fan-out below by a fixed wall-clock budget,
		// independent of the incoming context's deadline (which, for a
		// background stale-while-revalidate refresh, is already detached
		// from any client). Every call this handler makes is now a
		// snapshot lookup or a normal indexed query, so a build that still
		// exceeds this is a genuine anomaly (e.g. a starved connection
		// pool) — better to fail this one request fast and release the
		// singleflight lock than to hang for the client's full fetch
		// timeout the way the pre-snapshot live aggregates did.
		buildCtx, cancel := context.WithTimeout(reqCtx, discoveryHomeBuildTimeout)
		defer cancel()
		return h.buildDiscoveryHome(buildCtx, homeReq)
	}); err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "homepage discovery bundle is not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
}

// discoveryHomeWarmTimeout is longer than the request-path budget. The
// warmer runs off the request path, and a build that takes a few seconds
// is still worth caching so the next user request does not pay it.
const discoveryHomeWarmTimeout = 15 * time.Second

// RunDiscoveryHomeWarmer keeps the default 24h and 7d homepage bundles in
// the in-process cache. Traffic is sparse enough that a 60s TTL expired
// between requests and every hit rebuilt the page (p95 several seconds).
// Blocks until ctx is cancelled. A disabled cache returns immediately.
func (h Handlers) RunDiscoveryHomeWarmer(ctx context.Context) {
	if h.discoveryCache == nil || !h.cacheConfig.Enabled {
		return
	}
	interval := h.cacheConfig.ttlForFamily(publicCacheFamilyBundle) / 2
	if interval < 30*time.Second {
		interval = 30 * time.Second
	}
	if interval > 2*time.Minute {
		interval = 2 * time.Minute
	}
	apiErrLog.Info("discovery_home_warmer_enabled", "interval", interval.String())
	h.warmDiscoveryHome(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.warmDiscoveryHome(ctx)
		}
	}
}

func (h Handlers) warmDiscoveryHome(ctx context.Context) {
	windows := []struct {
		window time.Duration
		label  string
	}{
		{24 * time.Hour, "24h"},
		{7 * 24 * time.Hour, "7d"},
	}
	languages := []string{""}
	if stats, err := h.service.GetPublicDiscoveryNetworkStats(ctx, 8, query.DiscoveryScope{}); err == nil {
		languages = append(languages, selectableDiscoveryLanguages(stats, 5)...)
	}
	for _, lang := range languages {
		for _, spec := range windows {
			if !h.warmOneDiscoveryHome(ctx, spec.window, spec.label, lang) {
				return
			}
		}
	}
}

func (h Handlers) warmOneDiscoveryHome(ctx context.Context, window time.Duration, label, lang string) bool {
	homeReq := defaultDiscoveryHomeRequest(window, label)
	homeReq.language = lang
	buildCtx, cancel := context.WithTimeout(ctx, discoveryHomeWarmTimeout)
	started := time.Now()
	_, err := h.buildAndCachePayload(buildCtx, h.discoveryHomePolicy(homeReq), func(ctx context.Context) (map[string]any, error) {
		return h.buildDiscoveryHome(ctx, homeReq)
	})
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			return false
		}
		apiErrLog.Error("discovery_home_warm_failed", "window", label, "lang", lang, "error", err, "duration_s", time.Since(started).Seconds())
		return true
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		apiErrLog.Info("discovery_home_warm_slow", "window", label, "lang", lang, "duration_s", elapsed.Seconds())
	}
	return true
}

func (h Handlers) buildDiscoveryHome(ctx context.Context, req discoveryHomeRequest) (map[string]any, error) {
	// The five aggregates below are mutually independent — none of
	// them depends on another's result — so fan them out concurrently
	// instead of paying their latencies serially. A single section
	// failure degrades that section and keeps the rest of the bundle.
	var (
		notes            []query.TrendingNote
		trendingProfiles []query.TrendingProfile
		risingProfiles   []query.TrendingProfile
		domains          []query.DomainSummary
		networkStats     query.PublicDiscoveryNetworkStats
		notesErr         error
		trendingErr      error
		risingErr        error
		domainsErr       error
		networkErr       error
	)
	scope := discoveryScopeWithViewer(req.language, req.viewer)
	var (
		notesMeta    query.PersonalizationMeta
		trendingMeta query.PersonalizationMeta
		risingMeta   query.PersonalizationMeta
		domainsMeta  query.PersonalizationMeta
		cohortErr    error
	)
	personalized := req.viewer != ""
	if personalized {
		// Resolve the follow list once so the four ranking calls share it.
		cohortErr = h.service.PrepareViewerCohort(ctx, req.viewer)
		if cohortErr != nil {
			personalized = false
			scope.Viewer = ""
		}
	}
	var wg sync.WaitGroup
	wg.Add(5)
	go func() {
		defer wg.Done()
		if personalized {
			notes, notesMeta, notesErr = h.service.GetPersonalizedTrendingNotes(ctx, req.window, scope, req.notesLimit, 0)
			return
		}
		notes, notesErr = h.service.GetTrendingNotes(ctx, req.window, scope, req.notesLimit, 0)
	}()
	go func() {
		defer wg.Done()
		if personalized {
			trendingProfiles, trendingMeta, trendingErr = h.service.GetPersonalizedTrendingProfiles(ctx, req.window, scope, req.profilesLimit, 0)
			return
		}
		trendingProfiles, trendingErr = h.service.GetTrendingProfiles(ctx, req.window, scope, req.profilesLimit, 0)
	}()
	go func() {
		defer wg.Done()
		if personalized {
			risingProfiles, risingMeta, risingErr = h.service.GetPersonalizedRisingProfiles(ctx, req.window, scope, req.profilesLimit, 0)
			return
		}
		risingProfiles, risingErr = h.service.GetRisingProfiles(ctx, req.window, scope, req.profilesLimit, 0)
	}()
	go func() {
		defer wg.Done()
		// Trending domains are served from the top_domains_24h/7d
		// snapshot (see internal/derivation/projection_relay_window_snapshots.go).
		// /discovery/domains/trending reads the same snapshot.
		if personalized {
			domains, domainsMeta, domainsErr = h.service.GetPersonalizedTrendingDomains(ctx, req.window, scope, req.domainsLimit, 0)
			return
		}
		domains, domainsErr = h.service.GetHomeTrendingDomains(ctx, req.window, scope, req.domainsLimit)
	}()
	go func() {
		defer wg.Done()
		// Trending hashtags are served from the same top_hashtags_24h/7d
		// snapshot that backs network_summary.top_hashtags below, rather
		// than a second live GetTrendingHashtags aggregate. Fetch enough
		// rows to satisfy whichever of hashtagsLimit / hashtagStatLimit
		// is larger so both sections can slice down from one call.
		// A language scope replaces note volume, active authors, and the
		// hashtag list. Top languages stay global so the selector can
		// offer the other codes.
		networkStats, networkErr = h.service.GetPublicDiscoveryNetworkStats(ctx, max(req.hashtagsLimit, req.hashtagStatLimit), scope)
	}()
	wg.Wait()

	var degraded []string
	if cohortErr != nil {
		recordDiscoveryDegrade(ctx, "discovery_home", "viewer_cohort", cohortErr, &degraded)
	}
	if notesErr != nil {
		recordDiscoveryDegrade(ctx, "discovery_home", "trending_notes", notesErr, &degraded)
		notes = nil
	}
	if trendingErr != nil {
		recordDiscoveryDegrade(ctx, "discovery_home", "trending_profiles", trendingErr, &degraded)
		trendingProfiles = nil
	}
	if risingErr != nil {
		recordDiscoveryDegrade(ctx, "discovery_home", "rising_profiles", risingErr, &degraded)
		risingProfiles = nil
	}
	if domainsErr != nil {
		recordDiscoveryDegrade(ctx, "discovery_home", "trending_domains", domainsErr, &degraded)
		domains = nil
	}
	if networkErr != nil {
		recordDiscoveryDegrade(ctx, "discovery_home", "network_summary", networkErr, &degraded)
		networkStats = query.PublicDiscoveryNetworkStats{}
	}

	hashtagItems := buildDiscoveryHashtagItems(sliceHashtags(pickHashtagWindow(networkStats.TopHashtags, req.windowLabel), req.hashtagsLimit))
	h.attachHashtagBaselines(ctx, req.windowLabel, hashtagItems)

	identityPubkeys := make([]string, 0, len(notes)+len(trendingProfiles)+len(risingProfiles))
	for _, note := range notes {
		identityPubkeys = append(identityPubkeys, note.AuthorPubkey)
	}
	for _, profile := range trendingProfiles {
		identityPubkeys = append(identityPubkeys, profile.Pubkey)
	}
	for _, profile := range risingProfiles {
		identityPubkeys = append(identityPubkeys, profile.Pubkey)
	}
	profileIdentities, identitiesErr := h.resolveProfileIdentities(ctx, identityPubkeys)
	if identitiesErr != nil {
		recordDiscoveryDegrade(ctx, "discovery_home", "profile_identities", identitiesErr, &degraded)
		profileIdentities = map[string]profileIdentityFields{}
	}

	noteItems := buildDiscoveryNoteItems(notes, profileIdentities, req.windowLabel)
	h.attachTopZaps(ctx, noteItems)
	domainItems := buildDiscoveryDomainItems(domains, req.windowLabel)
	h.attachDomainBaselines(ctx, req.windowLabel, domainItems)
	trendingProfileItems := buildDiscoveryProfileItems(trendingProfiles, profileIdentities, discoverySurfaceTrending, req.windowLabel)
	risingProfileItems := buildDiscoveryProfileItems(risingProfiles, profileIdentities, discoverySurfaceRising, req.windowLabel)
	if personalized {
		personalizeRankingItems(noteItems, notesMeta, noteCohortEvidence(notes), nil, "followed_engagers", "cohort_engagers", "accounts")
		personalizeRankingItems(trendingProfileItems, trendingMeta, profileCohortEvidence(trendingProfiles), nil, "followed_engagers", "cohort_engagers", "accounts")
		personalizeRankingItems(risingProfileItems, risingMeta, profileCohortEvidence(risingProfiles), nil, "followed_by", "cohort_new_followers", "followers")
		domainEvidence, domainScores := domainBlend(domains)
		personalizeRankingItems(domainItems, domainsMeta, domainEvidence, domainScores, "followed_authors", "cohort_authors", "authors")
	}
	network := map[string]any{
		"totals": map[string]any{
			"events_ingested":    networkStats.EventsIngested,
			"projected_profiles": networkStats.ProjectedProfiles,
		},
		"activity": map[string]any{
			"active_authors": networkStats.ActiveAuthors,
			"note_volume":    networkStats.NoteVolume,
		},
		"relays": buildRelayStatsPayload(networkStats, "7d"),
	}
	if networkStats.TopHashtags != nil {
		// networkStats was fetched with max(hashtagsLimit, hashtagStatLimit)
		// rows so the trending_hashtags section above could slice down to
		// hashtagsLimit; re-slice here to the caller's requested
		// hashtag_stat_limit so network_summary.top_hashtags honors its
		// own independent limit.
		network["top_hashtags"] = &query.TrendingHashtagWindows{
			Last24h: sliceHashtags(networkStats.TopHashtags.Last24h, req.hashtagStatLimit),
			Last7d:  sliceHashtags(networkStats.TopHashtags.Last7d, req.hashtagStatLimit),
			Last30d: sliceHashtags(networkStats.TopHashtags.Last30d, req.hashtagStatLimit),
		}
	}
	if len(networkStats.TopLanguages24h) > 0 || len(networkStats.TopLanguages7d) > 0 || len(networkStats.TopLanguages30d) > 0 {
		network["top_languages"] = map[string]any{
			"24h": networkStats.TopLanguages24h,
			"7d":  networkStats.TopLanguages7d,
			"30d": networkStats.TopLanguages30d,
		}
	}

	payload := map[string]any{
		"surface":     "home",
		"window":      req.windowLabel,
		"computed_at": networkStats.ComputedAt,
		"sections": map[string]any{
			"trending_notes":    noteItems,
			"trending_hashtags": hashtagItems,
			"trending_domains":  domainItems,
			"profiles": map[string]any{
				"trending": trendingProfileItems,
				"rising":   risingProfileItems,
			},
			"network_summary": network,
		},
		"consistency": "eventual",
	}
	if req.language != "" {
		payload["language"] = req.language
	}
	if len(degraded) > 0 {
		payload["degraded"] = true
		payload["degraded_reasons"] = degraded
	}
	computedAt := networkStats.ComputedAt
	if computedAt == nil {
		now := time.Now().UTC()
		computedAt = &now
	}
	addDiscoveryListMeta(payload, req.windowLabel, computedAt, len(noteItems)+len(hashtagItems)+len(trendingProfileItems)+len(domainItems))
	h.addDiscoveryTrustMetadata(payload)
	if personalized {
		meta := notesMeta
		if meta.Source == "" {
			meta = trendingMeta
		}
		if meta.Source == "" {
			meta = risingMeta
		}
		if meta.Source == "" {
			meta = domainsMeta
		}
		attachPersonalization(payload, meta)
	}
	return payload, nil
}
