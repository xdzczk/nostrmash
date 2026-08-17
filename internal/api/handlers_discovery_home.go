package api

import (
	"context"
	"net/http"
	"sync"
	"time"

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
	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	offset, err := parseBoundedNonNegativeInt(r, "offset", 0, 5000)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyDiscovery, "trending_notes", map[string]any{
		"window": windowLabel,
		"limit":  limit,
		"offset": offset,
	})
	if err := h.servePublicCached(r.Context(), w, cachePolicy, func(ctx context.Context) (map[string]any, error) {
		notes, notesErr := h.service.GetTrendingNotes(ctx, window, limit, offset)
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
		payload := buildDiscoveryNoteItems(notes, noteAuthorIdentities)
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
	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	offset, err := parseBoundedNonNegativeInt(r, "offset", 0, 5000)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyDiscovery, "trending_long_form", map[string]any{
		"window": windowLabel,
		"limit":  limit,
		"offset": offset,
	})
	if err := h.servePublicCached(r.Context(), w, cachePolicy, func(ctx context.Context) (map[string]any, error) {
		articles, articlesErr := h.service.GetTrendingLongForm(ctx, window, limit, offset)
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
		payload := buildDiscoveryNoteItems(articles, authorIdentities)
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

func (h Handlers) GetHotConversations(w http.ResponseWriter, r *http.Request) {
	window, windowLabel, err := parseTrendingWindow(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	offset, err := parseBoundedNonNegativeInt(r, "offset", 0, 5000)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyDiscovery, "hot_conversations", map[string]any{
		"window": windowLabel,
		"limit":  limit,
		"offset": offset,
	})
	if err := h.servePublicCached(r.Context(), w, cachePolicy, func(ctx context.Context) (map[string]any, error) {
		conversations, conversationsErr := h.service.GetHotConversations(ctx, window, limit, offset)
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
				},
				"velocity_score": conversation.VelocityScore,
				"ranking":        buildConversationRanking(conversation, index+1),
			}
			item["preview"] = buildNotePreviewPayload(conversation.RootEventID, conversation.Content)
			items = append(items, item)
		}
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

func (h Handlers) GetDiscoveryHome(w http.ResponseWriter, r *http.Request) {
	window, windowLabel, err := parseTrendingWindow(r)
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
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyBundle, "discovery_home", map[string]any{
		"window":             windowLabel,
		"notes_limit":        notesLimit,
		"hashtags_limit":     hashtagsLimit,
		"profiles_limit":     profilesLimit,
		"domains_limit":      domainsLimit,
		"hashtag_stat_limit": hashtagStatLimit,
	})
	if err := h.servePublicCached(r.Context(), w, cachePolicy, func(reqCtx context.Context) (map[string]any, error) {
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
		var wg sync.WaitGroup
		wg.Add(5)
		go func() {
			defer wg.Done()
			notes, notesErr = h.service.GetTrendingNotes(buildCtx, window, notesLimit, 0)
		}()
		go func() {
			defer wg.Done()
			trendingProfiles, trendingErr = h.service.GetTrendingProfiles(buildCtx, window, profilesLimit, 0)
		}()
		go func() {
			defer wg.Done()
			risingProfiles, risingErr = h.service.GetRisingProfiles(buildCtx, window, profilesLimit, 0)
		}()
		go func() {
			defer wg.Done()
			// Trending domains are served from the top_domains_24h/7d
			// snapshot (see internal/derivation/projection_relay_window_snapshots.go)
			// instead of the live COUNT(DISTINCT) aggregate behind the
			// standalone /discovery/domains/trending endpoint.
			domains, domainsErr = h.service.GetHomeTrendingDomains(buildCtx, window, domainsLimit)
		}()
		go func() {
			defer wg.Done()
			// Trending hashtags are served from the same top_hashtags_24h/7d
			// snapshot that backs network_summary.top_hashtags below, rather
			// than a second live GetTrendingHashtags aggregate. Fetch enough
			// rows to satisfy whichever of hashtagsLimit / hashtagStatLimit
			// is larger so both sections can slice down from one call.
			networkStats, networkErr = h.service.GetPublicDiscoveryNetworkStats(buildCtx, max(hashtagsLimit, hashtagStatLimit))
		}()
		wg.Wait()

		var degraded []string
		if notesErr != nil {
			recordDiscoveryDegrade(reqCtx, "discovery_home", "trending_notes", notesErr, &degraded)
			notes = nil
		}
		if trendingErr != nil {
			recordDiscoveryDegrade(reqCtx, "discovery_home", "trending_profiles", trendingErr, &degraded)
			trendingProfiles = nil
		}
		if risingErr != nil {
			recordDiscoveryDegrade(reqCtx, "discovery_home", "rising_profiles", risingErr, &degraded)
			risingProfiles = nil
		}
		if domainsErr != nil {
			recordDiscoveryDegrade(reqCtx, "discovery_home", "trending_domains", domainsErr, &degraded)
			domains = nil
		}
		if networkErr != nil {
			recordDiscoveryDegrade(reqCtx, "discovery_home", "network_summary", networkErr, &degraded)
			networkStats = query.PublicDiscoveryNetworkStats{}
		}

		hashtagItems := buildDiscoveryHashtagItems(sliceHashtags(pickHashtagWindow(networkStats.TopHashtags, windowLabel), hashtagsLimit))

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
		profileIdentities, identitiesErr := h.resolveProfileIdentities(buildCtx, identityPubkeys)
		if identitiesErr != nil {
			recordDiscoveryDegrade(reqCtx, "discovery_home", "profile_identities", identitiesErr, &degraded)
			profileIdentities = map[string]profileIdentityFields{}
		}

		noteItems := buildDiscoveryNoteItems(notes, profileIdentities)
		domainItems := buildDiscoveryDomainItems(domains, windowLabel)
		trendingProfileItems := buildDiscoveryProfileItems(trendingProfiles, profileIdentities)
		risingProfileItems := buildDiscoveryProfileItems(risingProfiles, profileIdentities)
		network := map[string]any{
			"totals": map[string]any{
				"events_ingested":    networkStats.EventsIngested,
				"projected_profiles": networkStats.ProjectedProfiles,
			},
			"activity": map[string]any{
				"active_authors": networkStats.ActiveAuthors,
				"note_volume":    networkStats.NoteVolume,
			},
			"relays": buildRelayStatsPayload(networkStats),
		}
		if networkStats.TopHashtags != nil {
			// networkStats was fetched with max(hashtagsLimit, hashtagStatLimit)
			// rows so the trending_hashtags section above could slice down to
			// hashtagsLimit; re-slice here to the caller's requested
			// hashtag_stat_limit so network_summary.top_hashtags honors its
			// own independent limit.
			network["top_hashtags"] = &query.TrendingHashtagWindows{
				Last24h: sliceHashtags(networkStats.TopHashtags.Last24h, hashtagStatLimit),
				Last7d:  sliceHashtags(networkStats.TopHashtags.Last7d, hashtagStatLimit),
			}
		}
		if len(networkStats.TopLanguages24h) > 0 || len(networkStats.TopLanguages7d) > 0 {
			network["top_languages"] = map[string]any{
				"24h": networkStats.TopLanguages24h,
				"7d":  networkStats.TopLanguages7d,
			}
		}

		payload := map[string]any{
			"surface":     "home",
			"window":      windowLabel,
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
		if len(degraded) > 0 {
			payload["degraded"] = true
			payload["degraded_reasons"] = degraded
		}
		computedAt := networkStats.ComputedAt
		if computedAt == nil {
			now := time.Now().UTC()
			computedAt = &now
		}
		addDiscoveryListMeta(payload, windowLabel, computedAt, len(noteItems)+len(hashtagItems)+len(trendingProfileItems)+len(domainItems))
		h.addDiscoveryTrustMetadata(payload)
		return payload, nil
	}); err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "homepage discovery bundle is not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
}
