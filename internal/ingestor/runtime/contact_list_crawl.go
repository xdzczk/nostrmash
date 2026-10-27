package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/ingestor/backfill"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store"
)

// contactListPageLimit is one kind-3 list plus one kind-10002 relay list.
const contactListPageLimit = 2

type contactListCrawlStore interface {
	SeedContactListCrawl(ctx context.Context, batch int, rescanInterval time.Duration) (int, error)
	ClaimContactListCrawl(ctx context.Context, limit int, lease time.Duration) ([]store.ContactListCrawlEntry, error)
	MarkContactListCrawlFetched(ctx context.Context, pubkey string, foundList bool, cooldown time.Duration) error
	MarkContactListCrawlFailed(ctx context.Context, pubkey string, retryAfter time.Duration, cause error) error
	EnqueueContactListCrawl(ctx context.Context, pubkeys []string, hop int, maxPending int) (int, error)
	ContactListCrawlDueByHop(ctx context.Context) (map[int]int, error)
	LatestKind10002Raw(ctx context.Context, pubkey string) ([]byte, error)
}

var _ contactListCrawlStore = (*store.PostgresStore)(nil)

func runContactListCrawlLoop(
	ctx context.Context,
	log *slog.Logger,
	st contactListCrawlStore,
	cfg config.IngestorContactCrawlConfig,
	fallbackRelays []string,
	relayAllowlist []string,
	disabledRelays []string,
	requireTLS bool,
	fetcher backfill.PageFetcher,
	onMessage backfill.MessageHandler,
) {
	if st == nil || fetcher == nil || onMessage == nil || !cfg.Enabled {
		return
	}
	if log == nil {
		log = slog.Default()
	}
	run := func() {
		runContactListCrawlCycle(ctx, log, st, cfg, fallbackRelays, relayAllowlist, disabledRelays, requireTLS, fetcher, onMessage)
	}
	run()
	if cfg.Interval <= 0 {
		return
	}
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}

func runContactListCrawlCycle(
	ctx context.Context,
	log *slog.Logger,
	st contactListCrawlStore,
	cfg config.IngestorContactCrawlConfig,
	fallbackRelays []string,
	relayAllowlist []string,
	disabledRelays []string,
	requireTLS bool,
	fetcher backfill.PageFetcher,
	onMessage backfill.MessageHandler,
) {
	if log == nil {
		log = slog.Default()
	}
	if err := ctx.Err(); err != nil {
		return
	}
	seeded, err := st.SeedContactListCrawl(ctx, cfg.SeedBatch, cfg.SeedRescanInterval)
	if err != nil {
		log.Warn("contact_list_crawl_seed_failed", "error", err)
	}
	claimed, err := st.ClaimContactListCrawl(ctx, cfg.BatchSize, cfg.RetryDelay)
	if err != nil {
		metrics.IncContactListCrawlCycle("claim_error")
		log.Warn("contact_list_crawl_claim_failed", "error", err)
		return
	}
	defer publishContactListCrawlDepth(ctx, log, st, cfg.MaxHops)

	if len(claimed) == 0 {
		metrics.IncContactListCrawlCycle("empty")
		if seeded > 0 {
			log.Info("contact_list_crawl_cycle", "seeded", seeded, "claimed", 0)
		} else {
			log.Debug("contact_list_crawl_cycle", "seeded", 0, "claimed", 0)
		}
		return
	}

	allow := relayAllowlist
	if len(cfg.RelayAllowlist) > 0 {
		allow = cfg.RelayAllowlist
	}
	found := 0
	missing := 0
	failures := 0
	enqueued := 0
	for _, entry := range claimed {
		if err := ctx.Err(); err != nil {
			return
		}
		n, outcome := crawlOneContactList(ctx, log, st, cfg, entry, fallbackRelays, allow, disabledRelays, requireTLS, fetcher, onMessage)
		enqueued += n
		switch outcome {
		case "found":
			found++
			metrics.IncContactListCrawlPubkey("found")
		case "missing":
			missing++
			metrics.IncContactListCrawlPubkey("missing")
		default:
			failures++
			metrics.IncContactListCrawlPubkey("error")
		}
	}
	metrics.AddContactListCrawlTagsEnqueued(float64(enqueued))
	switch {
	case failures == 0:
		metrics.IncContactListCrawlCycle("success")
	case found+missing > 0:
		metrics.IncContactListCrawlCycle("partial")
	default:
		metrics.IncContactListCrawlCycle("error")
	}
	log.Info(
		"contact_list_crawl_cycle",
		"seeded", seeded,
		"claimed", len(claimed),
		"found", found,
		"missing", missing,
		"failures", failures,
		"enqueued", enqueued,
	)
}

func publishContactListCrawlDepth(ctx context.Context, log *slog.Logger, st contactListCrawlStore, maxHops int) {
	depths, err := st.ContactListCrawlDueByHop(ctx)
	if err != nil {
		log.Warn("contact_list_crawl_depth_failed", "error", err)
		return
	}
	if depths == nil {
		depths = map[int]int{}
	}
	if maxHops < 0 {
		maxHops = 0
	}
	for hop := 0; hop <= maxHops; hop++ {
		metrics.SetContactListCrawlQueueDepth(strconv.Itoa(hop), float64(depths[hop]))
	}
}

func crawlOneContactList(
	ctx context.Context,
	log *slog.Logger,
	st contactListCrawlStore,
	cfg config.IngestorContactCrawlConfig,
	entry store.ContactListCrawlEntry,
	fallbackRelays []string,
	allowlist []string,
	disabledRelays []string,
	requireTLS bool,
	fetcher backfill.PageFetcher,
	onMessage backfill.MessageHandler,
) (int, string) {
	raw, err := st.LatestKind10002Raw(ctx, entry.Pubkey)
	if err != nil {
		log.Warn("contact_list_crawl_relay_list_failed", "pubkey", entry.Pubkey, "error", err)
		raw = nil
	}
	relays := selectContactCrawlRelays(
		writeRelaysFromKind10002(raw),
		fallbackRelays,
		allowlist,
		disabledRelays,
		requireTLS,
		cfg.MaxRelaysPerPubkey,
	)
	found, contacts, err := fetchContactList(ctx, fetcher, onMessage, relays, entry.Pubkey)
	if err != nil {
		if markErr := st.MarkContactListCrawlFailed(ctx, entry.Pubkey, cfg.RetryDelay, err); markErr != nil {
			log.Warn("contact_list_crawl_mark_failed", "pubkey", entry.Pubkey, "error", markErr)
		}
		return 0, "error"
	}
	enqueued := 0
	if found && entry.Hop < cfg.MaxHops && len(contacts) > 0 {
		n, enqErr := st.EnqueueContactListCrawl(ctx, contacts, entry.Hop+1, cfg.MaxPending)
		if enqErr != nil {
			if markErr := st.MarkContactListCrawlFailed(ctx, entry.Pubkey, cfg.RetryDelay, enqErr); markErr != nil {
				log.Warn("contact_list_crawl_mark_failed", "pubkey", entry.Pubkey, "error", markErr)
			}
			return 0, "error"
		}
		enqueued = n
	}
	if markErr := st.MarkContactListCrawlFetched(ctx, entry.Pubkey, found, cfg.RefetchInterval); markErr != nil {
		log.Warn("contact_list_crawl_mark_fetched_failed", "pubkey", entry.Pubkey, "error", markErr)
	}
	if found {
		return enqueued, "found"
	}
	return 0, "missing"
}

// fetchContactList requests the current kind-3 and kind-10002 for one pubkey.
// There is no since filter: relays return the latest replaceable list however
// old its created_at is. The first relay that returns a kind 3 stops the walk.
func fetchContactList(
	ctx context.Context,
	fetcher backfill.PageFetcher,
	onMessage backfill.MessageHandler,
	relays []string,
	pubkey string,
) (found bool, contacts []string, err error) {
	if fetcher == nil || onMessage == nil {
		return false, nil, fmt.Errorf("contact list fetch is not configured")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return false, nil, fmt.Errorf("pubkey is required")
	}
	if len(relays) == 0 {
		return false, nil, fmt.Errorf("no relays configured for contact list crawl")
	}

	var lastErr error
	// A miss is confirmed only when every relay ends with EOSE and no kind 3.
	// One empty relay must not hide a list that a failed relay might hold.
	confirmedMiss := true
	for _, relayURL := range relays {
		if err := ctx.Err(); err != nil {
			return false, nil, err
		}
		page, err := fetcher.FetchPage(ctx, relayURL, backfill.PageRequest{
			Kinds:   []int{3, 10002},
			Authors: []string{pubkey},
			Limit:   contactListPageLimit,
		})
		if err != nil {
			lastErr = err
			confirmedMiss = false
			continue
		}
		var best *parsedCrawlEvent
		for _, payload := range page.Events {
			if err := onMessage(ctx, relayURL, payload); err != nil {
				return false, nil, err
			}
			parsed, ok := parseCrawlEvent(payload, pubkey)
			if !ok || !parsed.IsContact {
				continue
			}
			if best == nil || parsed.CreatedAt >= best.CreatedAt {
				copy := parsed
				best = &copy
			}
		}
		if best != nil {
			return true, best.Contacts, nil
		}
		if !page.EOSESeen {
			confirmedMiss = false
		}
	}
	if confirmedMiss {
		return false, nil, nil
	}
	if lastErr != nil {
		return false, nil, lastErr
	}
	return false, nil, fmt.Errorf("no relay confirmed a missing contact list for %s", pubkey)
}

type parsedCrawlEvent struct {
	CreatedAt int64
	Contacts  []string
	IsContact bool
}

func parseCrawlEvent(payload []byte, author string) (parsedCrawlEvent, bool) {
	var event struct {
		Kind      int        `json:"kind"`
		CreatedAt int64      `json:"created_at"`
		Tags      [][]string `json:"tags"`
	}
	if err := json.Unmarshal(payload, &event); err != nil {
		return parsedCrawlEvent{}, false
	}
	if event.Kind != 3 {
		return parsedCrawlEvent{CreatedAt: event.CreatedAt}, true
	}
	seen := make(map[string]struct{})
	contacts := make([]string, 0)
	for _, tag := range event.Tags {
		if len(tag) < 2 || tag[0] != "p" {
			continue
		}
		pubkey := strings.TrimSpace(tag[1])
		if pubkey == author || !isLowerHex64(pubkey) {
			continue
		}
		if _, ok := seen[pubkey]; ok {
			continue
		}
		seen[pubkey] = struct{}{}
		contacts = append(contacts, pubkey)
	}
	return parsedCrawlEvent{CreatedAt: event.CreatedAt, Contacts: contacts, IsContact: true}, true
}

func isLowerHex64(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		isDigit := c >= '0' && c <= '9'
		isLower := c >= 'a' && c <= 'f'
		if !isDigit && !isLower {
			return false
		}
	}
	return true
}
