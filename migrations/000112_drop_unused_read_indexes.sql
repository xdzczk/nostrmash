-- Indexes with zero scans since the 2026-10-21 stats reset. Each one is
-- rewritten on every insert into its table and none of the current read
-- queries can use it:
--
-- idx_thread_summaries_last_activity: hot-conversation reads filter on
--   COALESCE(last_activity_at, events.created_at), which cannot use this index.
--   Velocity ordering moved to the reply_weight indexes in 000080/000103.
-- idx_thread_summaries_hot_24h / hot_7d: same queries now order by
--   reply_weight_*, not raw replies_24h / replies_7d.
-- idx_event_urls_domain_created_at: reads filter canonical_domain
--   (idx_event_urls_canonical_domain_created_at, 000060).
-- idx_contact_lists_latest_created: contact lists are loaded by pubkey,
--   which is the primary key.
--
-- idx_event_relays_pubkey_relay_event stays. It has almost no traffic, but
-- it is the only index that keeps the author relay-analytics endpoint off a
-- sequential scan of event_relays.
DROP INDEX IF EXISTS idx_thread_summaries_last_activity;
DROP INDEX IF EXISTS idx_thread_summaries_hot_24h;
DROP INDEX IF EXISTS idx_thread_summaries_hot_7d;
DROP INDEX IF EXISTS idx_event_urls_domain_created_at;
DROP INDEX IF EXISTS idx_contact_lists_latest_created;
