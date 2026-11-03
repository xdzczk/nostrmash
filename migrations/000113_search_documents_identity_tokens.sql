-- identity_tokens && pubkey[] cannot use the (entity_type, entity_id)
-- primary key. The Meilisearch batch loader's identity branch was a
-- sequential scan of search_documents (~6s, multi-GB read) on every batch.
-- Only identity rows carry tokens worth indexing.
SET LOCAL statement_timeout = '20min';

CREATE INDEX IF NOT EXISTS idx_search_documents_identity_tokens
    ON search_documents USING gin (identity_tokens)
    WHERE entity_type = 'identity';
