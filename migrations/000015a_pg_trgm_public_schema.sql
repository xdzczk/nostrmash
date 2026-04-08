-- Keep pg_trgm objects in public so schema-scoped test/search_path pools
-- consistently resolve gin_trgm_ops across independent schemas.
CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;
ALTER EXTENSION pg_trgm SET SCHEMA public;
