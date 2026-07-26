package nostr

// MaxUnixCreatedAt is 2100-01-01T00:00:00Z. Nostr event created_at is unix
// seconds; values above this are corrupt (milliseconds, nanoseconds, or
// garbage) and must not be ingested or passed to Postgres to_timestamp(),
// which errors with SQLSTATE 22008.
const MaxUnixCreatedAt int64 = 4102444800
