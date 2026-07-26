package derivation

import "github.com/xdzczk/nostrmash/internal/nostr"

// maxSaneUnixCreatedAt mirrors nostr.MaxUnixCreatedAt for SQL predicates that
// must skip corrupt created_at values before calling to_timestamp().
const maxSaneUnixCreatedAt = nostr.MaxUnixCreatedAt
