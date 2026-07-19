package store

import "github.com/xdzczk/nostrmash/internal/store/account"

// AccountStateRow and AccountSignalRow now live in the account bounded-context
// package. They are re-exported here so existing callers that reference
// store.AccountStateRow keep compiling; the account-state methods themselves
// are promoted onto PostgresStore via the embedded *account.Store.
type AccountStateRow = account.AccountStateRow

// AccountSignalRow re-exports the account-signal projection.
type AccountSignalRow = account.AccountSignalRow
