// Package discoveryctx carries request-scoped discovery filters from the
// query service into store SQL without adding a parameter to every reader.
package discoveryctx

import "context"

type excludeBridgedKey struct{}

// WithExcludeBridged marks ctx so event rankings drop bridged events.
// A false flag returns ctx unchanged.
func WithExcludeBridged(ctx context.Context, exclude bool) context.Context {
	if ctx == nil || !exclude {
		return ctx
	}
	return context.WithValue(ctx, excludeBridgedKey{}, true)
}

// ExcludeBridged reports whether rankings should drop bridged events.
func ExcludeBridged(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(excludeBridgedKey{}).(bool)
	return v
}
