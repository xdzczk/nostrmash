package derivation

// authorAnalyticsDirtyKind reports whether this event can change windowed
// author analytics. The sweeper rebuild reads kind-1 posts plus the
// reaction, repost, and zap tables those kinds fill.
func authorAnalyticsDirtyKind(kind int) bool {
	switch kind {
	case 1, 6, 7, 9735:
		return true
	default:
		return false
	}
}

// profileStatsDirtyKind reports whether this event can change the counters
// the profile-stats sweeper recomputes. Kind 3 is handled separately by
// ProjectContactListsLatest and is intentionally absent here.
func profileStatsDirtyKind(kind int) bool {
	switch kind {
	case 1, 6, 7, 20, 21, 22, 9735, 34235, 34236:
		return true
	default:
		return false
	}
}
