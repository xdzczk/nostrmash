package derivation

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// RowQuerier is the subset of a pool or transaction used to read one median.
type RowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// SameWeekdayMedian is the median rolling 24-hour total for this UTC weekday
// over the last eight weeks, excluding the current hour. samples is the
// number of hourly rows in that set. column must be a stats_snapshot_history
// metric column; anything else is rejected.
func SameWeekdayMedian(ctx context.Context, q RowQuerier, column string) (typical float64, samples int, err error) {
	switch column {
	case "note_volume", "active_authors":
	default:
		return 0, 0, fmt.Errorf("unsupported weekday median column %q", column)
	}
	query := fmt.Sprintf(`
		SELECT COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY %s), 0),
		       count(*)::int
		FROM stats_snapshot_history
		WHERE bucket_start >= now() - interval '56 days'
		  AND bucket_start < (date_trunc('hour', now() AT TIME ZONE 'utc') AT TIME ZONE 'utc')
		  AND EXTRACT(DOW FROM (bucket_start AT TIME ZONE 'utc'))
		      = EXTRACT(DOW FROM (now() AT TIME ZONE 'utc'))
	`, column)
	if err := q.QueryRow(ctx, query).Scan(&typical, &samples); err != nil {
		if isUndefinedTable(err) {
			return 0, 0, nil
		}
		return 0, 0, fmt.Errorf("weekday median %s: %w", column, err)
	}
	return typical, samples, nil
}
