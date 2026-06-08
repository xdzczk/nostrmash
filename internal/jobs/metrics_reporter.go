package jobs

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xdzczk/nostrmash/internal/metrics"
)

// knownJobTypes is the fixed enum used to bound the cardinality of the
// nostrmash_jobs_rows{job_type=...} label. Anything not in this set is
// reported under job_type="other" so a buggy/legacy producer cannot blow up
// metric cardinality.
var knownJobTypes = map[string]struct{}{
	JobTypeDeriveEventBundle:        {},
	JobTypeDeriveEventRelationships: {},
	JobTypeUpdateReplaceableState:   {},
	JobTypeProjectProfilesLatest:    {},
	JobTypeProjectAuthorRecentEvent: {},
	JobTypeProjectReplyCounts:       {},
	JobTypeProjectReactionCounts:    {},
	JobTypeProjectRepostCounts:      {},
	JobTypeProjectReactionEvents:    {},
	JobTypeProjectRepostEvents:      {},
	JobTypeProjectDeletionEvents:    {},
	JobTypeProjectContactLists:      {},
	JobTypeProjectRelayLists:        {},
	JobTypeProjectDMUnreadCounts:    {},
	JobTypeProjectZapReceipts:       {},
	JobTypeUpdateThreadProjection:   {},
	JobTypeRepairUnresolvedRefs:     {},
	JobTypeRebuildProjectionScope:   {},

	JobTypeTrustSyncGraphRedis:     {},
	JobTypeTrustComputeGlobalScore: {},
	JobTypeTrustPromoteRun:         {},

	JobTypeHydrateAccount: {},
}

// metricsReporterLogger is the logger surface used by the row-count reporter.
// Defined locally so trust_worker / worker can reuse RetentionLogger and we do
// not need a separate interface declaration in the package surface.
type metricsReporterLogger = RetentionLogger

// RunRowCountMetricsReporter periodically publishes the jobs queue row count
// (broken down by status and job_type) and the age of the oldest terminal
// (succeeded/dead) row. It is safe to launch from multiple workers — both
// workers run the same query, the gauge value is overwritten each tick.
//
// Cardinality is bounded by the fixed knownJobTypes enum; any unrecognized
// job_type is folded into "other".
func RunRowCountMetricsReporter(ctx context.Context, log metricsReporterLogger, pool *pgxpool.Pool, every time.Duration) {
	if pool == nil || every <= 0 {
		return
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			collectJobsRowCounts(ctx, log, pool)
			collectJobsOldestFinishedAge(ctx, log, pool)
		}
	}
}

func collectJobsRowCounts(ctx context.Context, log metricsReporterLogger, pool *pgxpool.Pool) {
	rows, err := pool.Query(ctx, `
		SELECT status, job_type, COUNT(*)
		FROM jobs
		GROUP BY status, job_type
	`)
	if err != nil {
		log.Error("jobs_rows_metrics_query_failed", "error", err)
		return
	}
	defer rows.Close()

	// Reset prior label values so a status/job_type combination that drains
	// to zero stops appearing at its last reported value forever.
	metrics.ResetJobsRows()

	type bucket struct {
		status  string
		jobType string
	}
	folded := make(map[bucket]float64, 16)
	for rows.Next() {
		var status, jobType string
		var count int64
		if scanErr := rows.Scan(&status, &jobType, &count); scanErr != nil {
			log.Error("jobs_rows_metrics_scan_failed", "error", scanErr)
			return
		}
		if _, ok := knownJobTypes[jobType]; !ok {
			jobType = "other"
		}
		folded[bucket{status: status, jobType: jobType}] += float64(count)
	}
	if err := rows.Err(); err != nil {
		log.Error("jobs_rows_metrics_iter_failed", "error", err)
		return
	}
	for b, count := range folded {
		metrics.SetJobsRows(b.status, b.jobType, count)
	}
}

func collectJobsOldestFinishedAge(ctx context.Context, log metricsReporterLogger, pool *pgxpool.Pool) {
	for _, status := range []string{StatusSucceeded, StatusDead} {
		var oldest *float64
		err := pool.QueryRow(ctx, `
			SELECT EXTRACT(EPOCH FROM (now() - MIN(finished_at)))
			FROM jobs
			WHERE status = $1
			  AND finished_at IS NOT NULL
		`, status).Scan(&oldest)
		if err != nil {
			log.Error("jobs_oldest_finished_age_query_failed", "status", status, "error", err)
			continue
		}
		if oldest == nil {
			metrics.SetJobsOldestFinishedAgeSeconds(status, 0)
			continue
		}
		metrics.SetJobsOldestFinishedAgeSeconds(status, *oldest)
	}
}
