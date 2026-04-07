package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestHandlerIncludesGoRuntimeMetrics(t *testing.T) {
	ObserveAPI("GET", "/health", 200, 5*time.Millisecond)
	ObserveDBOperation("get_event_raw_by_id", "ok", 2*time.Millisecond)
	ObserveQueueOperation("enqueue", "error", 3*time.Millisecond)
	ObserveWorkerJobExecution("derive_event_bundle", "succeeded", 10*time.Millisecond)
	SetIngestCheckpointFreshness("live", "default_v1", 12)
	SetWorkerQueueBacklogOldestPendingAge(34)
	SetRebuildRunsActive(1)
	SetRebuildActiveOldestAge(56)
	AddStaleRecoveryRecovered("default", 1)
	AddStaleRecoveryDeadLettered("default", 1)
	ObserveStaleRecoveryDuration("default", "ok", 3*time.Millisecond)
	RegisterBuildInfo("api", "v1.2.3", "abc1234", "2026-04-06T00:00:00Z")
	RegisterDeploymentInfo("api", "nostrmash", "development")

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, "go_goroutines") {
		t.Fatalf("expected go runtime metrics in /metrics output")
	}
	if !strings.Contains(body, "process_cpu_seconds_total") {
		t.Fatalf("expected process metrics in /metrics output")
	}
	if !strings.Contains(body, "nostrmash_api_requests_total") {
		t.Fatalf("expected nostrmash app metrics in /metrics output")
	}
	if !strings.Contains(body, "nostrmash_db_operation_duration_seconds") {
		t.Fatalf("expected db operation metrics in /metrics output")
	}
	if !strings.Contains(body, "nostrmash_queue_operation_errors_total") {
		t.Fatalf("expected queue operation metrics in /metrics output")
	}
	if !strings.Contains(body, "nostrmash_worker_job_execution_duration_seconds") {
		t.Fatalf("expected worker job execution metrics in /metrics output")
	}
	if !strings.Contains(body, "nostrmash_ingest_checkpoint_freshness_seconds") {
		t.Fatalf("expected checkpoint freshness metrics in /metrics output")
	}
	if !strings.Contains(body, "nostrmash_worker_queue_backlog_oldest_pending_age_seconds") {
		t.Fatalf("expected queue backlog age metrics in /metrics output")
	}
	if !strings.Contains(body, "nostrmash_rebuild_active_oldest_age_seconds") {
		t.Fatalf("expected rebuild active age metrics in /metrics output")
	}
	if !strings.Contains(body, "nostrmash_build_info") {
		t.Fatalf("expected build info metrics in /metrics output")
	}
	if !strings.Contains(body, "nostrmash_deployment_info") {
		t.Fatalf("expected deployment info metrics in /metrics output")
	}
	if !strings.Contains(body, "nostrmash_worker_stale_recovery_recovered_total") {
		t.Fatalf("expected stale recovery recovered metrics in /metrics output")
	}
	if !strings.Contains(body, "nostrmash_worker_stale_recovery_dead_lettered_total") {
		t.Fatalf("expected stale recovery dead-lettered metrics in /metrics output")
	}
	if !strings.Contains(body, "nostrmash_worker_stale_recovery_duration_seconds") {
		t.Fatalf("expected stale recovery duration metrics in /metrics output")
	}
}

func TestDBPoolCollectorExportsSaturationSignals(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(newDBPoolCollector(fakeDBPoolProvider{
		stat: fakeDBPoolStat{
			totalConns:            8,
			acquiredConns:         6,
			idleConns:             2,
			maxConns:              10,
			acquireCount:          123,
			acquireDuration:       15 * time.Second,
			emptyAcquireCount:     7,
			canceledAcquireCount:  2,
			constructingConnCount: 1,
		},
	}))

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	got := map[string]bool{}
	for _, family := range families {
		got[family.GetName()] = true
	}

	wantNames := []string{
		"nostrmash_db_pool_open_connections",
		"nostrmash_db_pool_in_use_connections",
		"nostrmash_db_pool_idle_connections",
		"nostrmash_db_pool_max_open_connections",
		"nostrmash_db_pool_max_open_usage_ratio",
		"nostrmash_db_pool_acquire_count_total",
		"nostrmash_db_pool_acquire_duration_seconds_total",
		"nostrmash_db_pool_wait_count_total",
		"nostrmash_db_pool_canceled_acquire_count_total",
		"nostrmash_db_pool_constructing_connections",
	}
	for _, name := range wantNames {
		if !got[name] {
			t.Fatalf("expected metric %q to be exported", name)
		}
	}
}

type fakeDBPoolProvider struct {
	stat fakeDBPoolStat
}

func (p fakeDBPoolProvider) Stat() dbPoolStat {
	return p.stat
}

type fakeDBPoolStat struct {
	totalConns            int32
	acquiredConns         int32
	idleConns             int32
	maxConns              int32
	acquireCount          int64
	acquireDuration       time.Duration
	emptyAcquireCount     int64
	canceledAcquireCount  int64
	constructingConnCount int32
}

func (s fakeDBPoolStat) TotalConns() int32 {
	return s.totalConns
}

func (s fakeDBPoolStat) AcquiredConns() int32 {
	return s.acquiredConns
}

func (s fakeDBPoolStat) IdleConns() int32 {
	return s.idleConns
}

func (s fakeDBPoolStat) MaxConns() int32 {
	return s.maxConns
}

func (s fakeDBPoolStat) AcquireCount() int64 {
	return s.acquireCount
}

func (s fakeDBPoolStat) AcquireDuration() time.Duration {
	return s.acquireDuration
}

func (s fakeDBPoolStat) EmptyAcquireCount() int64 {
	return s.emptyAcquireCount
}

func (s fakeDBPoolStat) CanceledAcquireCount() int64 {
	return s.canceledAcquireCount
}

func (s fakeDBPoolStat) ConstructingConns() int32 {
	return s.constructingConnCount
}
