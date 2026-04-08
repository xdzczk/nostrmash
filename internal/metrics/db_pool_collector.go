package metrics

import (
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	dbPoolStatsMu        sync.Mutex
	dbPoolStatsCollector prometheus.Collector
)

func RegisterDBPool(pool *pgxpool.Pool) {
	if pool == nil {
		return
	}
	ensureRegistered()

	dbPoolStatsMu.Lock()
	defer dbPoolStatsMu.Unlock()
	if dbPoolStatsCollector != nil {
		return
	}

	dbPoolStatsCollector = newDBPoolCollector(realDBPoolProvider{pool: pool})
	registry.MustRegister(dbPoolStatsCollector)
}

type dbPoolCollector struct {
	pool dbPoolStatsProvider

	openConnections         *prometheus.Desc
	inUseConnections        *prometheus.Desc
	idleConnections         *prometheus.Desc
	maxOpenConnections      *prometheus.Desc
	maxOpenUsageRatio       *prometheus.Desc
	acquireCountTotal       *prometheus.Desc
	acquireDurationSeconds  *prometheus.Desc
	waitCountTotal          *prometheus.Desc
	canceledAcquireTotal    *prometheus.Desc
	constructingConnections *prometheus.Desc
}

func newDBPoolCollector(pool dbPoolStatsProvider) *dbPoolCollector {
	return &dbPoolCollector{
		pool:                    pool,
		openConnections:         prometheus.NewDesc("nostrmash_db_pool_open_connections", "Current number of open DB pool connections.", nil, nil),
		inUseConnections:        prometheus.NewDesc("nostrmash_db_pool_in_use_connections", "Current number of DB pool connections in use.", nil, nil),
		idleConnections:         prometheus.NewDesc("nostrmash_db_pool_idle_connections", "Current number of idle DB pool connections.", nil, nil),
		maxOpenConnections:      prometheus.NewDesc("nostrmash_db_pool_max_open_connections", "Configured maximum number of DB pool connections.", nil, nil),
		maxOpenUsageRatio:       prometheus.NewDesc("nostrmash_db_pool_max_open_usage_ratio", "Current in-use to max-open DB pool connection ratio.", nil, nil),
		acquireCountTotal:       prometheus.NewDesc("nostrmash_db_pool_acquire_count_total", "Total DB pool acquires.", nil, nil),
		acquireDurationSeconds:  prometheus.NewDesc("nostrmash_db_pool_acquire_duration_seconds_total", "Total seconds spent acquiring DB pool connections.", nil, nil),
		waitCountTotal:          prometheus.NewDesc("nostrmash_db_pool_wait_count_total", "Total DB pool acquires that waited due to saturation.", nil, nil),
		canceledAcquireTotal:    prometheus.NewDesc("nostrmash_db_pool_canceled_acquire_count_total", "Total canceled DB pool acquires.", nil, nil),
		constructingConnections: prometheus.NewDesc("nostrmash_db_pool_constructing_connections", "Current number of DB pool connections being constructed.", nil, nil),
	}
}

func (c *dbPoolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.openConnections
	ch <- c.inUseConnections
	ch <- c.idleConnections
	ch <- c.maxOpenConnections
	ch <- c.maxOpenUsageRatio
	ch <- c.acquireCountTotal
	ch <- c.acquireDurationSeconds
	ch <- c.waitCountTotal
	ch <- c.canceledAcquireTotal
	ch <- c.constructingConnections
}

type dbPoolStatsProvider interface {
	Stat() dbPoolStat
}

type dbPoolStat interface {
	TotalConns() int32
	AcquiredConns() int32
	IdleConns() int32
	MaxConns() int32
	AcquireCount() int64
	AcquireDuration() time.Duration
	EmptyAcquireCount() int64
	CanceledAcquireCount() int64
	ConstructingConns() int32
}

type realDBPoolProvider struct {
	pool *pgxpool.Pool
}

func (p realDBPoolProvider) Stat() dbPoolStat {
	return realDBPoolStat{stat: p.pool.Stat()}
}

type realDBPoolStat struct {
	stat *pgxpool.Stat
}

func (s realDBPoolStat) TotalConns() int32 {
	return s.stat.TotalConns()
}

func (s realDBPoolStat) AcquiredConns() int32 {
	return s.stat.AcquiredConns()
}

func (s realDBPoolStat) IdleConns() int32 {
	return s.stat.IdleConns()
}

func (s realDBPoolStat) MaxConns() int32 {
	return s.stat.MaxConns()
}

func (s realDBPoolStat) AcquireCount() int64 {
	return s.stat.AcquireCount()
}

func (s realDBPoolStat) AcquireDuration() time.Duration {
	return s.stat.AcquireDuration()
}

func (s realDBPoolStat) EmptyAcquireCount() int64 {
	return s.stat.EmptyAcquireCount()
}

func (s realDBPoolStat) CanceledAcquireCount() int64 {
	return s.stat.CanceledAcquireCount()
}

func (s realDBPoolStat) ConstructingConns() int32 {
	return s.stat.ConstructingConns()
}

func (c *dbPoolCollector) Collect(ch chan<- prometheus.Metric) {
	stats := c.pool.Stat()
	open := float64(stats.TotalConns())
	inUse := float64(stats.AcquiredConns())
	idle := float64(stats.IdleConns())
	maxOpen := float64(stats.MaxConns())

	waitCount := stats.EmptyAcquireCount()
	acquireCount := stats.AcquireCount()
	acquireDurationSeconds := stats.AcquireDuration().Seconds()

	ratio := 0.0
	if maxOpen > 0 {
		ratio = inUse / maxOpen
	}

	ch <- prometheus.MustNewConstMetric(c.openConnections, prometheus.GaugeValue, open)
	ch <- prometheus.MustNewConstMetric(c.inUseConnections, prometheus.GaugeValue, inUse)
	ch <- prometheus.MustNewConstMetric(c.idleConnections, prometheus.GaugeValue, idle)
	ch <- prometheus.MustNewConstMetric(c.maxOpenConnections, prometheus.GaugeValue, maxOpen)
	ch <- prometheus.MustNewConstMetric(c.maxOpenUsageRatio, prometheus.GaugeValue, ratio)
	ch <- prometheus.MustNewConstMetric(c.acquireCountTotal, prometheus.CounterValue, float64(acquireCount))
	ch <- prometheus.MustNewConstMetric(c.acquireDurationSeconds, prometheus.CounterValue, acquireDurationSeconds)
	ch <- prometheus.MustNewConstMetric(c.waitCountTotal, prometheus.CounterValue, float64(waitCount))
	ch <- prometheus.MustNewConstMetric(c.canceledAcquireTotal, prometheus.CounterValue, float64(stats.CanceledAcquireCount()))
	ch <- prometheus.MustNewConstMetric(c.constructingConnections, prometheus.GaugeValue, float64(stats.ConstructingConns()))
}
