package api

import (
	"context"
	"runtime"
	"time"
)

type adminSystemResponse struct {
	ServiceName string              `json:"service_name"`
	Environment string              `json:"environment"`
	AppVersion  string              `json:"app_version"`
	NowUTC      time.Time           `json:"now_utc"`
	UptimeS     int64               `json:"uptime_s"`
	Runtime     adminRuntimeDetails `json:"runtime"`
	Database    adminDatabaseStatus `json:"database"`
}

type adminRuntimeDetails struct {
	GoVersion    string `json:"go_version"`
	NumGoroutine int    `json:"num_goroutine"`
}

type adminDatabaseStatus struct {
	Reachable     bool  `json:"reachable"`
	PingMS        int64 `json:"ping_ms"`
	MaxConns      int32 `json:"max_conns"`
	TotalConns    int32 `json:"total_conns"`
	IdleConns     int32 `json:"idle_conns"`
	AcquiredConns int32 `json:"acquired_conns"`
}

func (s *adminService) GetSystem(ctx context.Context) (adminSystemResponse, error) {
	now := time.Now().UTC()
	resp := adminSystemResponse{
		ServiceName: s.serviceName,
		Environment: s.environment,
		AppVersion:  s.appVersion,
		NowUTC:      now,
		UptimeS:     int64(now.Sub(s.startedAt).Seconds()),
		Runtime: adminRuntimeDetails{
			GoVersion:    runtime.Version(),
			NumGoroutine: runtime.NumGoroutine(),
		},
	}

	stats := s.pool.Stat()
	resp.Database.MaxConns = stats.MaxConns()
	resp.Database.TotalConns = stats.TotalConns()
	resp.Database.IdleConns = stats.IdleConns()
	resp.Database.AcquiredConns = stats.AcquiredConns()

	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	pingStart := time.Now()
	err := s.pool.Ping(pingCtx)
	resp.Database.PingMS = time.Since(pingStart).Milliseconds()
	resp.Database.Reachable = err == nil
	return resp, nil
}
