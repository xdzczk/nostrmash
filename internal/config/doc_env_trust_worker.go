package config

func configEnvDocsTrustWorker() []EnvVarDoc {
	return []EnvVarDoc{
		{
			Name:         "TRUST_ENABLE_REDIS_SYNC",
			Runtimes:     []string{"trust_worker"},
			Required:     false,
			DefaultValue: "false",
			Description:  "Enable Redis graph synchronization trust job phases.",
		},
		{
			Name:         "TRUST_ENABLE_SCORE_COMPUTE",
			Runtimes:     []string{"trust_worker"},
			Required:     false,
			DefaultValue: "true",
			Description:  "Enable trust score computation trust job phases.",
		},
		{
			Name:         "TRUST_ENABLE_NEIGHBORHOODS",
			Runtimes:     []string{"trust_worker"},
			Required:     false,
			DefaultValue: "false",
			Description:  "Enable seeded trust neighborhood BFS between global score compute and promote.",
		},
		{
			Name:         "TRUST_NEIGHBORHOOD_MAX_MEMBERS",
			Runtimes:     []string{"trust_worker"},
			Required:     false,
			DefaultValue: "5000",
			Description:  "Maximum members retained per seed neighborhood, including the seed itself.",
		},
		{
			Name:         "TRUST_ENABLE_INTERACTION_GRAPH",
			Runtimes:     []string{"trust_worker"},
			Required:     false,
			DefaultValue: "false",
			Description:  "When true, refresh engagement-derived interaction edge weights and merge them into trust ranking adjacency. Keep false until reviewing the admin comparison report.",
		},
		{
			Name:         "TRUST_GRAPH_SNAPSHOT_REFRESH_INTERVAL",
			Runtimes:     []string{"trust_worker"},
			Required:     false,
			DefaultValue: "10m",
			Description:  "Interval between trust_graph_snapshot rebuilds (seeds + follower edges) that the ingest gate reads.",
		},
		{
			Name:         "TRUST_RUN_INTERVAL",
			Runtimes:     []string{"trust_worker"},
			Required:     false,
			DefaultValue: "1h",
			Description:  "Interval at which the trust worker schedules a global trust run when score compute is enabled and no run is active.",
		},
		{
			Name:         "TRUST_REDIS_URL",
			Runtimes:     []string{"trust_worker"},
			Required:     false,
			DefaultValue: "",
			Description:  "Redis connection string used for trust graph working state; required when TRUST_ENABLE_REDIS_SYNC=true.",
		},
		{
			Name:         "TRUST_REDIS_KEY_PREFIX",
			Runtimes:     []string{"trust_worker"},
			Required:     false,
			DefaultValue: "nostrmash",
			Description:  "Prefix namespace for trust-worker Redis graph/snapshot keys.",
		},
		{
			Name:         "TRUST_WORKER_CLAIM_BATCH_SIZE",
			Runtimes:     []string{"trust_worker"},
			Required:     false,
			DefaultValue: "5",
			Description:  "Maximum trust jobs claimed per poll loop.",
		},
		{
			Name:         "TRUST_WORKER_CONCURRENCY",
			Runtimes:     []string{"trust_worker"},
			Required:     false,
			DefaultValue: "2",
			Description:  "Trust worker goroutine concurrency.",
		},
		{
			Name:         "TRUST_WORKER_POLL_INTERVAL",
			Runtimes:     []string{"trust_worker"},
			Required:     false,
			DefaultValue: "1s",
			Description:  "Polling interval for trust queue claims.",
		},
		{
			Name:         "TRUST_WORKER_RETRY_DELAY",
			Runtimes:     []string{"trust_worker"},
			Required:     false,
			DefaultValue: "5s",
			Description:  "Retry delay when trust jobs fail.",
		},
	}
}
