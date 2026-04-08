package config

func configEnvDocsShared() []EnvVarDoc {
	return []EnvVarDoc{
		{
			Name:         "DATABASE_URL",
			Runtimes:     []string{"api", "ingestor", "trust_worker", "worker"},
			Required:     true,
			DefaultValue: "",
			Description:  "PostgreSQL connection string.",
		},
		{
			Name:         "DEBUG_ADDR",
			Runtimes:     []string{"api", "trust_worker", "worker"},
			Required:     false,
			DefaultValue: "",
			Description:  "Optional debug/pprof listen address. Leave empty by default; prefer localhost binding.",
		},
		{
			Name:         "ENVIRONMENT",
			Runtimes:     []string{"api", "ingestor", "trust_worker", "worker"},
			Required:     false,
			DefaultValue: "development",
			Description:  "Deployment environment label.",
		},
		{
			Name:         "METRICS_ADDR",
			Runtimes:     []string{"ingestor", "trust_worker", "worker"},
			Required:     false,
			DefaultValue: ":9090",
			Description:  "Prometheus metrics listen address for ingestor, worker, and trust_worker. API exposes /metrics on HTTP_ADDR.",
		},
	}
}
