package config

// ConfigEnvDocs is the single source of truth for config documentation.
func ConfigEnvDocs() []EnvVarDoc {
	docs := make([]EnvVarDoc, 0, 64)
	docs = append(docs, configEnvDocsShared()...)
	docs = append(docs, configEnvDocsAPI()...)
	docs = append(docs, configEnvDocsIngestor()...)
	docs = append(docs, configEnvDocsWorker()...)
	docs = append(docs, configEnvDocsTrustWorker()...)
	return docs
}
