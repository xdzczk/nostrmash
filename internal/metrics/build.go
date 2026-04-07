package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	buildInfo      *prometheus.GaugeVec
	deploymentInfo *prometheus.GaugeVec
)

func registerBuildMetrics() {
	buildInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nostrmash_build_info",
			Help: "Build metadata for the running binary.",
		},
		[]string{"binary_role", "version", "commit", "build_time"},
	)
	deploymentInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nostrmash_deployment_info",
			Help: "Deployment identity metadata for the running binary.",
		},
		[]string{"binary_role", "service_name", "environment"},
	)

	registry.MustRegister(buildInfo, deploymentInfo)
}

func RegisterBuildInfo(binaryRole, version, commit, buildTime string) {
	ensureRegistered()
	buildInfo.WithLabelValues(binaryRole, version, commit, buildTime).Set(1)
}

func RegisterDeploymentInfo(binaryRole, serviceName, environment string) {
	ensureRegistered()
	deploymentInfo.WithLabelValues(binaryRole, serviceName, environment).Set(1)
}
