#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 1 ]]; then
	echo "usage: $0 <scenario>" >&2
	echo "scenarios: api-read-pressure worker-throughput-pressure ingest-throughput-pressure replay-rebuild-pressure ws-api-pressure" >&2
	exit 1
fi

scenario="$1"
shift || true

case "${scenario}" in
api-read-pressure)
	bash ./loadtest/scenarios/api_read_pressure.sh "$@"
	;;
worker-throughput-pressure)
	bash ./loadtest/scenarios/worker_throughput_pressure.sh "$@"
	;;
ingest-throughput-pressure)
	bash ./loadtest/scenarios/ingest_throughput_pressure.sh "$@"
	;;
replay-rebuild-pressure)
	bash ./loadtest/scenarios/replay_rebuild_pressure.sh "$@"
	;;
ws-api-pressure)
	bash ./loadtest/scenarios/ws_api_pressure.sh "$@"
	;;
*)
	echo "unknown scenario: ${scenario}" >&2
	exit 1
	;;
esac
