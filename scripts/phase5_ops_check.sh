#!/usr/bin/env bash
# Phase 5 single-server ops pulse: disk, Meili size, memory caps, DLQ, search.
# Run on the Coolify host (or via SSH). Non-zero exit if a hard budget is broken.
set -euo pipefail

APP_UUID="${APP_UUID:-qnjot8wvldaof8t1j6b22qss}"
MEILI_VOL="${MEILI_VOL:-/var/lib/docker/volumes/${APP_UUID}_meili-data-v2}"
FAIL=0

echo "=== host ==="
df -h / | awk 'NR==2{print "rootfs", $3"/"$2, $5}'
if [[ -d "$MEILI_VOL" ]]; then
  MEILI_SIZE=$(du -sm "$MEILI_VOL" | awk '{print $1}')
  echo "meili_disk_mb=$MEILI_SIZE"
  if (( MEILI_SIZE > 12288 )); then
    echo "WARN: Meili disk > 12 GiB (target < 10 GiB steady-state)"
    FAIL=1
  fi
fi

echo
echo "=== memory caps ==="
docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' \
  | grep -E 'api-|worker-|ingestor-|trust_worker-|meilisearch-|p8ucu' || true

echo
echo "=== containers ==="
docker ps --format '{{.Names}} {{.Status}}' | grep -E 'api-|worker-|meilisearch|prometheus|alertmanager|grafana' || true

API=$(docker ps --format '{{.Names}}' | grep -E '^api-' | head -1 || true)
if [[ -z "$API" ]]; then
  echo "ERROR: api container missing"
  exit 2
fi
API_IP=$(docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{println $v.IPAddress}}{{end}}' "$API" | awk 'NF{print; exit}')
TOKEN=$(docker exec "$API" printenv ADMIN_BEARER_TOKEN)

echo
echo "=== build ==="
curl -sS -m 10 "http://${API_IP}:8080/metrics" | grep nostrmash_build_info || true

echo
echo "=== search smoke ==="
curl -sS -m 15 -o /tmp/nm_search.json -w "search http=%{http_code} t=%{time_total}s\n" \
  "http://${API_IP}:8080/api/v1/search?q=nostr&limit=5" || FAIL=1

echo
echo "=== jobs dead (admin) ==="
curl -sS -m 30 -H "Authorization: Bearer ${TOKEN}" \
  "http://${API_IP}:8080/admin/v1/jobs?status=dead&limit=5" | head -c 400 || true
echo

echo
echo "=== disk alert threshold ==="
USE=$(df -P / | awk 'NR==2{gsub(/%/,"",$5); print $5}')
echo "rootfs_used_pct=$USE"
if (( USE >= 85 )); then
  echo "WARN: disk >= 85%"
  FAIL=1
fi

exit "$FAIL"
