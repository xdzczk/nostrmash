#!/usr/bin/env bash
# Wipe NostrMash application data on a Coolify host without touching Coolify itself.
#
# Preserves: coolify, coolify-db, coolify-redis, coolify-proxy, coolify-realtime, coolify-sentinel
# Removes:   nostrmash api/worker/ingestor/trust_worker/meilisearch + app postgres/redis volumes
#
# Run on the server as root after saving env vars from Coolify.

set -euo pipefail

COOLIFY_GUARD='^(coolify|coolify-db|coolify-redis|coolify-proxy|coolify-realtime|coolify-sentinel)$'

log() { printf '\n==> %s\n' "$*"; }
warn() { printf '\n*** %s\n' "$*" >&2; }

if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  echo "Run as root (ssh root@your-server)." >&2
  exit 1
fi

log "Disk before"
df -h /

log "Backing up NostrMash env vars (if api container exists)"
API=$(docker ps -a --format '{{.Names}}' | grep -E '^api-' | head -1 || true)
if [[ -n "${API}" ]]; then
  docker inspect "$API" --format '{{range .Config.Env}}{{println .}}{{end}}' \
    | sort > "/root/nostrmash-env-backup-$(date +%Y%m%d-%H%M%S).txt"
  echo "Saved to /root/nostrmash-env-backup-*.txt"
else
  warn "No api-* container found; skip env backup or export from Coolify UI."
fi

log "Containers that will be STOPPED and REMOVED (NostrMash stack only)"
mapfile -t TARGET_CONTAINERS < <(
  docker ps -a --format '{{.Names}}' \
    | grep -E '^(api|worker|ingestor|trust_worker|meilisearch)-' \
    || true
)

# Coolify-managed postgres/redis for NostrMash use random hex names (20+ chars).
# Exclude anything that belongs to coolify-* containers.
mapfile -t COOLIFY_VOLUMES < <(
  docker ps -a --format '{{.Names}}' \
    | grep -E "$COOLIFY_GUARD" \
    | xargs -r docker inspect --format '{{range .Mounts}}{{if .Name}}{{.Name}}{{"\n"}}{{end}}{{end}}' 2>/dev/null \
    | sort -u
)

is_coolify_volume() {
  local vol=$1
  local cv
  for cv in "${COOLIFY_VOLUMES[@]}"; do
    [[ "$vol" == "$cv" ]] && return 0
  done
  return 1
}

mapfile -t HEX_CONTAINERS < <(
  docker ps -a --format '{{.Names}}' \
    | grep -E '^[a-z0-9]{20,}$' \
    || true
)

for c in "${HEX_CONTAINERS[@]}"; do
  if docker inspect "$c" --format '{{.Config.Image}}' 2>/dev/null \
    | grep -qE 'postgres|redis'; then
    TARGET_CONTAINERS+=("$c")
  fi
done

if [[ ${#TARGET_CONTAINERS[@]} -eq 0 ]]; then
  warn "No NostrMash containers found. Nothing to do."
  exit 0
fi

printf '  %s\n' "${TARGET_CONTAINERS[@]}"

log "Volumes attached to those containers (candidates for deletion)"
declare -A TARGET_VOLUMES=()
for c in "${TARGET_CONTAINERS[@]}"; do
  while IFS= read -r vol; do
    [[ -z "$vol" ]] && continue
    if is_coolify_volume "$vol"; then
      warn "Skipping Coolify volume: $vol (attached to $c but protected)"
      continue
    fi
    TARGET_VOLUMES["$vol"]=1
  done < <(
    docker inspect "$c" --format '{{range .Mounts}}{{if .Name}}{{.Name}}{{"\n"}}{{end}}{{end}}' 2>/dev/null
  )
done

if [[ ${#TARGET_VOLUMES[@]} -eq 0 ]]; then
  warn "No deletable volumes found on NostrMash containers."
else
  for vol in "${!TARGET_VOLUMES[@]}"; do
    size=$(docker system df -v 2>/dev/null | awk -v v="$vol" '$1 == v {print $3; exit}' || true)
    printf '  %s  (%s)\n' "$vol" "${size:-unknown size}"
  done
fi

log "Coolify containers that will NOT be touched"
docker ps -a --format '{{.Names}}' | grep -E "$COOLIFY_GUARD" || echo "  (none running)"

echo
warn "This permanently deletes all NostrMash Postgres/Meili/Redis data."
warn "Coolify itself (coolify-db, coolify-redis, proxy) is protected."
read -r -p "Type WIPE to continue: " CONFIRM
[[ "$CONFIRM" == "WIPE" ]] || { echo "Aborted."; exit 1; }

log "Stopping NostrMash containers"
for c in "${TARGET_CONTAINERS[@]}"; do
  docker stop "$c" 2>/dev/null || true
done

log "Removing NostrMash containers"
for c in "${TARGET_CONTAINERS[@]}"; do
  docker rm "$c" 2>/dev/null || true
done

log "Removing NostrMash data volumes"
for vol in "${!TARGET_VOLUMES[@]}"; do
  if is_coolify_volume "$vol"; then
    warn "Refusing to remove protected Coolify volume: $vol"
    continue
  fi
  docker volume rm "$vol" || warn "Could not remove volume $vol (may still be in use)"
done

log "Removing dangling nostrmash images (optional cleanup)"
docker images --format '{{.Repository}}:{{.Tag}} {{.ID}}' \
  | grep -E '^nostrmash:' \
  | awk '{print $2}' \
  | xargs -r docker rmi 2>/dev/null || true

log "Disk after"
df -h /

log "Remaining Docker volumes (Coolify + any leftovers)"
docker volume ls

cat <<'EOF'

Done. Next steps in Coolify:

1. Open your NostrMash Docker Compose application.
2. Update env vars BEFORE redeploy:
   - INGESTOR_RELAY_URLS: 3-5 relays only (not 26)
   - INGESTOR_LIVE_BOOTSTRAP_LOOKBACK_SECONDS=86400
   - RELAY_REGISTRY_ENABLED=false
   - MEILI_ENABLED=false (until you need search)
   - WORKER_CONCURRENCY=8, WORKER_LIVE_CONCURRENCY=8
3. Redeploy the application (Coolify recreates containers + empty Postgres).
4. Verify:
     curl -sS https://api.nostrmash.com/health
     curl -sS https://api.nostrmash.com/ready

EOF
