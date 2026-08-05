#!/usr/bin/env bash
# Phase 0 host prep for the single-server remediation plan.
# Run as root on the Coolify host (nostrmash-server).
#
# - 8 GiB swapfile (vm.swappiness=10)
# - Enable pg_stat_statements on the Coolify Postgres container
#
# Idempotent: safe to re-run.

set -euo pipefail

SWAPFILE="${SWAPFILE:-/swapfile}"
SWAP_SIZE_GB="${SWAP_SIZE_GB:-8}"
SWAPPINESS="${SWAPPINESS:-10}"

log() { printf '%s\n' "$*"; }

ensure_swap() {
  if swapon --show | grep -q .; then
    log "swap already active:"
    swapon --show
  else
    if [[ ! -f "$SWAPFILE" ]]; then
      log "creating ${SWAP_SIZE_GB}GiB swapfile at ${SWAPFILE}"
      fallocate -l "${SWAP_SIZE_GB}G" "$SWAPFILE" || dd if=/dev/zero of="$SWAPFILE" bs=1M count=$((SWAP_SIZE_GB * 1024)) status=progress
      chmod 600 "$SWAPFILE"
      mkswap "$SWAPFILE"
    fi
    swapon "$SWAPFILE"
    log "swap enabled"
    swapon --show
  fi

  if ! grep -qE "^${SWAPFILE}\\s" /etc/fstab 2>/dev/null; then
    echo "${SWAPFILE} none swap sw 0 0" >>/etc/fstab
    log "added ${SWAPFILE} to /etc/fstab"
  fi

  sysctl -w "vm.swappiness=${SWAPPINESS}" >/dev/null
  if [[ -d /etc/sysctl.d ]]; then
    echo "vm.swappiness=${SWAPPINESS}" >/etc/sysctl.d/99-nostrmash-swappiness.conf
  fi
  log "vm.swappiness=$(cat /proc/sys/vm/swappiness)"
}

enable_pg_stat_statements() {
  local pg
  pg="$(docker ps --format '{{.Names}}' | grep -E '^p8ucu|^.*postgres' | head -1 || true)"
  if [[ -z "$pg" ]]; then
    # Prefer the Coolify-managed Postgres resource used by NostrMash.
    pg="$(docker ps --format '{{.Names}} {{.Image}}' | awk '/postgres:18/ {print $1; exit}')"
  fi
  if [[ -z "$pg" ]]; then
    log "ERROR: could not find Postgres container"
    return 1
  fi
  log "using Postgres container: ${pg}"

  local preload
  preload="$(docker exec "$pg" psql -U postgres -d postgres -tAc 'SHOW shared_preload_libraries;' | tr -d '[:space:]')"
  if [[ "$preload" == *"pg_stat_statements"* ]]; then
    log "shared_preload_libraries already includes pg_stat_statements (${preload})"
  else
    if [[ -z "$preload" ]]; then
      docker exec "$pg" psql -U postgres -d postgres -v ON_ERROR_STOP=1 -c "ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';"
    else
      docker exec "$pg" psql -U postgres -d postgres -v ON_ERROR_STOP=1 -c "ALTER SYSTEM SET shared_preload_libraries = '${preload},pg_stat_statements';"
    fi
    docker exec "$pg" psql -U postgres -d postgres -v ON_ERROR_STOP=1 -c "ALTER SYSTEM SET pg_stat_statements.track = 'all';"
    docker exec "$pg" psql -U postgres -d postgres -v ON_ERROR_STOP=1 -c "ALTER SYSTEM SET pg_stat_statements.max = 10000;"
    log "updated postgresql.auto.conf — Postgres container must be restarted for preload to take effect"
    log "restarting ${pg} ..."
    docker restart "$pg"
    log "waiting for Postgres to accept connections"
    for _ in $(seq 1 60); do
      if docker exec "$pg" pg_isready -U postgres >/dev/null 2>&1; then
        break
      fi
      sleep 1
    done
  fi

  docker exec "$pg" psql -U postgres -d postgres -v ON_ERROR_STOP=1 -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;"
  docker exec "$pg" psql -U postgres -d postgres -c "SELECT extname, extversion FROM pg_extension WHERE extname = 'pg_stat_statements';"
  docker exec "$pg" psql -U postgres -d postgres -c "SHOW shared_preload_libraries;"
}

main() {
  if [[ "$(id -u)" -ne 0 ]]; then
    log "ERROR: run as root"
    exit 1
  fi
  ensure_swap
  enable_pg_stat_statements
  log "Phase 0 host prep complete"
}

main "$@"
