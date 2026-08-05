# Observability stack (single-server)

Use this page to deploy Prometheus, Alertmanager, and Grafana beside the Coolify
NostrMash app stack. Rule semantics stay in [observability.md](observability.md);
operator triage stays in [operations.md](operations.md).

## What this stack provides

| Service | Port (internal) | Role |
| --- | --- | --- |
| Prometheus | 9090 | Scrapes API `/metrics` and worker/ingestor/trust_worker `:9090`, evaluates recording + alert rules |
| Alertmanager | 9093 | Groups alerts (wire a Slack/email/Telegram receiver before relying on pages) |
| Grafana | 3000 | Pre-provisioned Prometheus datasource |

Memory budget is capped in Compose (~576 MiB combined). Do not raise limits without
revisiting the single-server RAM plan.

## Deploy on Coolify

1. Create a **Docker Compose** application pointing at this repository.
2. Set the Compose file to `observability/docker-compose.yml`.
3. Ensure the Coolify shared network named `coolify` exists (it already does when
   other Coolify apps run). The stack joins that network as `external: true`.
4. Set secrets:
   - `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` (required; change the default)
   - optional `GRAFANA_ROOT_URL` if you attach a public domain to Grafana
5. Deploy. Do **not** expose Prometheus or Alertmanager publicly. Grafana may be
   published behind Coolify auth / a private hostname only.

### Scrape targets

`observability/prometheus/prometheus.yml` scrapes:

| Job | Target |
| --- | --- |
| `nostrmash-api` | `api:8080/metrics` |
| `nostrmash-worker` | `worker:9090/metrics` |
| `nostrmash-ingestor` | `ingestor:9090/metrics` |
| `nostrmash-trust-worker` | `trust_worker:9090/metrics` |

Coolify already registers Compose service aliases (`api`, `worker`, …) on the
`coolify` network, so DNS resolution works across applications on the same host.

### Verify

From the Prometheus container (or via the Prometheus UI over SSH tunnel):

1. **Status → Targets** — all four NostrMash jobs `UP`.
2. **Status → Rules** — recording rules from `observability/recording_rules/` and
   alerts from `observability/alerts/` loaded without errors.
3. Query `nostrmash_build_info` and `nostrmash:api:error_ratio5m`.

## Phase 0 host prep (swap + pg_stat_statements)

On the Coolify host, as root:

```bash
# From a checkout of this repo on the host, or after copying the script:
sudo bash scripts/phase0_host_prep.sh
```

This is idempotent. It:

- creates an 8 GiB `/swapfile` if none exists, enables it, sets `vm.swappiness=10`
- enables `pg_stat_statements` via `ALTER SYSTEM` + container restart + `CREATE EXTENSION`

Expect a short Postgres blip during the restart. App containers reconnect automatically.

## Build identity (app stack)

In the NostrMash Coolify application, set:

```bash
APP_VERSION=<git-sha-or-release-tag>
SOURCE_COMMIT=<git-sha>
# optional
BUILD_TIME=<RFC3339 or CI timestamp>
```

`docker-compose.coolify.yml` passes these as Docker build `args` into the
Dockerfile ldflags so `nostrmash_build_info` stops reporting `version=dev` /
`commit=unknown`. Redeploy (rebuild) the app stack after setting them.

## Alert delivery

Default Alertmanager config keeps alerts visible in the UI but does not forward
them. Before treating Phase 0 as complete for paging:

1. Edit `observability/alertmanager/alertmanager.yml` (or mount an override).
2. Add Slack / email / Telegram receivers.
3. Redeploy the observability stack and fire a test alert.

## Related docs

- [observability.md](observability.md) — metric catalog and rule intent
- [operations.md](operations.md#alert-response-playbook) — what to do when alerts fire
- [coolify.md](coolify.md) — app stack layout
