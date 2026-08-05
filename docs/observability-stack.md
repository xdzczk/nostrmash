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
them. The Compose service runs `alertmanager/entrypoint.sh`, which renders a
live config from environment variables:

| Env | Effect |
| --- | --- |
| `ALERTMANAGER_TELEGRAM_BOT_TOKEN` | Bot token from [@BotFather](https://t.me/BotFather) |
| `ALERTMANAGER_TELEGRAM_CHAT_ID` | Numeric chat/group id (can be negative for groups) |
| `ALERTMANAGER_TELEGRAM_PARSE_MODE` | Default `HTML` |
| `ALERTMANAGER_WEBHOOK_URL` | Generic webhook (ntfy, Discord, custom) |
| `ALERTMANAGER_SLACK_WEBHOOK_URL` | Slack incoming webhook |
| `ALERTMANAGER_SLACK_CHANNEL` | Slack channel (default `#nostrmash-alerts`) |
| `ALERTMANAGER_SMTP_*` | Email (SMTP); see below — usually harder than Telegram |

Receiver priority in `entrypoint.sh`: webhook → Slack → **Telegram** → email → UI-only.

### Telegram setup (recommended)

1. In Telegram, message [@BotFather](https://t.me/BotFather) → `/newbot` → copy the bot token.
2. Start a chat with your bot (or add it to a private group) and send any message.
3. Get the chat id:
   - Open `https://api.telegram.org/bot<TOKEN>/getUpdates` in a browser, or
   - Message [@userinfobot](https://t.me/userinfobot) for your personal id.
   - Groups use a negative id (e.g. `-1001234567890`).
4. Set the vars on the **observability** stack (not the NostrMash app stack).
   For the host checkout at `/opt/nostrmash-observability`:

```bash
# in /opt/nostrmash-observability/.env
ALERTMANAGER_TELEGRAM_BOT_TOKEN=123456:ABC...
ALERTMANAGER_TELEGRAM_CHAT_ID=123456789
```

   If observability is a Coolify Compose app instead, set the same keys there.

5. Recreate Alertmanager so it re-renders config:

```bash
cd /opt/nostrmash-observability && docker compose up -d --force-recreate alertmanager
```

6. Confirm `telegram_configs` in the rendered config (`/-/ready` then
   `/api/v2/status`). Do not also set webhook/Slack env vars if you want Telegram
   (webhook wins first).

### Email setup (optional)

Needs a working SMTP relay (nostrmash.com mail is not required, but you must have *some* provider). Example:

```bash
ALERTMANAGER_SMTP_SMARTHOST=smtp.gmail.com:587
ALERTMANAGER_SMTP_FROM=alerts@yourdomain.com
ALERTMANAGER_SMTP_TO=you@yourdomain.com
ALERTMANAGER_SMTP_USERNAME=alerts@yourdomain.com
ALERTMANAGER_SMTP_PASSWORD=<app-password>
ALERTMANAGER_SMTP_REQUIRE_TLS=true
```

## Related docs

- [observability.md](observability.md) — metric catalog and rule intent
- [operations.md](operations.md#alert-response-playbook) — what to do when alerts fire
- [coolify.md](coolify.md) — app stack layout
