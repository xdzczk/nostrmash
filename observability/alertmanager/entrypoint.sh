#!/bin/sh
# Render Alertmanager config from env so Coolify can supply a webhook without
# editing the checked-in YAML. Falls back to the null-receiver config.
set -eu

TEMPLATE_SRC="${ALERTMANAGER_CONFIG_TEMPLATE:-/etc/alertmanager/alertmanager.yml}"
RENDERED="${ALERTMANAGER_CONFIG_RENDERED:-/tmp/alertmanager.rendered.yml}"

if [ -n "${ALERTMANAGER_WEBHOOK_URL:-}" ]; then
  cat >"$RENDERED" <<EOF
global:
  resolve_timeout: 5m

route:
  receiver: default
  group_by: ["alertname", "severity"]
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h
  routes:
    - matchers:
        - severity = "critical"
      receiver: default
      repeat_interval: 1h

receivers:
  - name: default
    webhook_configs:
      - url: '${ALERTMANAGER_WEBHOOK_URL}'
        send_resolved: true

inhibit_rules:
  - source_matchers:
      - severity = "critical"
    target_matchers:
      - severity = "warning"
    equal: ["alertname"]
EOF
elif [ -n "${ALERTMANAGER_SLACK_WEBHOOK_URL:-}" ]; then
  CHANNEL="${ALERTMANAGER_SLACK_CHANNEL:-#nostrmash-alerts}"
  cat >"$RENDERED" <<EOF
global:
  resolve_timeout: 5m

route:
  receiver: default
  group_by: ["alertname", "severity"]
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h
  routes:
    - matchers:
        - severity = "critical"
      receiver: default
      repeat_interval: 1h

receivers:
  - name: default
    slack_configs:
      - api_url: '${ALERTMANAGER_SLACK_WEBHOOK_URL}'
        channel: '${CHANNEL}'
        send_resolved: true

inhibit_rules:
  - source_matchers:
      - severity = "critical"
    target_matchers:
      - severity = "warning"
    equal: ["alertname"]
EOF
else
  cp "$TEMPLATE_SRC" "$RENDERED"
fi

exec /bin/alertmanager --config.file="$RENDERED" --storage.path=/alertmanager "$@"
