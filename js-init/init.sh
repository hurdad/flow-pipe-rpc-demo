#!/usr/bin/env bash
set -euo pipefail

NATS_URL="${NATS_URL:-nats://nats:4222}"
NATS_HTTP="${NATS_HTTP:-http://nats:8222}"

for i in {1..60}; do
  if curl -fsS "${NATS_HTTP}/jsz" >/dev/null; then
    break
  fi
  sleep 1
  if [[ "$i" -eq 60 ]]; then
    echo "NATS JetStream not ready"
    exit 1
  fi
done

curl -fsS -X PUT "${NATS_HTTP}/jsm/streams/FLOW" \
  -H 'Content-Type: application/json' \
  -d '{"name":"FLOW","subjects":["flow.jobs"],"storage":"file","retention":"limits"}' || true

curl -fsS -X PUT "${NATS_HTTP}/jsm/consumers/FLOW/FLOW_PIPE" \
  -H 'Content-Type: application/json' \
  -d '{"durable_name":"FLOW_PIPE","ack_policy":"explicit","ack_wait":30000000000,"max_deliver":5,"filter_subject":"flow.jobs"}' || true

echo "JetStream topology ready"
