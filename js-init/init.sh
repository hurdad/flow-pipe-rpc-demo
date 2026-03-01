#!/usr/bin/env sh
set -eu

NATS_URL="${NATS_URL:-nats://localhost:4222}"

echo "Waiting for NATS at ${NATS_URL}..."

until nats --server "${NATS_URL}" server check connection >/dev/null 2>&1; do
  sleep 1
done

echo "NATS is up."

echo "Ensuring stream FLOW exists..."
if ! nats --server "${NATS_URL}" stream info FLOW >/dev/null 2>&1; then
  nats --server "${NATS_URL}" stream add FLOW \
    --subjects flow.jobs \
    --retention limits \
    --storage file \
    --max-msgs=-1 \
    --max-bytes=-1 \
    --max-age=1h \
    --dupe-window=2m \
    --defaults
fi

echo "Ensuring durable consumer FLOW_PIPE exists..."
if ! nats --server "${NATS_URL}" consumer info FLOW FLOW_PIPE >/dev/null 2>&1; then
  nats --server "${NATS_URL}" consumer add FLOW FLOW_PIPE \
    --pull \
    --filter flow.jobs \
    --ack explicit \
    --deliver all \
    --replay instant \
    --max-deliver 10 \
    --defaults
fi

echo "JetStream bootstrap complete."