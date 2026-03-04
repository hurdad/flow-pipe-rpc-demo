#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

cleanup() {
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker compose up -d --build

client_output="$(docker compose run --rm grpc-client 2>&1)"
echo "$client_output"

if [[ "$client_output" != *"status=OK"* ]]; then
  echo "E2E assertion failed: grpc-client output did not contain status=OK" >&2
  exit 1
fi

if [[ "$client_output" != *"processed_by=transform_stage"* ]]; then
  echo "E2E assertion failed: grpc-client output did not contain processed_by=transform_stage" >&2
  exit 1
fi

echo "E2E smoke test passed"
