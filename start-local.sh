#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Error: required command '$1' not found in PATH" >&2
    exit 1
  fi
}

require_cmd go
require_cmd npm
require_cmd make

cd "$ROOT_DIR"

echo "[1/3] Building parser WASM artifact..."
make wasm

echo "[2/3] Installing web dependencies..."
npm --prefix web ci

echo "[3/3] Building web frontend..."
npm --prefix web run build

export WEBHOOK_HEADER_NAME="Authorization"
export WEBHOOK_HEADER_VALUE="123"

echo "Starting Go server in dev mode on http://localhost:8080"
echo "Webhook auth configured: ${WEBHOOK_HEADER_NAME}: ${WEBHOOK_HEADER_VALUE}"
cd server
exec go run . -dev
