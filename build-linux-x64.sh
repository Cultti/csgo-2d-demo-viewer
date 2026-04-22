#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${1:-$ROOT_DIR/deploy/linux-x64}"
TARBALL_PATH="${OUT_DIR}.tar.gz"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Error: required command '$1' not found in PATH" >&2
    exit 1
  fi
}

require_cmd go
require_cmd npm
require_cmd make
require_cmd tar

cd "$ROOT_DIR"

echo "[1/6] Cleaning output directory..."
rm -rf "$OUT_DIR" "$TARBALL_PATH"
mkdir -p "$OUT_DIR/bin" "$OUT_DIR/web"

echo "[2/6] Building parser WASM assets..."
make wasm

echo "[3/6] Installing web dependencies..."
npm --prefix web ci

echo "[4/6] Building web frontend..."
npm --prefix web run build

echo "[5/6] Building Linux x64 server binary..."
(
  cd server
  GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w" -o "$OUT_DIR/bin/csgo-demo-server" .
)

echo "[6/6] Packaging runtime files..."
cp -R "$ROOT_DIR/web/dist" "$OUT_DIR/web/dist"

cat > "$OUT_DIR/run.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export PORT="${PORT:-8080}"
export HOST="${HOST:-127.0.0.1}"
export WEB_DIST_DIR="${WEB_DIST_DIR:-$APP_DIR/web/dist}"

# Optional webhook/admin settings can be provided by env:
# WEBHOOK_HEADER_NAME, WEBHOOK_HEADER_VALUE, ADMIN_REPROCESS_TOKEN, REPLAYS_DIR, ALLOWED_ORIGINS

exec "$APP_DIR/bin/csgo-demo-server" -port "$PORT" -host "$HOST" -web-dir "$WEB_DIST_DIR"
EOF

chmod +x "$OUT_DIR/run.sh"
chmod +x "$OUT_DIR/bin/csgo-demo-server"

tar -czf "$TARBALL_PATH" -C "$(dirname "$OUT_DIR")" "$(basename "$OUT_DIR")"

echo
echo "Build complete."
echo "Deploy folder: $OUT_DIR"
echo "Tarball: $TARBALL_PATH"
echo
echo "Run on server:"
echo "  cd $(basename "$OUT_DIR") && ./run.sh"
echo
echo "Override port/host example:"
echo "  PORT=9000 HOST=127.0.0.1 ./run.sh"
