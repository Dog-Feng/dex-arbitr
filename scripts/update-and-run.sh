#!/usr/bin/env bash
# 仓库根目录：git pull → sidecar + 前端 + Rust release → 前台启动。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ ! -f config/default.yaml ]]; then
  echo "error: run from the repo (missing config/default.yaml)" >&2
  exit 1
fi

echo "== stop running dex-arbitr =="
if pgrep -x dex-arbitr >/dev/null 2>&1; then
  pkill -x dex-arbitr || true
  sleep 1
fi

echo "== git pull =="
git pull

echo "== sidecar =="
(
  cd scripts/exchange_sidecar
  go build -o exchange_sidecar .
)

echo "== web =="
(
  cd web
  npm install
  npm run build
)

echo "== rust release =="
cargo build --release

echo "== start =="
echo "panel: http://127.0.0.1:8090/   stop: Ctrl+C"
export RUST_LOG="${RUST_LOG:-info}"
exec ./target/release/dex-arbitr
