#!/usr/bin/env bash
# up-scanboard.sh — 只更新并重启多镜头扫描器（不动 arbd）
# 扫描应长跑：仅在需要更新扫描器代码/镜头时执行本脚本。
# 重启会重新预热各镜头估计器（zscore 15m / sma 1h），期间仍持续落盘
# （reason="mean not ready"），数据文件无缺口。
# 用法：/home/ubuntu/dex-arbitr/up-scanboard.sh
set -euo pipefail

BASE="${BASE:-/home/ubuntu/dex-arbitr}"
BRANCH="${BRANCH:-codex/zscore-spread}"
REPO="${REPO:-https://github.com/Dog-Feng/dex-arbitr.git}"
GOROOT_BIN="${GOROOT_BIN:-/usr/local/go/bin}"

log() { echo "[scan] $(date '+%F %T') $*"; }
export PATH="$GOROOT_BIN:$PATH"
mkdir -p "$BASE/bin"
cd "$BASE"

# ---------- 1. 拉取代码 ----------
if [ -d "$BASE/app/.git" ]; then
    log "pull $BRANCH"
    git -C "$BASE/app" fetch --all --prune
    git -C "$BASE/app" checkout "$BRANCH"
    git -C "$BASE/app" pull --ff-only
else
    log "clone $REPO"
    git clone "$REPO" "$BASE/app"
    git -C "$BASE/app" checkout "$BRANCH"
fi
log "HEAD: $(git -C "$BASE/app" log --oneline -1)"

# ---------- 2. 构建 scanboard ----------
log "build scanboard"
cd "$BASE/app"
go build -o "$BASE/bin/scanboard" ./cmd/scanboard
cd "$BASE"

# ---------- 3. 配置检查 ----------
[ -f "$BASE/config.yaml" ] || { log "缺少 config.yaml"; exit 1; }
[ -f "$BASE/lenses.json" ] || cp "$BASE/app/configs/lenses.example.json" "$BASE/lenses.json"

# ---------- 4. 重启 scanboard（只动 scanboard） ----------
log "restart scanboard"
pkill -f "$BASE/bin/scanboard" 2>/dev/null || true
sleep 2
nohup "$BASE/bin/scanboard" -config "$BASE/config.yaml" -lenses "$BASE/lenses.json" \
    -out "$BASE/scanboard-$(date +%Y%m%d).jsonl" >> "$BASE/scanboard.log" 2>&1 &

sleep 3
log "scanboard started, 日志: $BASE/scanboard.log"
tail -n 10 "$BASE/scanboard.log"
