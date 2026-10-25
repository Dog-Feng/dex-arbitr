#!/usr/bin/env bash
# up-arbd.sh — 只更新并重启交易引擎 arbd（不动 scanboard）
# 用法：/home/ubuntu/dex-arbitr/up-arbd.sh
#   SELFCHECK_LIVE=1 ./up-arbd.sh   自检额外跑真实 0.01 往返
set -euo pipefail

BASE="${BASE:-/home/ubuntu/dex-arbitr}"
BRANCH="${BRANCH:-codex/zscore-spread}"
REPO="${REPO:-https://github.com/Dog-Feng/dex-arbitr.git}"
GOROOT_BIN="${GOROOT_BIN:-/usr/local/go/bin}"

log() { echo "[arbd] $(date '+%F %T') $*"; }
export PATH="$GOROOT_BIN:$PATH"
mkdir -p "$BASE/bin" "$BASE/state"
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

# ---------- 2. 构建 arbd + selfcheck ----------
log "build"
cd "$BASE/app"
go build -o "$BASE/bin/arbd" ./cmd/arbd
go build -o "$BASE/bin/selfcheck" ./cmd/selfcheck
cd "$BASE"

# ---------- 3. 配置检查 ----------
[ -f "$BASE/config.yaml" ] || { log "缺少 config.yaml"; exit 1; }
[ -f "$BASE/arb.env" ] || { log "缺少 arb.env"; exit 1; }
chmod 600 "$BASE/arb.env"
sed -i "s|^state_dir:.*|state_dir: $BASE/state|" "$BASE/config.yaml"
sed -i "s|^samples_dir:.*|samples_dir: $BASE/state/samples|" "$BASE/config.yaml"

# ---------- 4. 只读自检（失败即停） ----------
log "selfcheck (read-only)"
set -a; source "$BASE/arb.env"; set +a
"$BASE/bin/selfcheck" -config "$BASE/config.yaml"
if [ "${SELFCHECK_LIVE:-0}" = "1" ]; then
    log "selfcheck -live（真实 0.01 往返）"
    "$BASE/bin/selfcheck" -config "$BASE/config.yaml" -live
fi

# ---------- 5. 重启 arbd（只动 arbd） ----------
log "restart arbd"
pkill -f "$BASE/bin/arbd" 2>/dev/null || true
sleep 2
rm -f "$BASE/state"/.agent-*.lock
nohup "$BASE/bin/arbd" -config "$BASE/config.yaml" >> "$BASE/arbd.log" 2>&1 &
echo $! > "$BASE/arbd.pid"

sleep 3
log "arbd pid=$(cat "$BASE/arbd.pid")"
tail -n 20 "$BASE/arbd.log"
