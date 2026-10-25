#!/usr/bin/env bash
# start.sh — 拉取代码 → 构建 → 只读自检 → 启动 arbd + scanboard
# 用法：上传到 /home/ubuntu/dex-arbitr/start.sh，chmod +x，然后 ./start.sh
# 运行前需先通过 SFTP 上传 config.yaml 和 arb.env 到同一目录。
# 环境变量可覆盖：
#   BASE=/home/ubuntu/dex-arbitr   工作目录
#   BRANCH=codex/zscore-spread     代码分支
#   SELFCHECK_LIVE=1               额外跑一次 -live 自检（会真实下单 0.01 往返！）
set -euo pipefail

BASE="${BASE:-/home/ubuntu/dex-arbitr}"
BRANCH="${BRANCH:-codex/zscore-spread}"
REPO="${REPO:-https://github.com/Dog-Feng/dex-arbitr.git}"
GOROOT_BIN="${GOROOT_BIN:-/usr/local/go/bin}"

log() { echo "[start] $(date '+%F %T') $*"; }

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

# ---------- 2. 构建 ----------
log "build"
cd "$BASE/app"
go build -o "$BASE/bin/arbd" ./cmd/arbd
go build -o "$BASE/bin/selfcheck" ./cmd/selfcheck
go build -o "$BASE/bin/scanboard" ./cmd/scanboard
go build -o "$BASE/bin/pairlist" ./cmd/pairlist
cd "$BASE"

# ---------- 3. 配置检查 ----------
[ -f "$BASE/config.yaml" ] || { log "缺少 config.yaml，请先 SFTP 上传"; exit 1; }
[ -f "$BASE/arb.env" ] || { log "缺少 arb.env，请先 SFTP 上传"; exit 1; }
[ -f "$BASE/lenses.json" ] || cp "$BASE/app/configs/lenses.example.json" "$BASE/lenses.json"
chmod 600 "$BASE/arb.env"

# 状态/样本目录改为绝对路径，避免相对路径随 CWD 漂移
sed -i "s|^state_dir:.*|state_dir: $BASE/state|" "$BASE/config.yaml"
sed -i "s|^samples_dir:.*|samples_dir: $BASE/state/samples|" "$BASE/config.yaml"

if grep -q '^dry_run: *true' "$BASE/config.yaml"; then
    log "WARN: config.yaml 中 dry_run=true —— 当前为纸面交易模式"
fi

# ---------- 4. 只读自检（失败即停止，不下单） ----------
log "selfcheck (read-only)"
set -a; source "$BASE/arb.env"; set +a
"$BASE/bin/selfcheck" -config "$BASE/config.yaml"

if [ "${SELFCHECK_LIVE:-0}" = "1" ]; then
    log "selfcheck -live（真实 0.01 往返，需已确认账户状态）"
    "$BASE/bin/selfcheck" -config "$BASE/config.yaml" -live
fi

# ---------- 5. 停掉本目录旧进程 + 清残留锁 ----------
# 只杀自己目录下的进程；/opt 下的 root 旧实盘需 root 手动 systemctl stop arbd
pkill -f "$BASE/bin/arbd" 2>/dev/null || true
pkill -f "$BASE/bin/scanboard" 2>/dev/null || true
sleep 2
rm -f "$BASE/state"/.agent-*.lock

# ---------- 6. 启动（可用 START_ARBD=0 / START_SCANBOARD=0 单独关掉其一） ----------
if [ "${START_ARBD:-1}" = "1" ]; then
    log "start arbd"
    nohup "$BASE/bin/arbd" -config "$BASE/config.yaml" >> "$BASE/arbd.log" 2>&1 &
    echo $! > "$BASE/arbd.pid"
    sleep 3
    log "arbd pid=$(cat "$BASE/arbd.pid"), 日志: $BASE/arbd.log"
    tail -n 20 "$BASE/arbd.log"
fi

if [ "${START_SCANBOARD:-1}" = "1" ]; then
    log "start scanboard"
    nohup "$BASE/bin/scanboard" -config "$BASE/config.yaml" -lenses "$BASE/lenses.json" \
        -out "$BASE/scanboard-$(date +%Y%m%d).jsonl" >> "$BASE/scanboard.log" 2>&1 &
    sleep 2
    log "scanboard 日志: $BASE/scanboard.log"
fi

log "启动完成。日常更新请分别使用 up-arbd.sh / up-scanboard.sh"
