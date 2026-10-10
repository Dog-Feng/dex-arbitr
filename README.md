# dex-arbitr

Cross-DEX perpetual spread step arbitrage (design: `docs/spread-arb-design.md`).
Currently integrated venues: **Lighter mainnet** (`kind: lighter`, chain 304) and **RH Lighter mainnet** (`kind: rhlighter`, chain 466324).

## Layout

```
cmd/arbd            engine
cmd/selfcheck       end-to-end self check (read-only / -live / -rescue)
configs/config.yaml config (secrets via ${ENV})
internal/domain     shared types
internal/exchange   Venue port + registry; lighter/ = Lighter-family adapter (REST + WS + signer)
internal/symbols    perp matching (suffix strip, exact base) + precision alignment
internal/spread     spread quote; estimator/ = pluggable mean (SMA)
internal/strategy   pure Decide()
internal/execution  two-leg IOC executor, order tracker, rescue ladder
internal/engine     one actor per pair
internal/store      JSON step state
```

## Credentials (environment variables, never commit)

```
export LIGHTER_ACCOUNT_INDEX=...  LIGHTER_API_KEY_INDEX=...  LIGHTER_PRIVATE_KEY=0x...
export RH_ACCOUNT_INDEX=...       RH_API_KEY_INDEX=...       RH_PRIVATE_KEY=0x...
```

API key index: Lighter 4–254, RH 4–156 / 158–254.

## Run

```
go test ./...
go run ./cmd/selfcheck                    # read-only: instruments, matching, precision, WS books, spread, signing
go run ./cmd/selfcheck -live              # + real SNDK 0.01 open -> close round trip
go run ./cmd/selfcheck -live -rescue      # + forced one-leg failure, verifies reduce-only flatten
go run ./cmd/arbd -config configs/config.yaml   # dry_run: true = paper trading
```

> Lighter blocks order submission (`sendTx`, authenticated WS) from restricted jurisdictions (e.g. US IPs, code 20558).
> Run live trading from a permitted region.

---

## Linux 部署（systemd）

以下以 Ubuntu/Debian x86_64 为例，部署目录 `/opt/dex-arbitr`，运行用户 `arb`。**服务器必须位于 Lighter 允许交易的地区**（否则下单返回 20558）。

### 1. 安装 Go（≥ 1.23）

```bash
sudo apt-get update && sudo apt-get install -y git curl
curl -fsSL https://go.dev/dl/go1.23.4.linux-amd64.tar.gz | sudo tar -C /usr/local -xz
echo 'export PATH=/usr/local/go/bin:$PATH' | sudo tee /etc/profile.d/go.sh
source /etc/profile.d/go.sh && go version
```

（ARM 服务器把 `linux-amd64` 换成 `linux-arm64`。）

### 2. 拉取代码并编译

```bash
sudo useradd -r -m -d /opt/dex-arbitr -s /bin/bash arb
sudo -iu arb
git clone https://github.com/Dog-Feng/dex-arbitr.git ~/app
cd ~/app
go test ./...
mkdir -p ~/bin
go build -o ~/bin/arbd ./cmd/arbd
go build -o ~/bin/selfcheck ./cmd/selfcheck
```

私有仓库时，clone 使用 `https://<token>@github.com/...` 或配置 SSH Deploy Key；不要把 token 写进仓库文件。

### 3. 配置密钥（环境变量文件）

```bash
cat > ~/arb.env <<'ENV'
LIGHTER_ACCOUNT_INDEX=123456
LIGHTER_API_KEY_INDEX=4
LIGHTER_PRIVATE_KEY=0x...
RH_ACCOUNT_INDEX=654321
RH_API_KEY_INDEX=4
RH_PRIVATE_KEY=0x...
ENV
chmod 600 ~/arb.env
```

运行配置：复制一份到仓库之外，避免 `git pull` 冲突：

```bash
cp ~/app/configs/config.yaml ~/config.yaml
# 编辑 ~/config.yaml：确认 pairs / step_qty / max_steps；state_dir 改为绝对路径 /opt/dex-arbitr/state
```

### 4. 上线前自检

```bash
cd ~/app
set -a; source ~/arb.env; set +a
~/bin/selfcheck -config ~/config.yaml                 # 只读检查
~/bin/selfcheck -config ~/config.yaml -live           # 实盘 0.01 开→平
~/bin/selfcheck -config ~/config.yaml -live -rescue   # 单腿失败补救
```

三项都显示 `RESULT: N passed, 0 failed` 后，把 `~/config.yaml` 中的 `dry_run` 改为 `false`（先用 `true` 模拟盘观察也可以）。

### 5. 注册 systemd 服务

```bash
exit   # 回到有 sudo 权限的用户
sudo tee /etc/systemd/system/arbd.service >/dev/null <<'UNIT'
[Unit]
Description=dex-arbitr spread step arbitrage
After=network-online.target
Wants=network-online.target

[Service]
User=arb
WorkingDirectory=/opt/dex-arbitr
EnvironmentFile=/opt/dex-arbitr/arb.env
ExecStart=/opt/dex-arbitr/bin/arbd -config /opt/dex-arbitr/config.yaml
Restart=on-failure
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
UNIT
sudo systemctl daemon-reload
sudo systemctl enable --now arbd
```

### 6. 日常运维

```bash
sudo systemctl status arbd          # 运行状态
sudo journalctl -u arbd -f          # 实时日志
sudo journalctl -u arbd --since "1 hour ago" | grep -E "action|step|rescue|ALERT"
sudo systemctl stop arbd            # 停止（收到 SIGTERM 会先保存步数状态）
cat /opt/dex-arbitr/state/*.json    # 当前持有的步
```

- 均值需要先采样 60 分钟才会开始开仓（日志出现 `mean ready -> RUNNING`）。
- 启动时会核对两边持仓；若两边不对冲（合计不为 0）会自动进入 `PAUSED`，需人工处理后再重启。

## 更新流程

```bash
sudo -iu arb
cd ~/app
git fetch && git log --oneline HEAD..origin/main     # 查看有哪些更新
git pull --ff-only
go test ./...                                        # 测试不过就不要继续
go build -o ~/bin/arbd.new ./cmd/arbd && go build -o ~/bin/selfcheck ./cmd/selfcheck
set -a; source ~/arb.env; set +a
~/bin/selfcheck -config ~/config.yaml                # 只读自检
diff ~/app/configs/config.yaml ~/config.yaml         # 检查是否有新增配置项需要同步
mv ~/bin/arbd ~/bin/arbd.prev && mv ~/bin/arbd.new ~/bin/arbd
exit
sudo systemctl restart arbd && sudo journalctl -u arbd -f
```

- 重启是安全的：步数状态保存在 `state/`，重启后自动加载并与实际持仓核对。
- 建议在没有补救单进行时更新（日志中无 `rescue order` 输出）。

**回滚：**

```bash
sudo -iu arb mv /opt/dex-arbitr/bin/arbd.prev /opt/dex-arbitr/bin/arbd
sudo systemctl restart arbd
```

如需同时回滚代码：`cd ~/app && git checkout <上一个提交号>` 后重新编译。
