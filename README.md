# dex-arbitr

Cross-DEX perpetual spread step arbitrage (design: `docs/spread-arb-design.md`).
Currently integrated venues: **Lighter mainnet** (`kind: lighter`, chain 304) and **RH Lighter mainnet** (`kind: rhlighter`, chain 466324).

本轮 F01–F40 修复、回归测试及恢复边界见 [交易安全与生命周期修复说明](docs/trading-safety-fixes.md)。代码检查只运行测试和编译，不自动启动项目或服务；下文运行命令由操作者明确选择执行。

`arbitrage / volume` 与 `manual / auto` 的四种组合、预热及参数规则见 [策略与选币完整链路](docs/strategy-selection-flow.md)。两种策略都采用配置中的 2 小时滚动窗口、1 小时预热；volume 的额外开平阈值固定为零，手续费、深度和所配置的波动过滤仍生效。

升级时请保留完整 `state_dir` 与配置，并注意：

- 同一状态目录只允许一个写入进程；停止信号保存状态，不承诺自动平仓。
- 尽量在订单和补救均已确认后升级。未决 checkpoint 即使步骤为空，重启仍保持 `PAUSED`，需核实交易所订单和两边仓位后再处理恢复。
- 旧状态缺少入场价格会暂停普通交易；历史费用或成交价格不确定时，会把 state 文件里的 `pnl_uncertain` 标记置位（仅作审计痕迹，不再作为开仓闸门），减仓与应急退出仍受确认条件保护。

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

普通开平仓 IOC 的让价幅度由配置文件控制，`arbd` 和实盘 `selfcheck` 共用：

```yaml
execution:
  price_ticks: 30
```

固定模式下，买单限价为决策价格加 N 个 tick，卖单为决策价格减 N 个 tick；tick 是各交易所的最小价格单位，Hyperliquid 腿按其 5 位有效数字规则在决策价格处取有效 tick（整数价格始终可表示，例如 ANTH ~1960 处 1 tick = 0.1，价格 ≥ 100000 处 1 tick = 1），救援阶梯与动态让价共用同一有效 tick。设为 `0` 时使用决策价格，省略时默认 `30`，负数配置会报错。启用 `dynamic_ticks` 且波动已就绪时，改用波动、盘口深度及 min/max ticks 计算范围；未就绪时回退到 `price_ticks`。可选开启 `execution.adaptive_latency` 后，波动窗口按实测腿延迟自适应（价差窗口取两腿较大者）。普通补救使用 IOC 价格阶梯；应急退出和 drain 使用 reduce-only 保护市价单，并确认每腿分别归零。修改配置在操作者重启后生效。

开平仓均要求短周期波动率 σ 就绪（按增量计数，攒满 `volatility.horizon/interval` 个增量即 ready，断链不作废）；σ 未就绪时策略开平仓被拦截，仅救援、drain 与应急清仓可用。

> Lighter blocks order submission (`sendTx`, authenticated WS) from restricted jurisdictions (e.g. US IPs, code 20558).
> Run live trading from a permitted region.

---

## Linux 部署（ubuntu 用户 + 家目录脚本，推荐）

交易引擎与多镜头扫描器分离运维：**扫描器长跑持续收集样本，交易引擎可频繁更新重启而不中断扫描**。

### 目录布局

| 路径 | 用途 |
|---|---|
| `/home/ubuntu/dex-arbitr/app` | 代码（git 仓库，ubuntu 属主） |
| `/home/ubuntu/dex-arbitr/bin` | 二进制：arbd / selfcheck / scanboard / pairlist |
| `/home/ubuntu/dex-arbitr/config.yaml` | 运行配置（SFTP 上传，仓库外） |
| `/home/ubuntu/dex-arbitr/arb.env` | 密钥（600，SFTP 上传） |
| `/home/ubuntu/dex-arbitr/lenses.json` | 扫描镜头定义（缺省用仓库示例） |
| `/home/ubuntu/dex-arbitr/state` | 步数状态（自动创建） |
| `/home/ubuntu/dex-arbitr/arbd.log` / `scanboard.log` | 运行日志 |
| `/home/ubuntu/dex-arbitr/scanboard-YYYYMMDD.jsonl` | 扫描数据（按天） |

### 首次部署

root 做一次目录准备：

```bash
mkdir -p /home/ubuntu/dex-arbitr && chown ubuntu:ubuntu /home/ubuntu/dex-arbitr
```

ubuntu 用户拉取脚本：

```bash
cd /home/ubuntu/dex-arbitr
curl -fsSL https://raw.githubusercontent.com/Dog-Feng/dex-arbitr/codex/zscore-spread/scripts/start.sh -o start.sh
curl -fsSL https://raw.githubusercontent.com/Dog-Feng/dex-arbitr/codex/zscore-spread/scripts/up-arbd.sh -o up-arbd.sh
curl -fsSL https://raw.githubusercontent.com/Dog-Feng/dex-arbitr/codex/zscore-spread/scripts/up-scanboard.sh -o up-scanboard.sh
chmod +x start.sh up-arbd.sh up-scanboard.sh
```

SFTP 上传 `config.yaml` 与 `arb.env` 到该目录后：

```bash
sed -i 's/\r$//' config.yaml arb.env start.sh up-arbd.sh up-scanboard.sh   # 去 Windows CRLF
./start.sh                                  # 首次引导：拉码→构建→只读自检→启动两个模块
```

`start.sh` 支持 `START_ARBD=0` / `START_SCANBOARD=0` 单独关闭某模块；`SELFCHECK_LIVE=1` 追加真实 0.01 往返自检（会真实下单）。

### 日常更新（两模块互不干扰）

```bash
./up-arbd.sh        # 只更新并重启交易引擎（扫描持续收集，不中断）
./up-scanboard.sh   # 只更新并重启扫描器（交易不中断）
```

### 注意事项

- 旧版 root/systemd 部署需先 `systemctl stop arbd` 并清理其 state 目录下的 `.agent-*.lock`，防止 agent-key 锁冲突。
- 扫描器重启后各镜头重新预热（zscore 15m / sma 1h），期间仍持续落盘（`"reason":"mean not ready"`），数据文件无缺口。
- zscore 均值状态随 state 持久化：arbd 重启在 6 小时内免预热。
- Windows 编辑/上传的文件必须先 `sed -i 's/\r$//'` 去 CRLF，否则脚本报错、env 值被 `\r` 污染。
- 服务器必须位于 Lighter 允许交易的地区（否则下单返回 20558）。

---

## Linux 部署（root 用户 + systemd，旧版备查）

以 Ubuntu/Debian x86_64 为例。**所有操作都以 root 用户执行**，统一目录：

| 路径 | 用途 |
|---|---|
| `/opt/dex-arbitr/app` | 代码（git 仓库） |
| `/opt/dex-arbitr/bin` | 编译好的程序 |
| `/opt/dex-arbitr/config.yaml` | 运行配置（仓库之外，避免 `git pull` 冲突） |
| `/opt/dex-arbitr/arb.env` | 密钥（权限 600） |
| `/opt/dex-arbitr/state` | 步数状态 |

**服务器必须位于 Lighter 允许交易的地区**（否则下单返回 20558）。

### 0. 切换到 root

```bash
if [ "$(id -u)" -ne 0 ]; then echo "当前用户是 $(whoami)，不是 root，请先执行：sudo -i"; else echo "OK：当前是 root"; fi
```

如果提示不是 root，执行 `sudo -i` 切换后再继续。之后所有步骤都在 root 下执行（命令里不再写 `sudo`）。

### 1. 安装依赖与 Go（≥ 1.23）

```bash
apt-get update && apt-get install -y git curl
curl -fsSL https://go.dev/dl/go1.23.4.linux-amd64.tar.gz | tar -C /usr/local -xz
echo 'export PATH=/usr/local/go/bin:$PATH' > /etc/profile.d/go.sh
source /etc/profile.d/go.sh && go version
```

ARM 服务器把 `linux-amd64` 换成 `linux-arm64`。

1G 内存的机器建议先加 2G swap，避免编译时内存不足：

```bash
fallocate -l 2G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
echo '/swapfile none swap sw 0 0' >> /etc/fstab
```

### 2. 拉取代码并编译

```bash
mkdir -p /opt/dex-arbitr/bin /opt/dex-arbitr/state
git clone https://github.com/Dog-Feng/dex-arbitr.git /opt/dex-arbitr/app
ls /opt/dex-arbitr/app/configs/config.yaml        # 能看到文件才继续
cd /opt/dex-arbitr/app
go test ./...
go build -o /opt/dex-arbitr/bin/arbd ./cmd/arbd
go build -o /opt/dex-arbitr/bin/selfcheck ./cmd/selfcheck
```

私有仓库时用 `https://<token>@github.com/...` 克隆或配置 SSH Deploy Key；不要把 token 写进仓库文件。

### 3. 配置密钥与运行配置

```bash
cat > /opt/dex-arbitr/arb.env <<'ENV'
LIGHTER_ACCOUNT_INDEX=123456
LIGHTER_API_KEY_INDEX=4
LIGHTER_PRIVATE_KEY=0x...
RH_ACCOUNT_INDEX=654321
RH_API_KEY_INDEX=4
RH_PRIVATE_KEY=0x...
ENV
chmod 600 /opt/dex-arbitr/arb.env

cp /opt/dex-arbitr/app/configs/config.yaml /opt/dex-arbitr/config.yaml
sed -i 's#^state_dir:.*#state_dir: /opt/dex-arbitr/state#' /opt/dex-arbitr/config.yaml
```

用 `nano /opt/dex-arbitr/config.yaml` 确认 `pairs` / `step_qty` / `max_steps`。

### 4. 上线前自检

```bash
cd /opt/dex-arbitr/app
set -a; source /opt/dex-arbitr/arb.env; set +a
/opt/dex-arbitr/bin/selfcheck -config /opt/dex-arbitr/config.yaml                 # 只读检查
/opt/dex-arbitr/bin/selfcheck -config /opt/dex-arbitr/config.yaml -live           # 实盘 0.01 开→平
/opt/dex-arbitr/bin/selfcheck -config /opt/dex-arbitr/config.yaml -live -rescue   # 单腿失败补救
```

三项都显示 `RESULT: N passed, 0 failed` 后，把 `/opt/dex-arbitr/config.yaml` 中 `dry_run` 改为 `false`（也可以先保持 `true` 用模拟盘观察）。

### 5. 注册 systemd 服务

```bash
cat > /etc/systemd/system/arbd.service <<'UNIT'
[Unit]
Description=dex-arbitr spread step arbitrage
After=network-online.target
Wants=network-online.target

[Service]
User=root
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
systemctl daemon-reload
systemctl enable --now arbd
```

### 6. 日常运维

```bash
systemctl status arbd             # 运行状态
journalctl -u arbd -f             # 实时日志
journalctl -u arbd --since "1 hour ago" | grep -E "action|step|rescue|ALERT"
systemctl stop arbd               # 停止（收到 SIGTERM 会先保存步数状态）
cat /opt/dex-arbitr/state/*.json  # 当前持有的步
```

- 均值需先采样 60 分钟、且 σ 预热完成后才开始开仓（日志出现 `mean ready -> RUNNING`；σ 未就绪时日志出现 `trading blocked until short-horizon volatility is ready`）。
- 实盘启动时先核对两边持仓；只有两边查询成功、相互对冲且分别与本地步骤记录一致，才允许开平仓。查询失败或持仓不一致时保持 `PAUSED`，处理后重启重新核对；均值就绪不会解除暂停。
- 限制日志占用：`/etc/systemd/journald.conf` 中设 `SystemMaxUse=500M`，然后 `systemctl restart systemd-journald`。

## 更新流程（root）

```bash
[ "$(id -u)" -eq 0 ] || echo "请先执行 sudo -i 切换到 root"
cd /opt/dex-arbitr/app
git fetch && git log --oneline HEAD..origin/main      # 查看有哪些更新
git pull --ff-only
go test ./...                                         # 测试不过就不要继续
go build -o /opt/dex-arbitr/bin/arbd.new ./cmd/arbd
go build -o /opt/dex-arbitr/bin/selfcheck ./cmd/selfcheck
set -a; source /opt/dex-arbitr/arb.env; set +a
/opt/dex-arbitr/bin/selfcheck -config /opt/dex-arbitr/config.yaml      # 只读自检
diff /opt/dex-arbitr/app/configs/config.yaml /opt/dex-arbitr/config.yaml   # 检查是否有新增配置项需要同步
mv /opt/dex-arbitr/bin/arbd /opt/dex-arbitr/bin/arbd.prev
mv /opt/dex-arbitr/bin/arbd.new /opt/dex-arbitr/bin/arbd
systemctl restart arbd && journalctl -u arbd -f
```

- 已结算步骤与按 UTC 日滚动的量/盈亏审计记账保存在 `/opt/dex-arbitr/state`，重启后加载并核对两边实际仓位；开启 `monitor.analytics` 时交易事件 JSONL 追加写入 `/opt/dex-arbitr/state/trades/trades.jsonl`，可用 `cmd/analytics` 离线统计。`Unresolved=true` 的状态不会自动恢复下单，需先核实交易所订单终态及持仓；详见 [恢复边界](docs/trading-safety-fixes.md#重启升级和持仓恢复边界)。
- 建议确认全部订单和补救已结算后更新；仅日志暂时没有 `rescue order` 不足以证明不存在未决订单。

**回滚：**

```bash
mv /opt/dex-arbitr/bin/arbd.prev /opt/dex-arbitr/bin/arbd
systemctl restart arbd
```

如需同时回滚代码：`cd /opt/dex-arbitr/app && git checkout <上一个提交号>` 后重新编译。

## 常见问题

**`cp: cannot stat '/opt/dex-arbitr/app/configs/config.yaml': No such file or directory`**

代码没有克隆到 `/opt/dex-arbitr/app`：

```bash
whoami                                 # 必须是 root，否则 sudo -i
ls -la /opt/dex-arbitr                 # 看是否有 app 目录
find / -name config.yaml -path "*dex-arbitr*" 2>/dev/null   # 是否克隆到了别处
```

- 没有 `app` 目录：重新执行 `git clone https://github.com/Dog-Feng/dex-arbitr.git /opt/dex-arbitr/app`，注意看有无报错。
- 目录存在但内容不全：`rm -rf /opt/dex-arbitr/app` 后重新克隆。
- `git: command not found`：`apt-get install -y git`。

**之前按旧文档创建过 `arb` 用户**（可选清理）：

```bash
systemctl stop arbd 2>/dev/null
userdel arb 2>/dev/null
chown -R root:root /opt/dex-arbitr
```

然后按本文档从第 2 步开始（已有代码可直接 `cd /opt/dex-arbitr/app && git pull`）。
