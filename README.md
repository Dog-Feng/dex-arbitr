# dex-arbitr

Lighter 主网 ↔ Lighter Robinhood ↔ SoDEX ↔ Entropy 永续价差套利。主逻辑是分段网格；`execution.enabled: true` 走统一决策环（定仓 + 先挂后吃）。`paper_trading: false` 且 `monitor_only: false` 时经 Go `exchange_sidecar` 实盘。当前默认 `execution` / `scan` 都关，走事件格子环。详见 [docs/项目说明.md](docs/项目说明.md)。

## 文档

| 文档 | 内容 |
|------|------|
| [docs/本系统套利流程.md](docs/本系统套利流程.md) | 本仓库：启动、三条环、格子、定仓、执行、滤网 |
| [docs/参考项目套利流程.md](docs/参考项目套利流程.md) | `crypto-trading-open-main`：监控 V2 / 分段网格 / V3 |
| [docs/参考项目V3套利流程.md](docs/参考项目V3套利流程.md) | 参考 V3 基础模式源码：价差/费率开仓、三点全平 |
| [web/参考项目.html](web/参考项目.html) | 参考终端监控布局原型（Rich Live 三种模式） |
| [docs/套利流程对比.md](docs/套利流程对比.md) | 逐项有/没有、刻意不同 |
| [docs/项目说明.md](docs/项目说明.md) | 总览、公式、配置表、控制台字段 |
| [docs/配置参考.md](docs/配置参考.md) | `config/default.yaml` 字段 |
| [docs/新增DEX对齐清单.md](docs/新增DEX对齐清单.md) | 接入新所的契约 |
| [docs/部署文档.md](docs/部署文档.md) | Windows / Linux 安装启动 |
| [docs/实盘验证.md](docs/实盘验证.md) | 小额实盘：sidecar + live-test |
| [docs/平仓判据备选方案B.md](docs/平仓判据备选方案B.md) | 格子回落 vs 往返净利止盈（当前用格子回落） |

## 快速开始

1. 复制 `config/venues/lighter.example.yaml` → `lighter.yaml`，Robinhood 同理，填入手钥。
2. 当前默认 `execution.enabled: false`、`scan.enabled: false`。只扫毛价差时打开 `scan.enabled`；跑套利打开 `execution.enabled`。`pairs.whitelist: []` 扫各所两两交集。模板：`config/venues/*.example.yaml`（含 `entropy.example.yaml`）。
3. 联调决策/定仓/paper 持仓（仍不发单）：`execution.enabled: true`、`scan.enabled: false`、`monitor_only: false`、`paper_trading: true`，并设 `sizing.fallback_available_usdc`。
4. 小额实盘：构建 `scripts/exchange_sidecar` 后 `cargo run --release --bin live-test -- account lighter`，再设 `paper_trading: false`。页面必须打开 **`http://127.0.0.1:8090/`**（不要直接打开 html）。
5. 仓库根目录编译并运行。日志在控制台和 `data/logs/`。跨 DEX 的 `nat` 在 `data/spreads.sqlite`，重启直接用，默认 30 分钟重算。
6. 拷回本机后可统计：`python scripts/analyze_scan_log.py dex-arbitr.log.2026-08-24`

详见[部署文档](docs/部署文档.md)。私钥不要提交 git。
