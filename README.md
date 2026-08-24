# dex-arbitr

Lighter 主网 ↔ Lighter Robinhood ↔ SoDEX 永续价差监控与套利（P1 + P1.5 + 实盘闭环）。默认 `monitor_only: true`、`scan.enabled: true` 三所两两扫描；可选 `execution.enabled: true` 走格子 + 定仓 + 执行（paper 或经 Go `exchange_sidecar` 实盘）。详见 [docs/实盘验证.md](docs/实盘验证.md)。

## 文档

| 文档 | 内容 |
|------|------|
| [docs/项目说明.md](docs/项目说明.md) | **当前实现**：流程、公式、配置与代币日志字段 |
| [docs/价差扫描与发现.md](docs/价差扫描与发现.md) | 复刻参考监控 V2：交集扫描、毛价差 Top-N |
| [docs/部署文档.md](docs/部署文档.md) | Windows / Linux 安装、配置、启动 |
| [docs/套利流程对比.md](docs/套利流程对比.md) | 本项目 vs `crypto-trading-open-main` |
| [docs/实盘验证.md](docs/实盘验证.md) | **小额实盘**：exchange_sidecar（Go）+ live-test CLI |
| [docs/参考项目套利流程.md](docs/参考项目套利流程.md) | **参考项目**：检测、决策、执行、仓位控制全流程 |
| [docs/开发设计文档.md](docs/开发设计文档.md) | 早期规划（部分已过时，以项目说明为准） |

## 快速开始

1. 复制 `config/venues/lighter.example.yaml` → `lighter.yaml`，Robinhood 同理，填入手钥。
2. 保持 `monitor_only: true`、`scan.enabled: true`。`whitelist: []` 扫三所两两交集。SoDEX 模板：`config/venues/sodex.example.yaml`。
3. 若要联调决策/定仓/paper 持仓（仍不发单）：`execution.enabled: true`、`scan.enabled: false`、`monitor_only: false`、`paper_trading: true`，并设 `sizing.fallback_available_usdc`。
4. 小额实盘：构建 `scripts/exchange_sidecar` 后 `cargo run --release --bin live-test -- account lighter`，再设 `paper_trading: false`。页面：`http://127.0.0.1:8090/`。
5. 仓库根目录编译并运行。日志在控制台和 `data/logs/`（扫描模式同一套代币行）。跨 DEX 的 `nat` 在 `data/spreads.sqlite`，重启直接用，默认 30 分钟重算。
6. 拷回本机后可统计：`python scripts/analyze_scan_log.py dex-arbitr.log.2026-08-24`

详见[部署文档](docs/部署文档.md)。私钥不要提交 git。
