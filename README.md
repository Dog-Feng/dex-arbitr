# dex-arbitr

Lighter 主网 ↔ Lighter Robinhood 永续价差监控（P1）。默认 `monitor_only: true`，扫描两边交集，用毛价差 ≥ 0.1% 发现机会（对齐参考监控 V2），不下单、不改格子。

## 文档

| 文档 | 内容 |
|------|------|
| [docs/项目说明.md](docs/项目说明.md) | **当前实现**：流程、公式、配置与面板字段 |
| [docs/价差扫描与发现.md](docs/价差扫描与发现.md) | 复刻参考监控 V2：交集扫描、毛价差 Top-N |
| [docs/部署文档.md](docs/部署文档.md) | Windows / Linux 安装、配置、启动 |
| [docs/套利流程对比.md](docs/套利流程对比.md) | 本项目 vs `crypto-trading-open-main` |
| [docs/开发设计文档.md](docs/开发设计文档.md) | 早期规划（部分已过时，以项目说明为准） |

## 快速开始

1. 复制 `config/venues/lighter.example.yaml` → `lighter.yaml`，Robinhood 同理，填入手钥。
2. 保持 `monitor_only: true`、`scan.enabled: true`。`whitelist: []` 扫两边交集。
3. 仓库根目录编译并直接运行 exe / 二进制（Windows 不要 `cargo run` 看面板）。

详见[部署文档](docs/部署文档.md)。私钥不要提交 git。
