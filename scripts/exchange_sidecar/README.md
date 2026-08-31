# exchange_sidecar

统一 **Lighter**（`lighter-go`）+ **SoDEX**（官方 Go SDK）+ **Entropy**（Hyperliquid HIP-3）实盘层。

Rust 主进程 `dex-arbitr` 通过 stdin JSON 调用本二进制，**无需 Python**。

## 构建

```bash
cd scripts/exchange_sidecar
go mod tidy
go build -o exchange_sidecar .    # Linux
go build -o exchange_sidecar.exe . # Windows
```

## 协议

与旧 `exchange_bridge.py` 相同：

```json
{"cmd":"account|place|cancel|order_status|funding|watch|fill_pnl","venue_yaml":"config/venues/lighter.yaml","params":{...}}
```

stdout：

```json
{"ok":true,"data":{...},"error":""}
```

## 环境变量

| 变量 | 说明 |
|------|------|
| `DEX_EXCHANGE_SIDECAR` | Rust 侧指定 sidecar 绝对路径（可选） |
| `SODEX_ACCOUNT_ADDRESS` | SoDEX 主钱包地址（venue yaml 未填时） |

## 部署

与 `dex-arbitr` 同机、同仓库根目录运行；Rust 默认查找 `scripts/exchange_sidecar/exchange_sidecar`。

`place` 会在 stderr 打一行链路耗时（Rust 以 info 转发，并填配置页「签名→确认」）。Lighter / SoDEX / Entropy 同一格式：

```
{venue} place rtt venue=… order=… sign_ms=… send_ms=… sign_to_ack_ms=… result=ok
```

`sign` = 本地签名，`send` = HTTP 收到所方收单回包，合计不含拉 nonce、等锁、IOC 成交回查。JSON 同时带 `sign_ms` / `send_ms` / `sign_to_ack_ms`。SoDEX 的 `sign_ms` 是 `PlacePerpsOrder` 合计减 HTTP（SDK 未导出签名）。全链路墙钟由 Rust `bridge_place` 另计（含认成交）。

## fill_pnl

平仓后读该所已实现盈亏。params：`{symbol, order_id}`。返回 `{realized_pnl, per_fill, found}`。

| 所 | 来源 | per_fill |
|---|---|---|
| Entropy | 成交 `closedPnl` | true |
| Lighter | 成交 `ask_account_pnl` / `bid_account_pnl`（按 client_order_index） | true |
| SoDEX | 持仓累计 `realizedPnL` | false |

盘口订阅没有盈亏字段。`found=false` 时上层显示 `—`，不要填 0。

旧组件 `scripts/exchange_bridge.py` 已弃用，请只用本 sidecar。
