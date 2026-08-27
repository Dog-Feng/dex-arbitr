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

## fill_pnl

平仓后读该所已实现盈亏。params：`{symbol, order_id}`。返回 `{realized_pnl, per_fill, found}`。

| 所 | 来源 | per_fill |
|---|---|---|
| Entropy | 成交 `closedPnl` | true |
| Lighter | 成交 `ask_account_pnl` / `bid_account_pnl`（按 client_order_index） | true |
| SoDEX | 持仓累计 `realizedPnL` | false |

盘口订阅没有盈亏字段。`found=false` 时上层显示 `—`，不要填 0。

旧组件 `scripts/exchange_bridge.py` 已弃用，请只用本 sidecar。
