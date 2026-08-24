# exchange_sidecar

统一 **Lighter**（`lighter-go`）+ **SoDEX**（官方 Go SDK）实盘层，对齐 `internal/exchange/`。

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
{"cmd":"account|place|cancel|order_status","venue_yaml":"config/venues/lighter.yaml","params":{...}}
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

旧组件 `scripts/exchange_bridge.py`、`scripts/sodex_bridge` 已弃用，请只用本 sidecar。
