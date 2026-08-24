#!/usr/bin/env python3
"""已弃用：请改用 scripts/exchange_sidecar（Go 统一 sidecar）。

  cd scripts/exchange_sidecar && go build -o exchange_sidecar .

Rust bridge.rs 不再调用本脚本。
"""
raise SystemExit(
    "exchange_bridge.py 已弃用。请构建 scripts/exchange_sidecar："
    "cd scripts/exchange_sidecar && go build -o exchange_sidecar ."
)
