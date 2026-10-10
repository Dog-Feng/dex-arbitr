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
