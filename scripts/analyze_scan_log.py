#!/usr/bin/env python3
"""统计 dex-arbitr 扫描日志：每个币 / 每个方向的上榜次数、价差、持续时间。

用法（仓库根目录或任意路径）：
  python scripts/analyze_scan_log.py dex-arbitr.log.2026-08-24
  python scripts/analyze_scan_log.py data/logs/dex-arbitr.log.2026-08-24 --top 20 --csv out.csv
"""

from __future__ import annotations

import argparse
import csv
import re
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

ANSI = re.compile(r"\x1b\[[0-9;]*m")
TS = re.compile(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")
KV = re.compile(r"(\w+)=(\S+)")
PCT = re.compile(r"([+-]?\d+(?:\.\d+)?)%")


def strip_ansi(text: str) -> str:
    return ANSI.sub("", text)


def parse_pct(value: str | None) -> float | None:
    if not value or value == "-":
        return None
    m = PCT.fullmatch(value)
    return float(m.group(1)) if m else None


def parse_age(value: str | None) -> float | None:
    if not value:
        return None
    if value.endswith("s"):
        value = value[:-1]
    try:
        return float(value)
    except ValueError:
        return None


def parse_ts(line: str) -> datetime | None:
    m = TS.search(line)
    if not m:
        return None
    return datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")


@dataclass
class Hit:
    ts: datetime | None
    pair: str
    buy: str
    sell: str
    raw: float | None
    nat: float | None
    res: float | None
    age: float | None
    gone: bool


@dataclass
class DirectionStat:
    pair: str
    buy: str
    sell: str
    hits: int = 0
    gones: int = 0
    episodes: int = 0
    raws: list[float] = field(default_factory=list)
    ress: list[float] = field(default_factory=list)
    nats: list[float] = field(default_factory=list)
    ages: list[float] = field(default_factory=list)
    max_age: float = 0.0
    first_ts: datetime | None = None
    last_ts: datetime | None = None
    _open: bool = False

    def add(self, hit: Hit) -> None:
        if hit.ts:
            self.first_ts = self.first_ts or hit.ts
            self.last_ts = hit.ts
        if hit.gone:
            self.gones += 1
            if self._open:
                self.episodes += 1
            self._open = False
            return
        if not self._open:
            self._open = True
        self.hits += 1
        if hit.raw is not None:
            self.raws.append(hit.raw)
        if hit.res is not None:
            self.ress.append(hit.res)
        if hit.nat is not None:
            self.nats.append(hit.nat)
        if hit.age is not None:
            self.ages.append(hit.age)
            self.max_age = max(self.max_age, hit.age)

    def close_open(self) -> None:
        if self._open:
            self.episodes += 1
            self._open = False

    @property
    def key(self) -> str:
        return f"{self.pair} {self.buy}->{self.sell}"

    @property
    def route(self) -> str:
        return f"{self.buy}->{self.sell}"

    @property
    def max_raw(self) -> float:
        return max(self.raws) if self.raws else 0.0

    @property
    def max_res(self) -> float:
        return max(self.ress) if self.ress else 0.0

    @property
    def median_raw(self) -> float:
        return statistics.median(self.raws) if self.raws else 0.0

    @property
    def flash_ratio(self) -> float:
        if not self.ages:
            return 0.0
        flashes = sum(1 for a in self.ages if a < 1.0)
        return flashes / len(self.ages)


def parse_file(path: Path) -> tuple[list[Hit], dict[str, int], datetime | None, datetime | None]:
    hits: list[Hit] = []
    extras: dict[str, int] = defaultdict(int)
    first_ts = last_ts = None
    with path.open(encoding="utf-8", errors="replace") as f:
        for raw_line in f:
            line = strip_ansi(raw_line).strip()
            if not line:
                continue
            ts = parse_ts(line)
            if ts:
                first_ts = first_ts or ts
                last_ts = ts
            if "pair=" not in line:
                if "subscribed" in line:
                    extras["ws_resub"] += 1
                elif "P1 start" in line:
                    extras["starts"] += 1
                continue
            fields = dict(KV.findall(line.split("INFO", 1)[-1]))
            pair = fields.get("pair")
            buy = fields.get("buy")
            sell = fields.get("sell")
            if not pair or not buy or not sell:
                continue
            gone = line.rstrip().endswith("gone") or fields.get("gone") is not None
            hits.append(
                Hit(
                    ts=ts,
                    pair=pair,
                    buy=buy,
                    sell=sell,
                    raw=parse_pct(fields.get("raw")),
                    nat=parse_pct(fields.get("nat")),
                    res=parse_pct(fields.get("res")),
                    age=parse_age(fields.get("age")),
                    gone=gone,
                )
            )
    return hits, extras, first_ts, last_ts


def summarize(hits: list[Hit]) -> dict[str, DirectionStat]:
    stats: dict[str, DirectionStat] = {}
    for hit in hits:
        key = f"{hit.pair}|{hit.buy}|{hit.sell}"
        st = stats.get(key)
        if st is None:
            st = DirectionStat(pair=hit.pair, buy=hit.buy, sell=hit.sell)
            stats[key] = st
        st.add(hit)
    for st in stats.values():
        st.close_open()
    return stats


def fmt_pct(v: float) -> str:
    return f"{v:+.4f}%"


def print_table(rows: list[tuple], headers: list[str], widths: list[int]) -> None:
    header = "  ".join(h.ljust(w) for h, w in zip(headers, widths))
    print(header)
    print("-" * len(header))
    for row in rows:
        cells = []
        for i, cell in enumerate(row):
            text = str(cell)
            cells.append(text.ljust(widths[i]) if i == 0 else text.rjust(widths[i]))
        print("  ".join(cells))


def configure_stdout() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass


def main() -> int:
    configure_stdout()
    parser = argparse.ArgumentParser(description="统计 dex-arbitr 扫描日志")
    parser.add_argument("log", nargs="+", help="日志文件，可多个")
    parser.add_argument("--top", type=int, default=15, help="每个榜单显示几行")
    parser.add_argument("--csv", help="把方向明细写到 CSV")
    args = parser.parse_args()

    all_hits: list[Hit] = []
    extras: dict[str, int] = defaultdict(int)
    first_ts = last_ts = None
    for item in args.log:
        path = Path(item)
        if not path.is_file():
            print(f"找不到文件: {path}", file=sys.stderr)
            return 1
        hits, extra, a, b = parse_file(path)
        all_hits.extend(hits)
        for k, v in extra.items():
            extras[k] += v
        if a and (first_ts is None or a < first_ts):
            first_ts = a
        if b and (last_ts is None or b > last_ts):
            last_ts = b

    stats = summarize(all_hits)
    if not stats:
        print("没有解析到 pair= 行")
        return 1

    live = [h for h in all_hits if not h.gone]
    gones = [h for h in all_hits if h.gone]
    pairs = sorted({s.pair for s in stats.values()})
    routes: dict[str, list[DirectionStat]] = defaultdict(list)
    for s in stats.values():
        routes[s.route].append(s)

    span = ""
    if first_ts and last_ts:
        mins = (last_ts - first_ts).total_seconds() / 60
        span = f"{first_ts}  ~  {last_ts}  ({mins:.1f} 分钟)"

    print("扫描日志统计")
    print(f"文件: {', '.join(args.log)}")
    if span:
        print(f"时间: {span}")
    print(
        f"行: 上榜 {len(live)}  下榜 {len(gones)}  币 {len(pairs)}  方向 {len(stats)}"
        f"  WS重订 {extras.get('ws_resub', 0)}  启动 {extras.get('starts', 0)}"
    )
    print()

    print("所对（买->卖）")
    route_rows = []
    for route, items in sorted(routes.items(), key=lambda x: -sum(i.hits for i in x[1])):
        raws = [r for s in items for r in s.raws]
        ages = [a for s in items for a in s.ages]
        route_rows.append(
            (
                route,
                sum(s.hits for s in items),
                sum(s.episodes for s in items),
                fmt_pct(max(raws) if raws else 0),
                f"{max(ages) if ages else 0:.1f}s",
                f"{100 * (sum(1 for a in ages if a < 1) / len(ages) if ages else 0):.0f}%",
            )
        )
    print_table(
        route_rows,
        ["route", "hits", "episodes", "max_raw", "max_age", "flash<1s"],
        [28, 8, 8, 10, 8, 10],
    )
    print()

    n = args.top
    by_raw = sorted(stats.values(), key=lambda s: s.max_raw, reverse=True)[:n]
    print(f"Top {n} 最大毛价差 raw")
    print_table(
        [
            (
                s.key,
                s.hits,
                s.episodes,
                fmt_pct(s.max_raw),
                fmt_pct(s.median_raw),
                fmt_pct(s.max_res),
                f"{s.max_age:.1f}s",
            )
            for s in by_raw
        ],
        ["direction", "hits", "ep", "max_raw", "med_raw", "max_res", "max_age"],
        [42, 6, 5, 10, 10, 10, 8],
    )
    print()

    by_age = sorted(stats.values(), key=lambda s: s.max_age, reverse=True)[:n]
    print(f"Top {n} 最长持续 age")
    print_table(
        [
            (
                s.key,
                s.hits,
                s.episodes,
                f"{s.max_age:.1f}s",
                fmt_pct(s.max_raw),
                f"{100 * s.flash_ratio:.0f}%",
            )
            for s in by_age
        ],
        ["direction", "hits", "ep", "max_age", "max_raw", "flash<1s"],
        [42, 6, 5, 8, 10, 10],
    )
    print()

    by_hits = sorted(stats.values(), key=lambda s: s.hits, reverse=True)[:n]
    print(f"Top {n} 上榜次数（闪得越多越吵，不一定能吃）")
    print_table(
        [
            (
                s.key,
                s.hits,
                s.gones,
                s.episodes,
                fmt_pct(s.max_raw),
                f"{s.max_age:.1f}s",
            )
            for s in by_hits
        ],
        ["direction", "hits", "gone", "ep", "max_raw", "max_age"],
        [42, 6, 6, 5, 10, 8],
    )
    print()

    long_ep = [s for s in stats.values() if s.max_age >= 5]
    print(
        f"age≥5s 的方向: {len(long_ep)} / {len(stats)}"
        f"    age<1s 占比: "
        f"{100 * (sum(1 for h in live if h.age is not None and h.age < 1) / max(len(live), 1)):.0f}%"
    )

    if args.csv:
        out = Path(args.csv)
        with out.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    "pair",
                    "buy",
                    "sell",
                    "hits",
                    "gones",
                    "episodes",
                    "max_raw",
                    "median_raw",
                    "max_res",
                    "max_age",
                    "flash_lt_1s",
                    "first_ts",
                    "last_ts",
                ]
            )
            for s in sorted(stats.values(), key=lambda x: x.max_raw, reverse=True):
                w.writerow(
                    [
                        s.pair,
                        s.buy,
                        s.sell,
                        s.hits,
                        s.gones,
                        s.episodes,
                        f"{s.max_raw:.6f}",
                        f"{s.median_raw:.6f}",
                        f"{s.max_res:.6f}",
                        f"{s.max_age:.2f}",
                        f"{s.flash_ratio:.4f}",
                        s.first_ts or "",
                        s.last_ts or "",
                    ]
                )
        print(f"\nCSV: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
