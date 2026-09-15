"""L1 資料廠:建造 T1 主表(分點×股票×日,2021+ 全史)並對帳。

設計(docs/REDESIGN_2026-09.md §3):
- T1 = 物化表 data/t1_broker_daily/year=YYYY.parquet(canonical 聚合,
  來源 = ws_core.broker_tx_daily_scan,三陷阱已在 ws-core 封裝)
- T2(價位表)不物化——ws_core.broker_tx_pricelevel_scan 即為 T2 的
  lazy 視圖,物化只會複製 9GB 生資料;一致性由 ws-core 對帳測試把關
- 每年一檔,逐年 streaming sink 控記憶體

用法:
  uv run python factory.py build              # 全史(缺哪年建哪年)
  uv run python factory.py build --year 2026  # 重建單年
  uv run python factory.py verify             # 對帳:抽樣 vs TEJ vol
"""

from __future__ import annotations

import argparse
import datetime
import random
from pathlib import Path

import polars as pl

from ws_core import broker_tx_daily_scan, prices

T1_DIR = Path(__file__).resolve().parent / "data" / "t1_broker_daily"
FIRST_YEAR = 2021


def build(year: int | None = None, force: bool = False) -> None:
    T1_DIR.mkdir(parents=True, exist_ok=True)
    this_year = datetime.date.today().year
    years = [year] if year else list(range(FIRST_YEAR, this_year + 1))
    for y in years:
        out = T1_DIR / f"year={y}.parquet"
        if out.exists() and not force and y != this_year:
            print(f"{y}: exists, skip(--force 重建)")
            continue
        lf = broker_tx_daily_scan(start=f"{y}-01-01", end=f"{y}-12-31")
        lf.sink_parquet(out, compression="zstd")
        n = pl.scan_parquet(out).select(pl.len()).collect()[0, 0]
        days = (pl.scan_parquet(out).select(pl.col("date").n_unique())
                .collect()[0, 0])
        print(f"{y}: {n:,} rows, {days} 交易日 → {out.name}", flush=True)


def scan_t1(*, start: str | None = None, end: str | None = None) -> pl.LazyFrame:
    """讀 T1(下游入口)。"""
    lf = pl.scan_parquet(T1_DIR / "*.parquet")
    if start:
        lf = lf.filter(pl.col("date") >= datetime.date.fromisoformat(start))
    if end:
        lf = lf.filter(pl.col("date") <= datetime.date.fromisoformat(end))
    return lf


def verify(n_samples: int = 60, seed: int = 20260915) -> None:
    """對帳:隨機抽 n 個(股票,日),T1 總買進(股)/1000 須 == TEJ vol(千股)。

    抽樣跨全史各年;>0.1% 相對差即 FAIL(2026-09 三股日先例為分毫不差)。
    """
    rng = random.Random(seed)
    pairs = (scan_t1().select("symbol_id", "date").unique()
             .collect().sample(min(n_samples * 20, 10_000), seed=seed))
    # 分層:每年抽 n/年數
    pairs = pairs.with_columns(pl.col("date").dt.year().alias("y"))
    chosen = []
    per_year = max(n_samples // pairs["y"].n_unique(), 5)
    for y in pairs["y"].unique().to_list():
        sub = pairs.filter(pl.col("y") == y)
        chosen.append(sub.sample(min(per_year, sub.height), seed=rng.randint(0, 9999)))
    sample = pl.concat(chosen)
    t1 = (scan_t1().join(sample.lazy().select("symbol_id", "date"),
                         on=["symbol_id", "date"], how="inner")
          .group_by("symbol_id", "date")
          .agg(pl.col("buy_sh").sum().alias("buy"), pl.col("sell_sh").sum().alias("sell"))
          .collect())
    px = (prices(coids=t1["symbol_id"].unique().to_list(),
                 start=str(t1["date"].min()), end=str(t1["date"].max()),
                 columns=["coid", "mdate", "vol"])
          .rename({"coid": "symbol_id", "mdate": "date"})
          .with_columns(pl.col("date").cast(pl.Date)))
    j = t1.join(px, on=["symbol_id", "date"], how="inner").filter(pl.col("vol") > 0)
    rel_b = ((j["buy"] / 1000 - j["vol"]).abs() / j["vol"])
    rel_s = ((j["sell"] / 1000 - j["vol"]).abs() / j["vol"])
    bad = j.filter((rel_b > 0.001) | (rel_s > 0.001))
    print(f"verify: 抽樣 {j.height} 股日(跨 {j['date'].dt.year().n_unique()} 年), "
          f"買方最大相對差 {rel_b.max():.5%}, 賣方 {rel_s.max():.5%}")
    if bad.height:
        print(bad.head(10))
        raise SystemExit(f"FAIL: {bad.height} 股日對不平 TEJ vol")
    print("PASS: T1 與 TEJ vol 閉環")


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("--year", type=int)
    b.add_argument("--force", action="store_true")
    v = sub.add_parser("verify")
    v.add_argument("--n", type=int, default=60)
    args = ap.parse_args()
    if args.cmd == "build":
        build(args.year, args.force)
    else:
        verify(args.n)


if __name__ == "__main__":
    main()
