"""表的宣告(資料即設定):加新表 = 在此加一筆,runner 一行不改。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import polars as pl

from ws_branch.tables.transforms import t1_broker_daily

FIRST_YEAR = 2021


@dataclass(frozen=True)
class Table:
    name: str
    build_year: Callable[[int], pl.LazyFrame]  # 純轉換:年 → LazyFrame
    verify: Callable[[int, int], None]         # (n_samples, seed) → raise on FAIL
    first_year: int = FIRST_YEAR
    date_col: str = "date"


def _verify_t1(n_samples: int, seed: int) -> None:
    """T1 對帳:跨年抽樣,總量 vs TEJ vol 閉環(>0.1% 即 FAIL)。"""
    import random

    from ws_core import prices

    from ws_branch.tables import io
    from ws_branch.tables.checks import reconcile_vol

    rng = random.Random(seed)
    pairs = (io.scan("t1_broker_daily").select("symbol_id", "date").unique()
             .collect().sample(min(n_samples * 20, 10_000), seed=seed)
             .with_columns(pl.col("date").dt.year().alias("y")))
    chosen = []
    per_year = max(n_samples // max(pairs["y"].n_unique(), 1), 5)
    for y in pairs["y"].unique().to_list():
        sub = pairs.filter(pl.col("y") == y)
        chosen.append(sub.sample(min(per_year, sub.height),
                                 seed=rng.randint(0, 9999)))
    sample = pl.concat(chosen)
    totals = (io.scan("t1_broker_daily")
              .join(sample.lazy().select("symbol_id", "date"),
                    on=["symbol_id", "date"], how="inner")
              .group_by("symbol_id", "date")
              .agg(pl.col("buy_sh").sum().alias("buy"),
                   pl.col("sell_sh").sum().alias("sell"))
              .collect())
    px = (prices(coids=totals["symbol_id"].unique().to_list(),
                 start=str(totals["date"].min()), end=str(totals["date"].max()),
                 columns=["coid", "mdate", "vol"])
          .rename({"coid": "symbol_id", "mdate": "date"})
          .with_columns(pl.col("date").cast(pl.Date)))
    bad = reconcile_vol(totals, px)
    n_joined = totals.join(px, on=["symbol_id", "date"], how="inner").height
    print(f"verify[t1_broker_daily]: 抽樣 {n_joined} 股日"
          f"(跨 {totals['date'].dt.year().n_unique()} 年)")
    if bad.height:
        print(bad.head(10))
        raise SystemExit(f"FAIL: {bad.height} 股日對不平 TEJ vol")
    print("PASS: T1 與 TEJ vol 閉環")


TABLES: dict[str, Table] = {
    "t1_broker_daily": Table(
        name="t1_broker_daily",
        build_year=t1_broker_daily.build_year,
        verify=_verify_t1,
    ),
    # T3 官方對齊表 / T4 特徵表:P3 於此登記
}
