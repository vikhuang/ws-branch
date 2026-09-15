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
    """T1 對帳:逐年抽日期→抽股票,總量 vs TEJ vol 閉環(>0.1% 即 FAIL)。

    記憶體紀律:單日 filter 走謂詞下推,峰值 <1GB——前版對全史 unique()
    收集曾膨脹 30GB 壓縮頁(2026-09-15 事故三)。
    """
    import random

    from ws_core import prices

    from ws_branch.tables import io
    from ws_branch.tables.checks import closure_overshoots

    rng = random.Random(seed)
    years = io.existing_years("t1_broker_daily")
    dates_per_year = max(n_samples // (len(years) * 5), 2)
    frames = []
    for y in years:
        ylf = pl.scan_parquet(io.year_path("t1_broker_daily", y))
        all_dates = ylf.select("date").unique().collect()["date"].to_list()
        for d in rng.sample(all_dates, min(dates_per_year, len(all_dates))):
            day = (ylf.filter(pl.col("date") == d)
                   .group_by("symbol_id", "date")
                   .agg(pl.col("buy_sh").sum().alias("buy"),
                        pl.col("sell_sh").sum().alias("sell"))
                   .collect())
            frames.append(day.sample(min(5, day.height),
                                     seed=rng.randint(0, 9999)))
    totals = pl.concat(frames)
    px = (prices(coids=totals["symbol_id"].unique().to_list(),
                 start=str(totals["date"].min()), end=str(totals["date"].max()),
                 columns=["coid", "mdate", "vol"])
          .rename({"coid": "symbol_id", "mdate": "date"})
          .with_columns(pl.col("date").cast(pl.Date)))
    joined = totals.join(px, on=["symbol_id", "date"], how="inner")
    # 逐年覆蓋揭露:本地 TEJ prices 為 2024+ 滾動視窗,舊年份 join 為零
    # 必須明示,不得靜默(家法);2021-2023 閉環需 BigQuery 全量另驗
    cov = (totals.with_columns(pl.col("date").dt.year().alias("y"))
           .group_by("y").len().rename({"len": "sampled"})
           .join(joined.with_columns(pl.col("date").dt.year().alias("y"))
                 .group_by("y").len().rename({"len": "joined"}),
                 on="y", how="left")
           .with_columns(pl.col("joined").fill_null(0)).sort("y"))
    print("verify[t1_broker_daily] 逐年覆蓋(sampled→joined):")
    for r in cov.iter_rows(named=True):
        note = "" if r["joined"] else " ← 本地 TEJ 無此年,未驗(需 BQ)"
        print(f"  {r['y']}: {r['sampled']} → {r['joined']}{note}")
    bad = closure_overshoots(totals, px)
    if bad.height:
        print(bad.head(10))
        raise SystemExit(f"FAIL: {bad.height} 股日 broker 超過 TEJ vol(硬性不變量破裂)")
    under = joined.with_columns(
        (pl.col("vol") - pl.col("buy") / 1000).alias("short_lots"))
    n_under = under.filter(pl.col("short_lots") > 1).height
    print(f"PASS: 零超出(broker ⊆ TEJ 成立);短少>1張 {n_under}/{joined.height} 股日"
          f"(鉅額/特殊交易,監控用)")


TABLES: dict[str, Table] = {
    "t1_broker_daily": Table(
        name="t1_broker_daily",
        build_year=t1_broker_daily.build_year,
        verify=_verify_t1,
    ),
    # T3 官方對齊表 / T4 特徵表:P3 於此登記
}
