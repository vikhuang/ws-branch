"""T4 特徵表:分點×日行為特徵(自 T1 聚合;分群重做/身分歸因的原料)。

特徵(金額口徑,dollar;股數版特徵留待需求):
- gross_buy/sell_amt, net_amt, gross_amt
- directional_ratio = |buy−sell| / (buy+sell)  ∈[0,1](散戶混合≈0,單向資金≈1)
- n_symbols(當日碰幾檔)
- top1_share / top5_share(火力集中度:最重倉 1/5 檔佔自身 gross 比)

記憶體紀律:逐月切塊(group 鍵含 date,月切無損)——單年單程序全量 group
含 list 排序會重演 30GB 壓縮頁事故。
"""

from __future__ import annotations

import polars as pl

from ws_branch.tables import io


def compute_broker_day(t1_slice: pl.DataFrame) -> pl.DataFrame:
    """純計算:T1 切片(分點×股票×日)→ 分點×日特徵。可用合成資料單測。"""
    g = t1_slice.with_columns(
        (pl.col("buy_dollar") + pl.col("sell_dollar")).alias("gross"))
    return (g.group_by("broker", "broker_name", "date")
            .agg(
                pl.col("buy_dollar").sum().alias("gross_buy_amt"),
                pl.col("sell_dollar").sum().alias("gross_sell_amt"),
                pl.len().alias("n_symbols"),
                pl.col("gross").sum().alias("gross_amt"),
                pl.col("gross").max().alias("_top1"),
                pl.col("gross").sort(descending=True).head(5).sum().alias("_top5"),
            )
            .with_columns(
                (pl.col("gross_buy_amt") - pl.col("gross_sell_amt")).alias("net_amt"),
                ((pl.col("gross_buy_amt") - pl.col("gross_sell_amt")).abs()
                 / (pl.col("gross_buy_amt") + pl.col("gross_sell_amt"))
                 ).fill_nan(None).alias("directional_ratio"),
                (pl.col("_top1") / pl.col("gross_amt")).fill_nan(None)
                .alias("top1_share"),
                (pl.col("_top5") / pl.col("gross_amt")).fill_nan(None)
                .alias("top5_share"),
            )
            .drop("_top1", "_top5"))


def build_year(year: int) -> pl.LazyFrame:
    parts: list[pl.DataFrame] = []
    for m in range(1, 13):
        nxt_y, nxt_m = (year + 1, 1) if m == 12 else (year, m + 1)
        sl = (pl.scan_parquet(io.year_path("t1_broker_daily", year))
              .filter((pl.col("date") >= pl.date(year, m, 1))
                      & (pl.col("date") < pl.date(nxt_y, nxt_m, 1)))
              .select("broker", "broker_name", "date",
                      "buy_dollar", "sell_dollar")
              .collect())
        if sl.height:
            parts.append(compute_broker_day(sl))
    return pl.concat(parts).lazy()
