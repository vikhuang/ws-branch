"""純檢查:對帳邏輯,零 IO(資料由呼叫端注入)。"""

from __future__ import annotations

import polars as pl


def reconcile_vol(t1_totals: pl.DataFrame, px: pl.DataFrame,
                  tol: float = 0.001) -> pl.DataFrame:
    """T1 每股日總買/賣(股)/1000 vs TEJ vol(千股),回傳超差列。

    t1_totals:symbol_id, date, buy, sell;px:symbol_id, date, vol。
    先例:2026-09 三股日與 2,955 萬格交叉比對皆分毫不差,tol 只留容錯餘裕。
    """
    j = t1_totals.join(px, on=["symbol_id", "date"], how="inner").filter(
        pl.col("vol") > 0)
    return j.with_columns(
        ((pl.col("buy") / 1000 - pl.col("vol")).abs() / pl.col("vol")).alias("rel_b"),
        ((pl.col("sell") / 1000 - pl.col("vol")).abs() / pl.col("vol")).alias("rel_s"),
    ).filter((pl.col("rel_b") > tol) | (pl.col("rel_s") > tol))
