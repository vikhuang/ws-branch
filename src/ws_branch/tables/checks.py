"""純檢查:對帳邏輯,零 IO(資料由呼叫端注入)。"""

from __future__ import annotations

import polars as pl


def reconcile_vol(t1_totals: pl.DataFrame, px: pl.DataFrame,
                  tol: float = 0.001, abs_lots: float = 1.0) -> pl.DataFrame:
    """T1 每股日總買/賣(股)/1000 vs TEJ vol(千股),回傳超差列。

    t1_totals:symbol_id, date, buy, sell;px:symbol_id, date, vol。
    容差 = max(vol×tol, abs_lots 張):**broker_tx 含零股、TEJ vol 只計整張**
    (2026-09-15 全史 verify 實證:尾差全部 <1 張,如 119,153 股 vs 119 張),
    小量股票的零股尾數以絕對 1 張容納,非資料錯誤。
    """
    j = t1_totals.join(px, on=["symbol_id", "date"], how="inner").filter(
        pl.col("vol") > 0)
    allow = pl.max_horizontal(pl.col("vol") * tol, pl.lit(abs_lots))
    return j.with_columns(
        (pl.col("buy") / 1000 - pl.col("vol")).abs().alias("diff_b"),
        (pl.col("sell") / 1000 - pl.col("vol")).abs().alias("diff_s"),
    ).filter((pl.col("diff_b") > allow) | (pl.col("diff_s") > allow))


def closure_overshoots(t1_totals: pl.DataFrame, px: pl.DataFrame,
                       abs_lots: float = 1.0) -> pl.DataFrame:
    """硬性不變量:broker 日報 ⊆ TEJ vol——broker 超過 TEJ 即資料錯誤。

    短少不算錯(TEJ vol 含鉅額/特殊交易,broker 日報不含;2026-09-15 六日
    6,600+ 股日實證零超出、短少 0.2-2.6%/日中位 <1.5%),由呼叫端另行
    回報分佈監控結構變化。
    """
    j = t1_totals.join(px, on=["symbol_id", "date"], how="inner").filter(
        pl.col("vol") > 0)
    return j.with_columns(
        (pl.col("buy") / 1000 - pl.col("vol")).alias("over_b"),
        (pl.col("sell") / 1000 - pl.col("vol")).alias("over_s"),
    ).filter((pl.col("over_b") > abs_lots) | (pl.col("over_s") > abs_lots))
