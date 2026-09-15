"""T1 主表轉換:分點×股票×日(canonical 聚合)。

純轉換薄層:三個資料陷阱(單位股/千分位逗號/dash coalesce)已封在
ws-core `broker_tx_daily_scan`,此處只負責年窗切割。
"""

from __future__ import annotations

import polars as pl
from ws_core import broker_tx_daily_scan


def build_year(year: int) -> pl.LazyFrame:
    return broker_tx_daily_scan(start=f"{year}-01-01", end=f"{year}-12-31")
