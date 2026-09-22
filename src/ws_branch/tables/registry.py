"""表的宣告(資料即設定):加新表 = 在此加一筆,runner 一行不改。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import polars as pl

from ws_branch.tables import verify
from ws_branch.tables.transforms import (
    t4_broker_measure,
    t1_broker_daily,
    t3_official_daily,
    t3b_accounting_bounds,
    t4_broker_features,
)

FIRST_YEAR = 2021


@dataclass(frozen=True)
class Table:
    name: str
    build_year: Callable[[int], pl.LazyFrame]  # 純轉換:年 → LazyFrame
    verify: Callable[[int, int], None]         # (n_samples, seed) → raise on FAIL
    first_year: int = FIRST_YEAR
    date_col: str = "date"
    manifest: Callable[[int], dict] | None = None   # §4.5:年 → manifest 內容(runner 寫檔)
    frozen: bool = False   # Step F:退役的現行產出——仍可 build 供重現,但不隨年份更新
    after_sink: Callable[[int], None] | None = None   # 年檔落地後的表專屬收尾(如 T1 清 part)


TABLES: dict[str, Table] = {
    "t1_broker_daily": Table(
        name="t1_broker_daily",
        build_year=t1_broker_daily.build_year,
        verify=verify.verify_t1,
        after_sink=t1_broker_daily.cleanup_parts,
    ),
    "t3_official_daily": Table(
        name="t3_official_daily",
        build_year=t3_official_daily.build_year,
        verify=verify.verify_t3,
        first_year=2016,
    ),
    "t3b_accounting_bounds": Table(
        name="t3b_accounting_bounds",
        build_year=t3b_accounting_bounds.build_year,
        verify=verify.verify_t3b,
        first_year=2021,
    ),
    # T4 v2(rank 版指紋、multiplicity):Step F 退役(docs/STEP_F_INVENTORY_2026-09-21.md)
    # ——舊檔保留、可重建供重現,不原地覆蓋語意;新消費端一律接 t4_broker_measure
    "t4_broker_features": Table(
        name="t4_broker_features",
        build_year=t4_broker_features.build_year,
        verify=verify.verify_t4,
        frozen=True,
    ),
    # T4 v3(Step E):與 v2 並存,不原地覆蓋(§11);manifest 由 runner 寫
    "t4_broker_measure": Table(
        name="t4_broker_measure",
        build_year=t4_broker_measure.build_year,
        verify=verify.verify_t4v3,
        manifest=t4_broker_measure.manifest,
    ),
    # T2 價位表不物化:ws-core broker_tx_pricelevel_scan 即視圖(REDESIGN §3)
    # TDCC/主動 ETF 量小,ws-core 直讀不物化
}
