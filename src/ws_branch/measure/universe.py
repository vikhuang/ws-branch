"""股票 universe 與 cohort 的宣告與解析(純函數,零 IO)。

v3 架構文件(ws-quant `docs/actor_layer_v3_architecture_alignment_2026-09-18.md`)
§4.1/§5.3:**股票 universe 是第一個聚合 gate**,涵蓋 T3-only 總量、市場向量、
actor 向量、集中度與會計表——不能只靠 T1 inner join 順便擋掉。

實證(2026-09-14,全市場):T3 的「指數」型代號(IX*/MSCI 等)帶 12,765 億
外資買入金額,佔 T3 當日外資買入 82%,而普通股只有 2,734 億。任何 T3-only
的加總不套 universe,數字會差五倍。

universe 來源 = TEJ `stock_attr`(**逐日**,有 `stktp_c` 證券類型與 `mkt_bd_c`
市場別),不是 tickers 快照——後者只有 active 標記,無下市日,歷史回溯會有
倖存者偏誤。stock_attr 本身不含興櫃,與 T3 的覆蓋一致(T3 亦無興櫃列)。
"""

from __future__ import annotations

from typing import Final

import polars as pl

# cohort 宣告已切到 measure/cohort.py(SRP 2026-09-22);此處 re-export 維持既有匯入點
from ws_branch.measure.cohort import (  # noqa: F401
    FOREIGN_BROKER_CODES, FOREIGN_BROKER_COHORT_VERSION, FOREIGN_BROKER_NAMES, FOREIGN_COHORT,
    HISTORICAL_FOREIGN_CODES, KNOWN_HIDDEN_FOREIGN, CohortMember, assert_cohort_names,
    cohort_codes, cohort_expr, cohort_members_in_year, seat_class,
)

UNIVERSE_VERSION: Final = "stock_v1"
"""普通股 universe v1(2026-09-18 定案,user 拍板「排興櫃、排所有非股票產品」)。

納入 `stktp_c` ∈ {普通股, 普通股-海外}:
- 普通股-海外(KY 股等在台第一上市外國企業)為一般交易的普通股,三大法人
  資料有覆蓋,納入(2026-09-14:119 檔,佔 T1 金額 3.8%)。
- 排除:指數(非可交易證券)、ETF、國外 ETF、特別股、台灣存託憑證、REIT。
  合計佔 T1 當日金額 7.6%,佔 T3 外資買入 82%(幾乎全是指數彙總列)。
- 市場別 `mkt_bd_c` 全納(上市一般板/上櫃一般板/創新板);創新板 2026-09-14
  僅 29 檔、48 億(0.3%),交易制度受限但仍是普通股,排除與否對結論無影響,
  故不另設規則——若之後要排,在此加 board 條件並升版本號。
- 興櫃:stock_attr 本身即無,與 T3 一致;不需另外排除規則。
"""

STOCK_TYPES: Final[frozenset[str]] = frozenset({"普通股", "普通股-海外"})

def stock_universe(stock_attr_slice: pl.DataFrame) -> pl.DataFrame:
    """stock_attr 切片 → 逐日 universe(symbol_id, date)。

    stock_attr_slice 需含 `coid`, `mdate`, `stktp_c`。回傳去重的股票日清單,
    供所有聚合入口 semi-join。
    """
    return (
        stock_attr_slice.select(
            pl.col("coid").alias("symbol_id"),
            pl.col("mdate").cast(pl.Date).alias("date"),
            "stktp_c",
        )
        .filter(pl.col("stktp_c").is_in(STOCK_TYPES))
        .select("symbol_id", "date")
        .unique()
    )


def apply_universe(df: pl.DataFrame, universe: pl.DataFrame) -> pl.DataFrame:
    """把 universe gate 套到任何帶 (symbol_id, date) 的表上(semi-join)。

    刻意用 semi-join 而非 filter+is_in:universe 是**逐日**的,同一檔股票可能
    今天在、下市後不在,用單一集合會靜默用錯日期的成分。
    """
    return df.join(universe, on=["symbol_id", "date"], how="semi")


def universe_exclusion_report(
    df: pl.DataFrame, universe: pl.DataFrame, amount_col: str
) -> pl.DataFrame:
    """被 universe 擋掉的東西要看得見(家法#3:不得靜默丟棄)。

    回傳被排除列的筆數與金額,呼叫端負責印出或寫入 manifest。
    """
    excluded = df.join(universe, on=["symbol_id", "date"], how="anti")
    return excluded.select(
        pl.len().alias("excluded_rows"),
        pl.col("symbol_id").n_unique().alias("excluded_symbols"),
        pl.col(amount_col).sum().alias("excluded_amount"),
    )
