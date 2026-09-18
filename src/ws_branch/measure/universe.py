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

FOREIGN_BROKER_COHORT_VERSION: Final = "foreign_seat_v1"
"""外資券商席位名單 v1(2026-09-18,以 broker **代號**宣告)。

12 家外資券商在台分點,代號與名稱 2026 年內一對一穩定(已查證)。

與 `identity.classify_broker_cohort` 的 `institutional` 桶**不同**:後者的
規則含「名字帶法人」,會把本土券商的法人部席位(國票-敦北法人,代號 7790)
一起算進來——做外資覆蓋率會高估,故本名單獨立宣告,不從名字規則推導。

有效期:2026(建立時的觀測窗)。跨年使用前須重新查證代號是否易主/更名——
2026 年已有台新/元富合併造成 51 個代號換名的先例(本名單 12 家不受影響)。
"""

FOREIGN_BROKER_CODES: Final[frozenset[str]] = frozenset({
    "8440",  # 摩根大通
    "1480",  # 美商高盛
    "1470",  # 台灣摩根士丹利
    "1650",  # 新加坡商瑞銀
    "1440",  # 美林
    "1590",  # 花旗環球
    "8960",  # 香港上海匯豐
    "8900",  # 法銀巴黎
    "8890",  # 大和國泰
    "6010",  # 犇亞證券
    "9200",  # 港商野村
    "9100",  # 港商麥格理
})


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
