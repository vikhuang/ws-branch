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

FOREIGN_BROKER_COHORT_VERSION: Final = "foreign_seat_v2"
"""外資券商席位名單 v2(2026-09-20,以 broker **代號**宣告)。

11 家外國證券商在台分支機構。名單經**三方對照 + 官方登記檔**驗證:
repo `broker_names.json`(981 筆代號→名稱)、T1 實際資料、TWSE
`證券商基本資料.xls`(838 家在營)與 `證券商結束營業資料.xls`(400 家)。

與 `identity.classify_broker_cohort` 的 `institutional` 桶**不同**,兩處差異:
1. 後者規則含「名字帶法人」,會把本土券商法人部(國票-敦北法人 7790)算進來。
2. 後者的 `_FOREIGN_EXACT` 含**犇亞證券(6010),但它是本土券商**——見下。

**v1 → v2 的兩處修正**:
- 移除犇亞證券(6010)。證據:開業日民國 78 年(1989,早於絕大多數外資
  來台);登記地址台北市復興北路 99 號 3-4 樓(一般辦公室,非金融大樓);
  **擁有兩家零售分行**(犇亞-網路 6012 線上下單、犇亞-鑫豐 601d),且
  601d 係 2018-12-28 併購本土券商鑫豐(8850)而來——外國證券商在台分支
  機構不會併購本土零售券商並當分行經營。對照:其餘 11 家全部零分行,
  地址集中於台北 101(信義路五段 7 號 48/54/72/83 樓)、松智路、敦化南路
  等金融大樓。犇亞僅佔 cohort 量 0.11%,但定義錯誤會污染個股層級讀數。
  **`identity.py` 的 `_FOREIGN_EXACT` 有同一錯誤**(自 ws-quant 凍結規則
  繼承),該檔為多個下游共用,本輪不動,僅在其 docstring 立牌。
- 修正 v1 憑印象填錯的兩碼:9200/9100 實為凱基/群益金鼎(本土券商總公司),
  真代號為 1560(港商野村)、1360(港商麥格理)。

**有效期 = 2026**。跨年重建前必須重查,已知兩筆歷史變動:
- 瑞士信貸(1520)2023 併入新加坡商瑞銀(1650):建 2023 前的表時 1520 要入名單。
- 港商法國興業(1570)已合併退出:早年資料中存在,2026 已無。
- 2026 年另有台新/元富合併造成 51 個代號換名(本名單不受影響)。
建表入口以 `assert_cohort_names` 對當期資料強制驗證,不符即 raise。
"""

FOREIGN_BROKER_NAMES: Final[dict[str, str]] = {
    "8440": "摩根大通",
    "1480": "美商高盛",
    "1470": "台灣摩根士丹利",
    "1650": "新加坡商瑞銀",
    "1440": "美林",
    "1590": "花旗環球",
    "8960": "香港上海匯豐",
    "8900": "法銀巴黎",
    "8890": "大和國泰",
    "1560": "港商野村",
    "1360": "港商麥格理",
}
"""代號→名稱對照,供 `assert_cohort_names` 對實際資料自我驗證。

**代號必須自資料查得,不得憑印象填**:2026-09-18 首版曾把 9200/9100 誤填為
港商野村/港商麥格理,實際上 9200=凱基、9100=群益金鼎(兩個本土券商總公司),
使 cohort 混入本土 HQ、真外資席位反而漏掉,t3b 全表數字失真。名單改為帶
名稱的 dict + 建表時強制對帳,即為此錯而設。
"""

FOREIGN_BROKER_CODES: Final[frozenset[str]] = frozenset(FOREIGN_BROKER_NAMES)

HISTORICAL_FOREIGN_CODES: Final[dict[str, str]] = {
    "1520": "瑞士信貸(2023 併入 1650 新加坡商瑞銀)",
    "1570": "港商法國興業(已合併退出)",
}
"""2026 已不存在、但建早年表時必須納入 cohort 的外資代號。

出處:`證券商結束營業資料.xls`、`data/derived/broker_merge_map.json`。
本版只建 2026 故未納入;跨年重建時須按年份有效期展開,不可沿用本名單。"""


def assert_cohort_names(broker_name_map: pl.DataFrame) -> None:
    """用實際資料的 (broker, broker_name) 驗證 cohort 名單,不符即 raise。

    broker_name_map 需含 `broker`, `broker_name`。建表入口必須呼叫——宣告的
    代號與資料對不上時要當場炸掉,不能讓錯誤的 cohort 靜默產出一整年的表。
    """
    actual = dict(broker_name_map.select("broker", "broker_name").unique().iter_rows())
    wrong = {c: (n, actual.get(c)) for c, n in FOREIGN_BROKER_NAMES.items()
             if actual.get(c) not in (None, n)}
    missing = [c for c in FOREIGN_BROKER_NAMES if c not in actual]
    if wrong:
        raise ValueError(
            f"外資 cohort 代號與資料不符({FOREIGN_BROKER_COHORT_VERSION}):"
            + "; ".join(f"{c} 宣告={exp} 實際={got}" for c, (exp, got) in wrong.items()))
    if missing:
        print(f"  警告:cohort 代號 {missing} 在本期資料中未出現(可能未交易)")


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
