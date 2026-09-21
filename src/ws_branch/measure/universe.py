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

import datetime
from dataclasses import dataclass
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

FOREIGN_BROKER_COHORT_VERSION: Final = "foreign_seat_v3"
"""外資券商席位名單 v3(2026-09-21:v2 的 11 家 + **時變有效期**,見 FOREIGN_COHORT)。

現行 11 家外國證券商在台分支機構(v2,2026-09-20 定案)。名單經**三方對照 + 官方登記檔**驗證:
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

**有效期**:cohort 是**時變**的(v3,2026-09-21 起)。每個成員帶 valid_from /
valid_to,由 `cohort_expr` / `cohort_codes` 按日期解析;已知變動全部自資料查得:
- 瑞士信貸 1520:最後交易日 2023-11-01(併入 1650 新加坡商瑞銀)。
- 港商法國興業 1570:最後交易日 2025-07-31(合併退出)。
- 台灣匯立 1380:最後交易日 2025-10-17;2025-10-20 起代號 9A81「永豐金-匯立」
  (併入永豐,不接新客、只剩舊外資客戶)。**user 2026-09-21 決定 9A81 不入
  cohort**:cohort 定義是「外資券商執照」,不是「客戶是外資」;9A81 在 2026
  日均 gross 約 126 億、111 檔,比 cohort 內四家小額台都大,其流量會落在
  HiddenForeign 下界內——讀本與 findings 引用時要註明。
- 2026 年另有台新/元富合併造成 51 個代號換名(本名單不受影響)。
建表入口以 `assert_cohort_names(…, year=)` 對當年資料強制驗證,不符即 raise。
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
"""**現行**(2026)成員的代號→名稱。歷史成員見 `FOREIGN_COHORT`。

**代號必須自資料查得,不得憑印象填**:2026-09-18 首版曾把 9200/9100 誤填為
港商野村/港商麥格理,實際上 9200=凱基、9100=群益金鼎(兩個本土券商總公司),
使 cohort 混入本土 HQ、真外資席位反而漏掉,t3b 全表數字失真。名單改為帶
名稱的 dict + 建表時強制對帳,即為此錯而設。
"""

FOREIGN_BROKER_CODES: Final[frozenset[str]] = frozenset(FOREIGN_BROKER_NAMES)
"""現行成員代號。**只能用於 2026 及之後的切片**;跨年一律用 `cohort_expr`。"""


@dataclass(frozen=True, slots=True)
class CohortMember:
    code: str
    name: str
    valid_from: datetime.date | None   # None = 資料起點即在
    valid_to: datetime.date | None     # None = 至今;否則為最後交易日(含)
    note: str = ""


FOREIGN_COHORT: Final[tuple[CohortMember, ...]] = tuple(
    [CohortMember(c, n, None, None) for c, n in FOREIGN_BROKER_NAMES.items()] + [
        CohortMember("1520", "瑞士信貸", None, datetime.date(2023, 11, 1),
                     "併入 1650 新加坡商瑞銀"),
        CohortMember("1570", "港商法國興業", None, datetime.date(2025, 7, 31),
                     "合併退出"),
        CohortMember("1380", "台灣匯立", None, datetime.date(2025, 10, 17),
                     "併入永豐,2025-10-20 起為 9A81 永豐金-匯立(不入 cohort)"),
    ])
"""外資券商 cohort 全史成員(含已退出者),有效期自 broker_tx 逐日查得。"""

KNOWN_HIDDEN_FOREIGN: Final[dict[str, str]] = {
    "9A81": "永豐金-匯立(2025-10-20 起;原 1380 台灣匯立,只剩舊外資客戶;"
            "user 2026-09-21 決定不入 cohort)",
}
"""明知客戶以外資為主、但依「外資券商執照」規則不入 cohort 的席位。

其流量會計上落在 HiddenForeign(Y)裡——引用 Y 下界時要註明這一家。"""

HISTORICAL_FOREIGN_CODES: Final[dict[str, str]] = {
    m.code: f"{m.name}({m.note})" for m in FOREIGN_COHORT if m.valid_to is not None}
"""已退出成員的代號→說明(由 FOREIGN_COHORT 導出,向後相容)。"""


def _active(m: CohortMember, d: datetime.date) -> bool:
    return (m.valid_from is None or d >= m.valid_from) and \
           (m.valid_to is None or d <= m.valid_to)


def cohort_codes(as_of: datetime.date) -> frozenset[str]:
    """某日有效的 cohort 代號。"""
    return frozenset(m.code for m in FOREIGN_COHORT if _active(m, as_of))


def cohort_members_in_year(year: int) -> tuple[CohortMember, ...]:
    """該年內任一日有效的成員(建表驗證用)。"""
    y0, y1 = datetime.date(year, 1, 1), datetime.date(year, 12, 31)
    return tuple(m for m in FOREIGN_COHORT
                 if (m.valid_from is None or m.valid_from <= y1)
                 and (m.valid_to is None or m.valid_to >= y0))


def cohort_expr(broker_col: str = "broker", date_col: str = "date") -> pl.Expr:
    """逐列布林:該 (broker, date) 是否在 cohort 內。跨年聚合一律用此式。"""
    expr = pl.lit(False)
    for m in FOREIGN_COHORT:
        cond = pl.col(broker_col) == m.code
        if m.valid_from is not None:
            cond = cond & (pl.col(date_col) >= m.valid_from)
        if m.valid_to is not None:
            cond = cond & (pl.col(date_col) <= m.valid_to)
        expr = expr | cond
    return expr.alias("in_cohort")


def assert_cohort_names(broker_name_map: pl.DataFrame, *,
                        year: int | None = None) -> list[str]:
    """用實際資料的 (broker, broker_name) 驗證 cohort 名單,不符即 raise。

    broker_name_map 需含 `broker`, `broker_name`。建表入口必須呼叫——宣告的
    代號與資料對不上時要當場炸掉,不能讓錯誤的 cohort 靜默產出一整年的表。
    year 給了就驗該年有效的成員(含當年退出者);不給 = 只驗現行成員。

    回傳「本期資料中未出現的代號」清單(可能該席位當期未交易,或名單過期)
    交由呼叫端決定如何揭露——**本模組為純函數,不印 stdout**(§3.2)。
    """
    members = cohort_members_in_year(year) if year is not None else tuple(
        m for m in FOREIGN_COHORT if m.valid_to is None)
    actual = dict(broker_name_map.select("broker", "broker_name").unique().iter_rows())
    wrong = {m.code: (m.name, actual.get(m.code)) for m in members
             if actual.get(m.code) not in (None, m.name)}
    if wrong:
        raise ValueError(
            f"外資 cohort 代號與資料不符({FOREIGN_BROKER_COHORT_VERSION}):"
            + "; ".join(f"{c} 宣告={exp} 實際={got}" for c, (exp, got) in wrong.items()))
    return [m.code for m in members if m.code not in actual]


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


def seat_class(broker_col: str = "broker", name_col: str = "broker_name", *,
               cohort_codes: frozenset[str]) -> pl.Expr:
    """席位的行政分類(讀本/profile 共用):外資席位 / 外資客戶* / 分行 / 總公司。

    「外資客戶*」= KNOWN_HIDDEN_FOREIGN(9A81),明知客戶以外資為主但依券商執照規則
    不入 cohort——不得混進總公司/分行。這是行政分類,**不是投資人身份**。
    """
    return (pl.when(pl.col(broker_col).is_in(list(cohort_codes))).then(pl.lit("外資席位"))
            .when(pl.col(broker_col).is_in(list(KNOWN_HIDDEN_FOREIGN))).then(pl.lit("外資客戶*"))
            .otherwise(pl.when(pl.col(name_col).str.contains("-")).then(pl.lit("分行"))
                       .otherwise(pl.lit("總公司"))))
