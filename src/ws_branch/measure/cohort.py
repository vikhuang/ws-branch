"""外資券商 cohort 的宣告與解析(純函數,零 IO)。

從 measure/universe.py 切出(2026-09-22 SRP):universe 管「哪些股票」,本模組管
「哪些席位算外資券商 cohort、何時算」。cohort 是**行政分類**(券商執照),不是投資人
身份——所有下游(t3b、T4 v3、讀本、profile、Phase 4)都從這裡取名單與有效期。
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import Final

import polars as pl

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
