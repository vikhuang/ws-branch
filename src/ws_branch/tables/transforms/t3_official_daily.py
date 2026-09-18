"""T3 官方對齊表:股票×日的三大法人流(tej_shareholding 語意層)。

單位在此鎖死:TEJ 量欄=千股、金額欄=千元(ws-core shareholding docstring
契約)→ 表內一律 **股 / 元**,欄名帶 _sh/_amt 後綴。下游不再碰 ×1000。
margin/借券欄位未納入(P6 按需求拉,見 registry 註記)。

v2(2026-09-18):補自營兩桶的金額欄(dlrp_bamt/samt、dlrh_bamt/samt)與
三大法人合計(tot_buy/sell/bamt/samt)。上游本來就有(9 月非空率 99.8%;
實測 3450@2026-09-14 自營自行 71,769 千元 / 141 張 = 509 元/股 = 當日收盤),
先前建表時未選,不是資料缺口——四個官方桶的 amount cosine 至此才有同口徑
輸入。但自營**無分點層級錨**(A2:「-自營」席位僅 0.06% 全量),其 cosine
只能描述,校準狀態一律標 unanchored。`tot_*` 實測 = 四桶合計(2026-09 全市場
最大差 2 張),是四桶互斥性的一致性檢查,**不是**全市場成交總量 V。

本表刻意保留所有證券種類(含指數/ETF/特別股):股票 universe gate 由聚合
入口套用(架構文件 §4.1)。**任何 T3-only 加總必須先過 measure.universe**
——2026-09-14 實證,不套 gate 時「指數」型代號帶 12,765 億外資買入,是普通股
2,734 億的 4.7 倍。
"""

from __future__ import annotations

import polars as pl
from ws_core import shareholding

_RENAME_K = {  # TEJ 欄 → (新名, 千股/千元 → 股/元)
    "qfii_buy": "foreign_buy_sh", "qfii_sell": "foreign_sell_sh",
    "qfii_ex": "foreign_net_sh",
    "fund_buy": "fund_buy_sh", "fund_sell": "fund_sell_sh",
    "fund_ex": "fund_net_sh",
    "dlrp_buy": "prop_self_buy_sh", "dlrp_sell": "prop_self_sell_sh",
    "dlrp_ex": "prop_self_net_sh",
    "dlrh_buy": "prop_hedge_buy_sh", "dlrh_sell": "prop_hedge_sell_sh",
    "dlrh_ex": "prop_hedge_net_sh",
    "qfii_bamt": "foreign_buy_amt", "qfii_samt": "foreign_sell_amt",
    "fund_bamt": "fund_buy_amt", "fund_samt": "fund_sell_amt",
    # v2:自營兩桶金額 + 三大法人合計(量與金額)
    "dlrp_bamt": "prop_self_buy_amt", "dlrp_samt": "prop_self_sell_amt",
    "dlrh_bamt": "prop_hedge_buy_amt", "dlrh_samt": "prop_hedge_sell_amt",
    "tot_buy": "official_total_buy_sh", "tot_sell": "official_total_sell_sh",
    "tot_bamt": "official_total_buy_amt", "tot_samt": "official_total_sell_amt",
    "vol_dt": "day_trade_sh",
}


def convert(raw: pl.LazyFrame | pl.DataFrame) -> pl.LazyFrame:
    """純轉換:rename + ×1000 + 當沖佔比原樣保留。"""
    lf = raw.lazy() if isinstance(raw, pl.DataFrame) else raw
    return (lf.rename({"coid": "symbol_id", "mdate": "date"})
            .with_columns([(pl.col(k) * 1000).alias(v)
                           for k, v in _RENAME_K.items()]
                          + [pl.col("vol_dtp").alias("day_trade_pct"),
                             pl.col("date").cast(pl.Date)])
            .select(["symbol_id", "date"] + sorted(_RENAME_K.values())
                    + ["day_trade_pct"]))


def build_year(year: int) -> pl.LazyFrame:
    raw = shareholding(
        start=f"{year}-01-01", end=f"{year}-12-31",
        columns=["coid", "mdate"] + list(_RENAME_K) + ["vol_dtp"])
    return convert(raw)
