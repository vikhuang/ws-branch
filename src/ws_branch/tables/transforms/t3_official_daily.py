"""T3 官方對齊表:股票×日的三大法人流(tej_shareholding 語意層)。

單位在此鎖死:TEJ 量欄=千股、金額欄=千元(ws-core shareholding docstring
契約)→ 表內一律 **股 / 元**,欄名帶 _sh/_amt 後綴。下游不再碰 ×1000。
margin/借券欄位未納入(P6 按需求拉,見 registry 註記)。
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
