"""逐月切塊的共用窗口算術(純函數)。建表與研究腳本共用一份,不各自算月底。"""

from __future__ import annotations

import datetime

import polars as pl


def month_bounds(year: int, month: int) -> tuple[datetime.date, datetime.date]:
    start = datetime.date(year, month, 1)
    end = (datetime.date(year + 1, 1, 1) if month == 12
           else datetime.date(year, month + 1, 1)) - datetime.timedelta(days=1)
    return start, end


def month_read_from(cal: pl.DataFrame, month_start: datetime.date) -> datetime.date:
    """讀取起點 = min(月初, 該月首個交易日的前一交易日)——basket 要看前一日,
    由交易日曆決定,不用固定曆日緩衝(農曆年連休 12 天曾咬人)。"""
    prior = cal.filter(pl.col("date") >= month_start)["prev_date"].min()
    return min(prior, month_start) if prior is not None else month_start
