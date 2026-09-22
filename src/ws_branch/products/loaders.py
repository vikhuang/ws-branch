"""讀本 / profile 的資料載入(IO 層;渲染在 stock_readbook / broker_profile,計算在 measure/)。

三個 CLI 原本各自組裝 as-of 切片與 salience 管線,兩支還各漏了一處 gate(2026-09-21
外部審查)。這裡是唯一的組裝點;訊息用 `notes` 回傳,由 CLI 決定怎麼印。

as-of 紀律:一律 `date ≤ d`;T4 v3 的 d 列 available_at = d 21:45 台北(推定)。
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field

import polars as pl
from ws_core import stock_attr

from ws_branch.measure import cohort, salience, state, universe
from ws_branch.tables import io

WINDOW, MIN_PERIODS, LOOKBACK_DAYS = 60, 20, 150
T1_COLS = ["broker", "symbol_id", "date", "buy_dollar", "sell_dollar"]


@dataclass
class ReadbookInputs:
    t1_day: pl.DataFrame
    bounds_day: pl.DataFrame
    t3_day: pl.DataFrame
    cohort_codes: frozenset[str]
    salience_day: pl.DataFrame | None = None
    seat_state_day: pl.DataFrame | None = None
    notes: list[str] = field(default_factory=list)


@dataclass
class ProfileInputs:
    history: pl.DataFrame
    cohort_codes: frozenset[str]
    stock_rows: pl.DataFrame | None = None
    notes: list[str] = field(default_factory=list)


def t4_history(d: datetime.date, lookback_days: int = LOOKBACK_DAYS) -> pl.DataFrame:
    """T4 v3 到 d 為止(含)的全席位切片。"""
    start = d - datetime.timedelta(days=lookback_days)
    return io.scan("t4_broker_measure", start=str(start), end=str(d)).collect()


def universe_days(start: datetime.date, end: datetime.date) -> pl.DataFrame:
    return universe.stock_universe(stock_attr(start=str(start), end=str(end),
                                              columns=["coid", "mdate", "stktp_c"]))


def _gate_note(excluded: pl.DataFrame, total: int) -> str | None:
    if excluded.height == 0:
        return None
    return (f"[gate] 排除 {excluded.height:,}/{total:,} 列 T1({excluded['date'].min()}~"
            f"{excluded['date'].max()} 不在普通股 universe,留 null 不補零)")


def readbook_inputs(symbol_id: str, d: datetime.date, *, window: int = WINDOW,
                    min_periods: int = MIN_PERIODS) -> ReadbookInputs:
    day = str(d)
    out = ReadbookInputs(
        t1_day=io.scan("t1_broker_daily", start=day, end=day).filter(pl.col("symbol_id") == symbol_id).collect(),
        bounds_day=io.scan("t3b_accounting_bounds", start=day, end=day).filter(pl.col("symbol_id") == symbol_id).collect(),
        t3_day=io.scan("t3_official_daily", start=day, end=day).filter(pl.col("symbol_id") == symbol_id).collect(),
        cohort_codes=cohort.cohort_codes(d))
    hist = t4_history(d)
    if hist.height == 0:
        out.notes.append("(salience/性格略:t4_broker_measure 無資料,請先 build --table t4_broker_measure)")
        return out
    if hist.filter(pl.col("date") == d).height:
        out.seat_state_day = state.seat_state(hist, date=d, primitives=("top5_share", "directional_ratio"),
                                              window=window, min_periods=min_periods)
    else:
        out.notes.append(f"(性格/狀態略:t4_broker_measure 無 {d} 的列)")
    start = d - datetime.timedelta(days=LOOKBACK_DAYS)
    uni = universe_days(start, d)
    if uni.filter((pl.col("symbol_id") == symbol_id) & (pl.col("date") == d)).height == 0:
        out.notes.append(f"(salience 略:{symbol_id} 於 {d} 不在普通股 universe,分子/分母口徑不一致)")
        return out
    t1 = (io.scan("t1_broker_daily", start=str(start), end=day)
          .filter(pl.col("symbol_id") == symbol_id).select(T1_COLS).collect())
    if t1.height == 0:
        return out
    gated, excluded = salience.gate_numerator(t1, uni)
    if (n := _gate_note(excluded, t1.height)):
        out.notes.append(n)
    res = salience.pair_pipeline(gated, hist.select("broker", "date", "gross_amt"),
                                 symbols=[symbol_id], universe_days=uni,
                                 window=window, min_periods=min_periods)
    if res.height:
        out.salience_day = (res.filter(pl.col("date") == d)
                            .select("broker", "salience", "sal_mean", "salience_z",
                                    "stock_gross_z", "participation_rate", "denominator_effect"))
    return out


def profile_inputs(broker: str, d: datetime.date, *, top_n: int = 10, window: int = WINDOW,
                   min_periods: int = MIN_PERIODS) -> ProfileInputs:
    hist = t4_history(d)
    out = ProfileInputs(history=hist, cohort_codes=cohort.cohort_codes(d))
    branch_day = hist.filter(pl.col("broker") == broker).select("broker", "date", "gross_amt")
    if branch_day.filter(pl.col("date") == d).height == 0:
        return out
    start = d - datetime.timedelta(days=LOOKBACK_DAYS)
    uni = universe_days(start, d)
    t1 = (io.scan("t1_broker_daily", start=str(start), end=str(d))
          .filter(pl.col("broker") == broker).select(T1_COLS).collect())
    gated, excluded = salience.gate_numerator(t1, uni)
    if (n := _gate_note(excluded, t1.height)):
        out.notes.append(n)
    today = (gated.filter(pl.col("date") == d)
             .with_columns((pl.col("buy_dollar") + pl.col("sell_dollar")).alias("g"))
             .sort("g", descending=True).head(top_n)["symbol_id"].to_list())
    if not today:
        return out
    res = salience.pair_pipeline(gated, branch_day, symbols=today, universe_days=uni,
                                 window=window, min_periods=min_periods)
    out.stock_rows = res.filter(pl.col("date") == d) if res.height else None
    return out
