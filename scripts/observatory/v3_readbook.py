"""個股讀本 v1 CLI(IO 殼;渲染邏輯在 products/stock_readbook.py)。"""

from __future__ import annotations

import argparse
import datetime

import polars as pl

from ws_core import stock_attr

from ws_branch.measure import salience, state, universe
from ws_branch.products import stock_readbook
from ws_branch.tables import io

WINDOW, MIN_PERIODS, LOOKBACK_DAYS = 60, 20, 150


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("symbol_id")
    ap.add_argument("date", help="YYYY-MM-DD")
    ap.add_argument("--top", type=int, default=12)
    a = ap.parse_args()
    d = datetime.date.fromisoformat(a.date)
    t1 = (io.scan("t1_broker_daily", start=a.date, end=a.date)
          .filter(pl.col("symbol_id") == a.symbol_id).collect())
    bounds = (io.scan("t3b_accounting_bounds", start=a.date, end=a.date)
              .filter(pl.col("symbol_id") == a.symbol_id).collect())
    t3 = (io.scan("t3_official_daily", start=a.date, end=a.date)
          .filter(pl.col("symbol_id") == a.symbol_id).collect())
    print(stock_readbook.render(t1, bounds, t3, symbol_id=a.symbol_id, date=d,
                                cohort_codes=universe.cohort_codes(d), top_n=a.top,
                                salience_day=_salience(a.symbol_id, d),
                                seat_state_day=_seat_state(d)))


def _t4_history(d: datetime.date) -> pl.DataFrame:
    """T4 v3 到 d 為止的歷史切片(as-of:只取 date ≤ d;d 的列 available_at = d 21:45)。"""
    start = str(d - datetime.timedelta(days=LOOKBACK_DAYS))
    return io.scan("t4_broker_measure", start=start, end=str(d)).collect()


def _seat_state(d: datetime.date) -> pl.DataFrame | None:
    hist = _t4_history(d)
    if hist.filter(pl.col("date") == d).height == 0:
        print(f"  (性格/狀態略:t4_broker_measure 無 {d} 的列)")
        return None
    return state.seat_state(hist, date=d, primitives=("top5_share", "directional_ratio"),
                            window=WINDOW, min_periods=MIN_PERIODS)


def _salience(symbol_id: str, d: datetime.date) -> pl.DataFrame | None:
    """單股查詢算 salience 三問(§7:首版不物化 branch×stock×day)。"""
    start = str(d - datetime.timedelta(days=LOOKBACK_DAYS))
    hist = _t4_history(d)
    if hist.height == 0:
        print("  (salience 略:t4_broker_measure 無資料,請先 build --table t4_broker_measure)")
        return None
    # 分母(席位日總額)已套普通股 gate;分子若是 ETF/興櫃等 universe 外標的,
    # salience 定義不一致——寧可不算,不能靜默給錯數
    uni = universe.stock_universe(stock_attr(start=start, end=str(d),
                                             columns=["coid", "mdate", "stktp_c"]))
    if uni.filter((pl.col("symbol_id") == symbol_id)
                  & (pl.col("date") == d)).height == 0:
        print(f"  (salience 略:{symbol_id} 於 {d} 不在普通股 universe,分子/分母口徑不一致)")
        return None
    branch_day = hist.select("broker", "date", "gross_amt")   # 分母:席位日總額(已 gate)
    t1 = (io.scan("t1_broker_daily", start=start, end=str(d))
          .filter(pl.col("symbol_id") == symbol_id)
          .select("broker", "symbol_id", "date", "buy_dollar", "sell_dollar")
          .collect())
    if t1.height == 0:
        return None
    # 分子先套逐日 universe(中途上市/類型變更的日子是無定義,不是未交易);
    # 8102 於 2025-12-22 上櫃,之前興櫃期間有成交但不在 universe(外部審查 09-21)
    gated = universe.apply_universe(t1, uni)
    if gated.height != t1.height:
        ex = t1.join(uni, on=["symbol_id", "date"], how="anti")
        print(f"  [gate] 排除 {t1.height - gated.height:,} 列 T1({ex['date'].min()}~{ex['date'].max()} "
              f"不在普通股 universe,留 null 不補零)")
    t1 = gated
    panel = salience.build_pair_panel(
        salience.daily_salience(t1, branch_day, universe=uni),
        branch_day, symbol_id=symbol_id, universe=uni)
    hist = salience.pair_history(panel, window=WINDOW, min_periods=MIN_PERIODS)
    return (salience.pair_anomaly(hist).filter(pl.col("date") == d)
            .select("broker", "salience", "sal_mean", "salience_z",
                    "stock_gross_z", "participation_rate", "denominator_effect"))


if __name__ == "__main__":
    main()
