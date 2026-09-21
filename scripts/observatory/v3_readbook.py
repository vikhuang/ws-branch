"""個股讀本 v1 CLI(IO 殼;渲染邏輯在 products/stock_readbook.py)。"""

from __future__ import annotations

import argparse
import datetime
from pathlib import Path

import polars as pl

from ws_branch.measure import salience
from ws_branch.measure.universe import FOREIGN_BROKER_CODES
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
                                cohort_codes=FOREIGN_BROKER_CODES, top_n=a.top,
                                salience_day=_salience(a.symbol_id, d)))


def _salience(symbol_id: str, d: datetime.date) -> pl.DataFrame | None:
    """單股查詢算 salience 三問(§7:首版不物化 branch×stock×day)。"""
    prim = Path("/tmp/v3_phase1_primitives.parquet")
    if not prim.exists():
        return None
    start = str(d - datetime.timedelta(days=LOOKBACK_DAYS))
    branch_day = pl.read_parquet(prim).select("broker", "date", "gross_amt")
    t1 = (io.scan("t1_broker_daily", start=start, end=str(d))
          .filter(pl.col("symbol_id") == symbol_id)
          .select("broker", "symbol_id", "date", "buy_dollar", "sell_dollar")
          .collect())
    if t1.height == 0:
        return None
    panel = salience.build_pair_panel(
        salience.daily_salience(t1, branch_day), branch_day, symbol_id=symbol_id)
    hist = salience.pair_history(panel, window=WINDOW, min_periods=MIN_PERIODS)
    return (salience.pair_anomaly(hist).filter(pl.col("date") == d)
            .select("broker", "salience", "sal_mean", "salience_z",
                    "stock_gross_z", "participation_rate", "denominator_effect"))


if __name__ == "__main__":
    main()
