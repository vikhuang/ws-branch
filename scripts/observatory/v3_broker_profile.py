"""分點 profile CLI(IO 殼;渲染在 products/broker_profile.py)。

用法:`uv run python scripts/observatory/v3_broker_profile.py 8440 2026-09-14 [--top 10]`
"""

from __future__ import annotations

import argparse
import datetime

import polars as pl

from ws_core import stock_attr

from ws_branch.measure import salience, universe
from ws_branch.products import broker_profile
from ws_branch.tables import io

WINDOW, MIN_PERIODS, LOOKBACK_DAYS = 60, 20, 150


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("broker")
    ap.add_argument("date", help="YYYY-MM-DD")
    ap.add_argument("--top", type=int, default=10)
    a = ap.parse_args()
    d = datetime.date.fromisoformat(a.date)
    start = str(d - datetime.timedelta(days=LOOKBACK_DAYS))
    hist = io.scan("t4_broker_measure", start=start, end=str(d)).collect()   # as-of:≤ d
    print(broker_profile.render(hist, broker=a.broker, date=d,
                                cohort_codes=universe.cohort_codes(d),
                                stock_rows=_stocks(a.broker, d, start, hist, a.top),
                                window=WINDOW, min_periods=MIN_PERIODS, top_n=a.top))


def _stocks(broker: str, d: datetime.date, start: str, hist: pl.DataFrame,
            top_n: int) -> pl.DataFrame | None:
    """該席位當日本子最重的 top_n 檔,各自建 pair 歷史(只此席位)算 salience 三問。"""
    branch_day = hist.filter(pl.col("broker") == broker).select("broker", "date", "gross_amt")
    if branch_day.filter(pl.col("date") == d).height == 0:
        return None
    uni = universe.stock_universe(stock_attr(start=start, end=str(d),
                                             columns=["coid", "mdate", "stktp_c"]))
    t1 = universe.apply_universe(
        io.scan("t1_broker_daily", start=start, end=str(d))
        .filter(pl.col("broker") == broker)
        .select("broker", "symbol_id", "date", "buy_dollar", "sell_dollar").collect(), uni)
    today = (t1.filter(pl.col("date") == d)
             .with_columns((pl.col("buy_dollar") + pl.col("sell_dollar")).alias("g"))
             .sort("g", descending=True).head(top_n)["symbol_id"].to_list())
    daily = salience.daily_salience(t1.filter(pl.col("symbol_id").is_in(today)), branch_day,
                                    universe=uni)
    out = []
    for s in today:
        panel = salience.build_pair_panel(daily, branch_day, symbol_id=s, universe=uni)
        h = salience.pair_history(panel, window=WINDOW, min_periods=MIN_PERIODS)
        out.append(salience.pair_anomaly(h).filter(pl.col("date") == d))
    return pl.concat(out) if out else None


if __name__ == "__main__":
    main()
