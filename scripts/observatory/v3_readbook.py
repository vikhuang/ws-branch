"""個股讀本 v1 CLI(IO 殼;渲染邏輯在 products/stock_readbook.py)。"""

from __future__ import annotations

import argparse
import datetime

import polars as pl

from ws_branch.measure.universe import FOREIGN_BROKER_CODES
from ws_branch.products import stock_readbook
from ws_branch.tables import io


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
                                cohort_codes=FOREIGN_BROKER_CODES, top_n=a.top))


if __name__ == "__main__":
    main()
