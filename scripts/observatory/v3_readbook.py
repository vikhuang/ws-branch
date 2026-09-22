"""個股讀本 CLI(薄殼:載入在 products/loaders,渲染在 products/stock_readbook)。

用法:`uv run python scripts/observatory/v3_readbook.py 3450 2026-09-14 [--top 12]`
"""

from __future__ import annotations

import argparse
import datetime

from ws_branch.products import loaders, stock_readbook


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("symbol_id")
    ap.add_argument("date", help="YYYY-MM-DD")
    ap.add_argument("--top", type=int, default=12)
    a = ap.parse_args()
    d = datetime.date.fromisoformat(a.date)
    inp = loaders.readbook_inputs(a.symbol_id, d)
    for n in inp.notes:
        print(f"  {n}")
    print(stock_readbook.render(inp.t1_day, inp.bounds_day, inp.t3_day, symbol_id=a.symbol_id,
                                date=d, cohort_codes=inp.cohort_codes, top_n=a.top,
                                salience_day=inp.salience_day, seat_state_day=inp.seat_state_day))


if __name__ == "__main__":
    main()
