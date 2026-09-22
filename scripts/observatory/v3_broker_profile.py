"""分點 profile CLI(薄殼:載入在 products/loaders,渲染在 products/broker_profile)。

用法:`uv run python scripts/observatory/v3_broker_profile.py 8440 2026-09-14 [--top 10]`
"""

from __future__ import annotations

import argparse
import datetime

from ws_branch.products import broker_profile, loaders


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("broker")
    ap.add_argument("date", help="YYYY-MM-DD")
    ap.add_argument("--top", type=int, default=10)
    a = ap.parse_args()
    d = datetime.date.fromisoformat(a.date)
    inp = loaders.profile_inputs(a.broker, d, top_n=a.top)
    for n in inp.notes:
        print(f"  {n}")
    print(broker_profile.render(inp.history, broker=a.broker, date=d, cohort_codes=inp.cohort_codes,
                                stock_rows=inp.stock_rows, window=loaders.WINDOW,
                                min_periods=loaders.MIN_PERIODS, top_n=a.top))


if __name__ == "__main__":
    main()
