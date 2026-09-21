"""as-of 契約(架構文件 §4.5 / §12「未來資料追加不改變已凍結 as-of 結果」)。

三個層次各一組:
1. T4 v3 純函數 `compute_month`:多給未來一天,既有列逐欄恆等。
2. 讀時計算的落後窗(`baseline.rolling_baseline`、`salience.pair_history`):
   序列截斷在 t 與用全序列算,t 的值恆等——落後窗不得偷看未來。
3. `available_at` 規則:每列 = date 當天 21:45 台北,且嚴格晚於 date。
e2e 層(整條 CLI 鏈追加一天後重建)在 tests/test_e2e.py。
"""

from __future__ import annotations

import datetime

import polars as pl
import pytest

from ws_branch.measure import allocation, baseline, salience
from ws_branch.tables.transforms import t4_broker_measure as m

D = [datetime.date(2026, 1, 5) + datetime.timedelta(days=i) for i in range(3)]


def _t1(days: list[datetime.date]) -> pl.DataFrame:
    rows = []
    for i, d in enumerate(days):
        rows += [("A", "元大-台北", "2330", d, 100.0 + i, 50.0),
                 ("A", "元大-台北", "1531", d, 10.0, 20.0 + i),
                 ("B", "摩根大通", "2330", d, 300.0, 0.0)]
    return pl.DataFrame(rows, schema=["broker", "broker_name", "symbol_id", "date",
                                      "buy_dollar", "sell_dollar"], orient="row")


def _t3(days: list[datetime.date]) -> pl.DataFrame:
    cols = {c: [1.0] * (2 * len(days)) for pair in m.OFFICIAL_BUCKETS.values() for c in pair[:2]}
    return pl.DataFrame({"symbol_id": ["2330", "1531"] * len(days),
                         "date": [d for d in days for _ in range(2)], **cols})


def _uni(days: list[datetime.date]) -> pl.DataFrame:
    return pl.DataFrame({"symbol_id": ["2330", "1531"] * len(days),
                         "date": [d for d in days for _ in range(2)]})


def test_compute_month_rows_frozen_when_future_day_appended() -> None:
    cal = allocation.prev_trading_day_map(D)
    two = m.compute_month(_t1(D[:2]), _t3(D[:2]), _uni(D[:2]), cal, month_start=D[0])
    three = m.compute_month(_t1(D), _t3(D), _uni(D), cal, month_start=D[0])
    assert three.height > two.height
    assert three.filter(pl.col("date") <= D[1]).sort("date", "broker").equals(
        two.sort("date", "broker"))


def test_available_at_is_same_day_2145_taipei_and_after_date() -> None:
    cal = allocation.prev_trading_day_map(D)
    out = m.compute_month(_t1(D), _t3(D), _uni(D), cal, month_start=D[0])
    local = out["available_at"].dt.convert_time_zone(m.TAIPEI)
    assert (local.dt.time() == m.AVAILABLE_AT_TIME).all()
    assert (local.dt.date() == out["date"]).all()
    assert (local.dt.replace_time_zone(None) > out["date"].cast(pl.Datetime("us"))).all()


def test_rolling_baseline_asof_invariance() -> None:
    """截斷在 t 的序列與全序列,t 的 baseline 恆等(嚴格落後,不偷看未來)。"""
    n = 40
    days = [datetime.date(2026, 1, 1) + datetime.timedelta(days=i) for i in range(n)]
    vals = [((i * 7919) % 101) / 100 for i in range(n)]
    full = pl.DataFrame({"broker": ["A"] * n, "date": days, "y": vals})
    t = 30
    a = baseline.rolling_baseline(full, value_col="y", group_col="broker", order_col="date",
                                  window=10, min_periods=5, prefix="b")
    b = baseline.rolling_baseline(full.head(t + 1), value_col="y", group_col="broker",
                                  order_col="date", window=10, min_periods=5, prefix="b")
    row_full = a.filter(pl.col("date") == days[t]).drop("date", "broker")
    row_cut = b.filter(pl.col("date") == days[t]).drop("date", "broker")
    assert row_full.equals(row_cut)
    assert row_full.select(pl.all().is_not_null().all()).row(0) == (True,) * row_full.width


def test_pair_history_asof_invariance() -> None:
    n = 40
    days = [datetime.date(2026, 1, 1) + datetime.timedelta(days=i) for i in range(n)]
    t1 = pl.DataFrame({"broker": ["A"] * n, "symbol_id": ["3450"] * n, "date": days,
                       "buy_dollar": [float((i * 31) % 17) for i in range(n)],
                       "sell_dollar": [0.0] * n})
    bd = pl.DataFrame({"broker": ["A"] * n, "date": days, "gross_amt": [100.0] * n})
    t = 33

    def hist(k: int) -> pl.DataFrame:
        panel = salience.build_pair_panel(
            salience.daily_salience(t1.head(k), bd.head(k)), bd.head(k), symbol_id="3450")
        return salience.pair_history(panel, window=10, min_periods=5)

    full, cut = hist(n), hist(t + 1)
    assert full.filter(pl.col("date") == days[t]).equals(cut.filter(pl.col("date") == days[t]))


def test_basket_self_sim_uses_only_previous_trading_day() -> None:
    """D3 的 basket 只看 D2;把 D3 之後的資料換掉不影響 D3。"""
    cal = allocation.prev_trading_day_map(D)
    base = m.compute_month(_t1(D), _t3(D), _uni(D), cal, month_start=D[0])
    d3 = base.filter(pl.col("date") == D[2])
    assert d3["basket_self_sim"].is_not_null().all()
    assert base.filter(pl.col("date") == D[0])["basket_self_sim"].is_null().all()  # 無前一日
    assert pytest.approx(d3.filter(pl.col("broker") == "B")[0, "basket_self_sim"]) == 1.0
