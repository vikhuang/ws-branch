"""T3/T4 純轉換單元測試:合成資料、手算答案。"""

from __future__ import annotations

import datetime

import polars as pl
import pytest

from ws_branch.tables.transforms.t3_official_daily import convert
from ws_branch.tables.transforms.t4_broker_features import compute_broker_day

D = datetime.date(2026, 1, 5)


def test_t3_convert_units_and_names() -> None:
    raw = pl.DataFrame({
        "coid": ["2330"], "mdate": [D],
        "qfii_buy": [11449.0], "qfii_sell": [1000.0], "qfii_ex": [10449.0],
        "fund_buy": [168.0], "fund_sell": [0.0], "fund_ex": [168.0],
        "dlrp_buy": [1.0], "dlrp_sell": [2.0], "dlrp_ex": [-1.0],
        "dlrh_buy": [3.0], "dlrh_sell": [4.0], "dlrh_ex": [-1.0],
        "qfii_bamt": [28_050_947.0], "qfii_samt": [2_450_000.0],
        "fund_bamt": [411_600.0], "fund_samt": [0.0],
        "vol_dt": [2057.0], "vol_dtp": [10.37],
    })
    out = convert(raw).collect()
    assert out[0, "foreign_buy_sh"] == pytest.approx(11_449_000)  # 千股→股
    assert out[0, "fund_net_sh"] == pytest.approx(168_000)
    assert out[0, "prop_hedge_sell_sh"] == pytest.approx(4_000)
    assert out[0, "foreign_buy_amt"] == pytest.approx(28_050_947_000)  # 千元→元
    assert out[0, "day_trade_sh"] == pytest.approx(2_057_000)
    assert out[0, "day_trade_pct"] == pytest.approx(10.37)  # % 原樣
    assert out[0, "symbol_id"] == "2330"


def _t1_rows(rows):
    return pl.DataFrame(
        {"broker": [r[0] for r in rows],
         "broker_name": [r[0] for r in rows],
         "date": [D] * len(rows),
         "buy_dollar": [float(r[2]) for r in rows],
         "sell_dollar": [float(r[3]) for r in rows]})


def test_t4_features_hand_computed() -> None:
    # 分點 A:三檔,買 100/50/10、賣 40/0/0 → gross=140/50/10
    rows = [("A", "s1", 100, 40), ("A", "s2", 50, 0), ("A", "s3", 10, 0)]
    out = compute_broker_day(_t1_rows(rows))
    assert out.height == 1
    r = out.row(0, named=True)
    assert r["n_symbols"] == 3
    assert r["gross_buy_amt"] == 160 and r["gross_sell_amt"] == 40
    assert r["net_amt"] == 120
    assert r["directional_ratio"] == pytest.approx(120 / 200)
    assert r["top1_share"] == pytest.approx(140 / 200)
    assert r["top5_share"] == pytest.approx(1.0)  # 只有 3 檔,top5=全部


def test_t4_top5_with_seven_symbols() -> None:
    # 7 檔 gross = 70,60,50,40,30,20,10(全買零賣)→ top5 = 250/280
    rows = [("B", f"s{i}", g, 0) for i, g in enumerate([70, 60, 50, 40, 30, 20, 10])]
    r = compute_broker_day(_t1_rows(rows)).row(0, named=True)
    assert r["top5_share"] == pytest.approx(250 / 280)
    assert r["top1_share"] == pytest.approx(70 / 280)
    assert r["directional_ratio"] == pytest.approx(1.0)  # 純單向


def test_t4_dash_only_zero_dollar_is_null_not_crash() -> None:
    # dash-only 分點:金額 0(僅 dash 股數)→ ratio/share 為 null,不得 NaN/爆炸
    r = compute_broker_day(_t1_rows([("P", "s1", 0, 0)])).row(0, named=True)
    assert r["directional_ratio"] is None
    assert r["top1_share"] is None and r["top5_share"] is None
