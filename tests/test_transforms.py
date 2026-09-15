"""T3/T4 純轉換單元測試:合成資料、手算答案。"""

from __future__ import annotations

import datetime

import polars as pl
import pytest

import math

from ws_branch.measure import actor
from ws_branch.tables.transforms.t3_official_daily import convert
from ws_branch.tables.transforms.t4_broker_features import compute_broker_day

D = datetime.date(2026, 1, 5)
D2 = datetime.date(2026, 1, 6)
D3 = datetime.date(2026, 1, 7)


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


# ── actor.py(t4 v2:多重性/指紋)──────────────────────────────────────


def test_basket_self_similarity_hand_computed() -> None:
    # A:day1(s1=100,s2=50)→day2(s1=90,s2=60)→day3(s1=100,無 s2)
    t1 = pl.DataFrame({
        "broker": ["A"] * 5, "symbol_id": ["s1", "s2", "s1", "s2", "s1"],
        "date": [D, D, D2, D2, D3],
        "buy_dollar": [100.0, 50.0, 90.0, 0.0, 100.0],
        "sell_dollar": [0.0, 0.0, 0.0, 60.0, 0.0],
    })
    out = actor.basket_self_similarity(t1).sort("date")
    n1, n2, n3 = math.sqrt(100**2 + 50**2), math.sqrt(90**2 + 60**2), 100.0
    assert out.height == 2  # day1 無「昨天」可比,不產生列
    assert out[0, "date"] == D2
    assert out[0, "basket_self_sim"] == pytest.approx((100 * 90 + 50 * 60) / (n1 * n2))
    assert out[1, "date"] == D3
    assert out[1, "basket_self_sim"] == pytest.approx((100 * 90) / (n3 * n2))


def test_identity_similarity_buy_perfect_sell_reverse() -> None:
    symbols = [f"s{i}" for i in range(5)]
    t1 = pl.DataFrame({
        "broker": ["A"] * 5, "symbol_id": symbols, "date": [D] * 5,
        "buy_dollar": [10.0, 20.0, 30.0, 40.0, 50.0], "sell_dollar": [0.0] * 5,
    })
    t3 = pl.DataFrame({
        "symbol_id": symbols, "date": [D] * 5,
        "foreign_buy_sh": [100.0, 200.0, 300.0, 400.0, 500.0],   # 同序 → rho=1
        "fund_buy_sh": [500.0, 400.0, 300.0, 200.0, 100.0],      # 逆序 → rho=-1
        "foreign_sell_sh": [0.0] * 5, "fund_sell_sh": [0.0] * 5,
    })
    r = actor.identity_similarity(t1, t3).row(0, named=True)
    assert r["foreign_sim_buy"] == pytest.approx(1.0)
    assert r["fund_sim_buy"] == pytest.approx(-1.0)
    assert r["foreign_sim_sell"] is None  # 沒賣過任何股票 → 無支撐集合
    assert r["foreign_sim_confidence"] == "high"
    assert r["fund_sim_confidence"] == "low"


def test_identity_similarity_below_min_support_is_null() -> None:
    # 只碰 2 檔(< _MIN_SUPPORT=5)→ 不硬湊相關係數,標 null
    t1 = pl.DataFrame({
        "broker": ["A", "A"], "symbol_id": ["s1", "s2"], "date": [D, D],
        "buy_dollar": [10.0, 20.0], "sell_dollar": [0.0, 0.0],
    })
    t3 = pl.DataFrame({
        "symbol_id": ["s1", "s2"], "date": [D, D],
        "foreign_buy_sh": [100.0, 200.0], "fund_buy_sh": [100.0, 200.0],
        "foreign_sell_sh": [0.0, 0.0], "fund_sell_sh": [0.0, 0.0],
    })
    r = actor.identity_similarity(t1, t3).row(0, named=True)
    assert r["foreign_sim_buy"] is None


def test_sector_concentration_and_daytrade_assoc_hand_computed() -> None:
    t1 = pl.DataFrame({
        "broker": ["A", "A", "A"], "symbol_id": ["s1", "s2", "s3"], "date": [D] * 3,
        "buy_dollar": [60.0, 30.0, 10.0], "sell_dollar": [0.0, 0.0, 0.0],
    })
    tickers = pl.DataFrame({
        "symbol_id": ["s1", "s2", "s3"], "industry_local": ["半導體", "半導體", "食品"]
    })
    sec = actor.sector_concentration(t1, tickers).row(0, named=True)
    assert sec["sector_hhi"] == pytest.approx(0.9**2 + 0.1**2)  # 0.82
    assert sec["top_sector"] == "半導體"

    t3 = pl.DataFrame({
        "symbol_id": ["s1", "s2", "s3"], "date": [D] * 3,
        "day_trade_pct": [10.0, 20.0, 0.0],
    })
    dta = actor.daytrade_association(t1, t3).row(0, named=True)
    assert dta["daytrade_assoc"] == pytest.approx((60 * 10 + 30 * 20 + 10 * 0) / 100)


def test_sector_concentration_missing_industry_bucketed_not_dropped() -> None:
    # 查無產業碼(新股/ETF)→ 歸「未知」桶,不得靜默丟棄(家法#3)
    t1 = pl.DataFrame({
        "broker": ["A"], "symbol_id": ["new_ipo"], "date": [D],
        "buy_dollar": [10.0], "sell_dollar": [0.0],
    })
    tickers = pl.DataFrame({"symbol_id": ["s1"], "industry_local": ["半導體"]})
    sec = actor.sector_concentration(t1, tickers).row(0, named=True)
    assert sec["top_sector"] == "未知"
    assert sec["sector_hhi"] == pytest.approx(1.0)


def test_compute_multiplicity_hand_computed_and_null_self_sim_fallback() -> None:
    daily = pl.DataFrame({
        "broker": ["A", "B"],
        "directional_ratio": [0.8, 0.8],
        "top5_share": [0.6, 0.6],
        "basket_self_sim": [0.4, None],  # B 首個活躍日,無「昨天」可比
    })
    out = actor.compute_multiplicity(daily)
    a, b = out.row(0, named=True), out.row(1, named=True)
    assert a["multiplicity"] == pytest.approx(1 - (0.8 + 0.6 + 0.4) / 3)
    assert b["multiplicity"] == pytest.approx(1 - (0.8 + 0.6) / 2)  # 退回兩分量


def test_rolling_identity_similarity_hand_computed() -> None:
    dates = [datetime.date(2026, 1, d) for d in range(1, 11)]
    daily = pl.DataFrame({
        "broker": ["A"] * 10, "date": dates,
        "foreign_sim_buy": [float(i) for i in range(10)],
        "foreign_sim_sell": [None] * 10,
        "fund_sim_buy": [None] * 10, "fund_sim_sell": [None] * 10,
    })
    out = actor.rolling_identity_similarity(daily, windows=(3,))
    vals = out.sort("date")["foreign_sim_buy_3d"].to_list()
    assert vals[0] == pytest.approx(0.0)          # 暖機:只有自己 1 筆
    assert vals[1] == pytest.approx((0 + 1) / 2)  # 暖機:2 筆
    assert vals[2] == pytest.approx((0 + 1 + 2) / 3)
    assert vals[9] == pytest.approx((7 + 8 + 9) / 3)  # 滿窗:最近 3 個活躍日
