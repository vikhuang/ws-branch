"""salience 的純函數測試:合成資料、手算答案。

重點在規格 §7 點名的四個陷阱:
零不是缺值、歷史不足不得標成最高異常、分母效應、two_sidedness 不是當沖。
"""

from __future__ import annotations

import datetime

import polars as pl
import pytest

from ws_branch.measure import salience

D0 = datetime.date(2026, 1, 5)


def _days(n: int) -> list[datetime.date]:
    return [D0 + datetime.timedelta(days=i) for i in range(n)]


def _t1(rows: list[tuple[str, str, int, float, float]]) -> pl.DataFrame:
    """(broker, symbol, day_idx, buy_dollar, sell_dollar)"""
    return pl.DataFrame({
        "broker": [r[0] for r in rows], "symbol_id": [r[1] for r in rows],
        "date": [D0 + datetime.timedelta(days=r[2]) for r in rows],
        "buy_dollar": [r[3] for r in rows], "sell_dollar": [r[4] for r in rows]})


def _branch_day(rows: list[tuple[str, int, float]]) -> pl.DataFrame:
    return pl.DataFrame({
        "broker": [r[0] for r in rows],
        "date": [D0 + datetime.timedelta(days=r[1]) for r in rows],
        "gross_amt": [r[2] for r in rows]})


# ── daily_salience ────────────────────────────────────────────────


def test_salience_and_signed_contrib_hand_computed() -> None:
    t1 = _t1([("A", "3450", 0, 60.0, 40.0)])
    bd = _branch_day([("A", 0, 1_000.0)])
    r = salience.daily_salience(t1, bd).row(0, named=True)
    assert r["stock_gross"] == pytest.approx(100.0)
    assert r["salience"] == pytest.approx(0.10)          # 100 / 1,000
    assert r["signed_contrib"] == pytest.approx(0.02)    # (60−40) / 1,000
    assert r["two_sidedness"] == pytest.approx(40 / 60)


def test_two_sidedness_null_when_both_sides_zero() -> None:
    """§7 明令:兩側皆零 → null,不是 0。"""
    t1 = _t1([("A", "3450", 0, 0.0, 0.0)])
    r = salience.daily_salience(t1, _branch_day([("A", 0, 1_000.0)])).row(0, named=True)
    assert r["two_sidedness"] is None
    assert r["salience"] == pytest.approx(0.0)   # 但 salience 是真實的零


def test_one_sided_flow_two_sidedness_is_zero_not_null() -> None:
    t1 = _t1([("A", "3450", 0, 100.0, 0.0)])
    r = salience.daily_salience(t1, _branch_day([("A", 0, 500.0)])).row(0, named=True)
    assert r["two_sidedness"] == pytest.approx(0.0)


# ── build_pair_panel:零不是缺值 ───────────────────────────────────


def test_panel_fills_untraded_days_with_real_zero() -> None:
    """席位當天有活動但沒碰這檔 = 真實的零,必須入 panel(否則 baseline 高估)。"""
    t1 = _t1([("A", "3450", 0, 100.0, 0.0), ("A", "3450", 2, 100.0, 0.0)])
    bd = _branch_day([("A", i, 1_000.0) for i in range(4)])
    daily = salience.daily_salience(t1, bd)
    panel = salience.build_pair_panel(daily, bd, symbol_id="3450").sort("date")
    assert panel.height == 4                             # 4 個活躍日全在
    assert panel["salience"].to_list() == [0.1, 0.0, 0.1, 0.0]
    assert panel["traded"].to_list() == [True, False, True, False]


def test_panel_excludes_days_branch_was_inactive() -> None:
    """席位完全沒活動的日子 = 無定義,不得補零(那會稀釋 baseline)。"""
    t1 = _t1([("A", "3450", 0, 100.0, 0.0)])
    bd = _branch_day([("A", 0, 1_000.0), ("A", 2, 1_000.0)])   # 第 1 天沒活動
    daily = salience.daily_salience(t1, bd)
    panel = salience.build_pair_panel(daily, bd, symbol_id="3450")
    assert panel.height == 2 and set(panel["date"]) == {D0, D0 + datetime.timedelta(days=2)}


def test_panel_only_covers_brokers_that_touched_the_stock() -> None:
    t1 = _t1([("A", "3450", 0, 100.0, 0.0)])
    bd = _branch_day([("A", 0, 1_000.0), ("B", 0, 900.0)])
    panel = salience.build_pair_panel(salience.daily_salience(t1, bd), bd,
                                      symbol_id="3450")
    assert set(panel["broker"]) == {"A"}


def test_panel_leaves_days_outside_universe_null_not_zero() -> None:
    """該股不在 universe 的日子(未上市/已下市)= 無定義 → null,不得補零。

    2026-09-21 複查:首版對席位活躍日一律補零,中途上市的股票會被灌進
    上市前的假零,拉低 baseline。
    """
    t1 = _t1([("A", "3450", 2, 100.0, 0.0)])
    bd = _branch_day([("A", i, 1_000.0) for i in range(4)])
    uni = pl.DataFrame({"symbol_id": ["3450"] * 2,
                        "date": [D0 + datetime.timedelta(days=i) for i in (2, 3)]})
    panel = (salience.build_pair_panel(salience.daily_salience(t1, bd), bd,
                                       symbol_id="3450", universe=uni)
             .sort("date"))
    assert panel.height == 4
    assert panel["salience"].to_list() == [None, None, 0.1, 0.0]
    assert panel["traded"].to_list() == [None, None, True, False]
    # 無定義日不得進 baseline:第 3 天的落後歷史只有第 2 天一筆
    hist = salience.pair_history(panel, window=60, min_periods=1)
    assert hist.sort("date")["history_n"].to_list() == [0, 0, 0, 1]


def test_panel_rejects_trade_outside_universe() -> None:
    """有成交卻不在 universe = 分子沒套 gate,要當場 raise(B 類 → A 類)。"""
    t1 = _t1([("A", "0050", 0, 100.0, 0.0)])
    bd = _branch_day([("A", 0, 1_000.0)])
    uni = pl.DataFrame({"symbol_id": ["3450"], "date": [D0]})
    with pytest.raises(ValueError, match="未套 gate"):
        salience.build_pair_panel(salience.daily_salience(t1, bd), bd,
                                  symbol_id="0050", universe=uni)


# ── pair_history / pair_anomaly ───────────────────────────────────


def _panel_from(sal_by_day: list[float], branch_gross: float = 1_000.0):
    rows = [("A", "3450", i, s * branch_gross, 0.0)
            for i, s in enumerate(sal_by_day) if s > 0]
    bd = _branch_day([("A", i, branch_gross) for i in range(len(sal_by_day))])
    daily = salience.daily_salience(_t1(rows), bd)
    return salience.build_pair_panel(daily, bd, symbol_id="3450"), bd


def test_history_is_strictly_lagged_and_includes_zeros() -> None:
    panel, _ = _panel_from([0.1, 0.0, 0.1, 0.0, 0.2])
    h = salience.pair_history(panel, window=4, min_periods=1).sort("date")
    # 第 5 天的落後均值 = mean(0.1, 0, 0.1, 0) = 0.05(含零!)
    assert h["sal_mean"].to_list()[4] == pytest.approx(0.05)
    assert h["sal_mean"].to_list()[0] is None            # 首日無歷史
    assert h["participation_rate"].to_list()[4] == pytest.approx(0.5)


def test_conditional_mean_differs_from_unconditional() -> None:
    """salience 無條件均值 = 參與率 × 條件規模——兩者分開讀。"""
    panel, _ = _panel_from([0.1, 0.0, 0.1, 0.0, 0.2])
    h = salience.pair_history(panel, window=4, min_periods=1).sort("date").row(4, named=True)
    assert h["salience_cond_mean"] == pytest.approx(0.1)   # 只算有交易的兩天
    assert h["sal_mean"] == pytest.approx(0.05)
    assert h["sal_mean"] == pytest.approx(
        h["participation_rate"] * h["salience_cond_mean"])


def test_insufficient_history_returns_null_not_max_anomaly() -> None:
    """§7 明令:新 pair / 新席位 / 新上市股票不得因沒有歷史就標成最高異常。"""
    panel, _ = _panel_from([0.0, 0.0, 0.5])
    out = salience.pair_anomaly(
        salience.pair_history(panel, window=60, min_periods=20)).sort("date")
    assert out["salience_z"].drop_nulls().len() == 0
    assert out["history_n"].to_list()[-1] == 2           # 歷史筆數要揭露


def test_denominator_effect_flags_shrinking_branch() -> None:
    """席位總額縮水 → salience 升但該股絕對金額降;§7 點名的陷阱。"""
    # 前 4 天:席位 1,000,該股 100(salience 0.10)
    rows = [("A", "3450", i, 100.0, 0.0) for i in range(4)]
    bd = [("A", i, 1_000.0) for i in range(4)]
    # 第 5 天:席位縮到 300,該股只有 60 → salience 0.20(升),金額 60(降)
    rows.append(("A", "3450", 4, 60.0, 0.0))
    bd.append(("A", 4, 300.0))
    daily = salience.daily_salience(_t1(rows), _branch_day(bd))
    panel = salience.build_pair_panel(daily, _branch_day(bd), symbol_id="3450")
    out = salience.pair_anomaly(
        salience.pair_history(panel, window=4, min_periods=1)).sort("date").row(4, named=True)
    assert out["salience"] == pytest.approx(0.2) and out["sal_mean"] == pytest.approx(0.1)
    assert out["salience_ratio"] == pytest.approx(2.0)
    assert out["stock_gross"] < out["gross_mean"]        # 絕對金額其實變少
    assert out["denominator_effect"] is True


def test_no_denominator_effect_when_both_rise() -> None:
    # 歷史要有變異,否則 sd=0 → z 依設計回 null(見 test_zero_variance)
    rows = [("A", "3450", i, g, 0.0) for i, g in enumerate([80.0, 100.0, 120.0, 100.0])]
    bd = [("A", i, 1_000.0) for i in range(4)]
    rows.append(("A", "3450", 4, 400.0, 0.0))
    bd.append(("A", 4, 1_000.0))
    daily = salience.daily_salience(_t1(rows), _branch_day(bd))
    panel = salience.build_pair_panel(daily, _branch_day(bd), symbol_id="3450")
    out = salience.pair_anomaly(
        salience.pair_history(panel, window=4, min_periods=1)).sort("date").row(4, named=True)
    assert out["denominator_effect"] is False
    assert out["salience_z"] is not None and out["stock_gross_z"] is not None


def test_zero_variance_history_returns_null_z_not_inf() -> None:
    """歷史完全無變異 → z 無定義,回 null 而非 inf(與 baseline 模組同規矩)。"""
    panel, _ = _panel_from([0.1, 0.1, 0.1, 0.1, 0.3])
    out = salience.pair_anomaly(
        salience.pair_history(panel, window=4, min_periods=1)).sort("date").row(4, named=True)
    assert out["salience_z"] is None
    assert out["salience_ratio"] == pytest.approx(3.0)   # ratio 仍可讀
