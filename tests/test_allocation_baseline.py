"""allocation / baseline 的純函數測試。

涵蓋架構文件 §12 點名的向量案例:同市場向量但買賣 cosine 不同、零 norm、
完整資料下無重疊 basket 為 0、缺資料為 null;以及 §4.5 的時間紀律
(baseline 嚴格落後、最小樣本不足回傳 null)。
"""

from __future__ import annotations

import datetime
import math

import polars as pl
import pytest

from ws_branch.measure import allocation, baseline

D1 = datetime.date(2026, 2, 11)   # 農曆年前最後交易日
D2 = datetime.date(2026, 2, 23)   # 年後第一個交易日(相隔 12 個曆日)
D3 = datetime.date(2026, 2, 24)


def _flow(rows: list[tuple[str, str, datetime.date, float]]) -> pl.DataFrame:
    return pl.DataFrame(
        {"broker": [r[0] for r in rows], "symbol_id": [r[1] for r in rows],
         "date": [r[2] for r in rows], "gross": [r[3] for r in rows]})


# ── 全 universe cosine ────────────────────────────────────────────


def test_cosine_uses_full_universe_norm_not_support_only() -> None:
    """參考向量的 norm 必須含分點沒碰的股票(這正是 O1 用錯的地方)。"""
    flow = _flow([("A", "s1", D1, 3.0)])
    ref = pl.DataFrame({"symbol_id": ["s1", "s2"], "date": [D1, D1],
                        "ref": [4.0, 3.0]})
    got = allocation.amount_cosine(flow, ref, amount_col="gross", ref_col="ref",
                                   out="cos").row(0, named=True)["cos"]
    # dot=12, ‖a‖=3, ‖r_full‖=5 → 0.8(若誤用 support-only 則 ‖r‖=4 → 1.0)
    assert got == pytest.approx(0.8)
    assert got != pytest.approx(1.0)


def test_cosine_buy_and_sell_differ_on_same_market_vector() -> None:
    """同一份市場參考向量,買向量與賣向量的 cosine 不同(§7 不預設對稱)。"""
    flow = pl.DataFrame({
        "broker": ["A", "A"], "symbol_id": ["s1", "s2"], "date": [D1, D1],
        "buy": [10.0, 0.0], "sell": [0.0, 10.0]})
    ref = pl.DataFrame({"symbol_id": ["s1", "s2"], "date": [D1, D1],
                        "m": [10.0, 1.0]})
    b = allocation.amount_cosine(flow, ref, amount_col="buy", ref_col="m",
                                 out="c").row(0, named=True)["c"]
    s = allocation.amount_cosine(flow, ref, amount_col="sell", ref_col="m",
                                 out="c").row(0, named=True)["c"]
    n = math.sqrt(101.0)
    assert b == pytest.approx(10 / n) and s == pytest.approx(1 / n)
    assert b != pytest.approx(s)


def test_cosine_zero_norm_returns_null_not_zero() -> None:
    flow = _flow([("A", "s1", D1, 0.0)])          # 該側完全無成交
    ref = pl.DataFrame({"symbol_id": ["s1"], "date": [D1], "ref": [1.0]})
    out = allocation.amount_cosine(flow, ref, amount_col="gross",
                                   ref_col="ref", out="cos")
    assert out.height == 0  # 無正值列 → 不產生該分點日,left join 後為 null


# ── basket self-similarity ────────────────────────────────────────


def test_basket_prev_day_from_calendar_spans_lunar_new_year() -> None:
    """前一交易日相隔 12 個曆日(農曆年)仍要接得上——固定 7/14 天緩衝會斷。"""
    cal = allocation.prev_trading_day_map([D1, D2, D3])
    flow = _flow([("A", "s1", D1, 3.0), ("A", "s2", D1, 4.0),
                  ("A", "s1", D2, 3.0), ("A", "s2", D2, 4.0)])
    out = allocation.basket_self_similarity(flow, cal)
    assert out.height == 1 and out[0, "date"] == D2
    assert out[0, "basket_self_sim"] == pytest.approx(1.0)  # 完全相同的籃子


def test_basket_no_overlap_is_zero_missing_day_is_absent() -> None:
    """完整兩日但籃子不重疊 = 0;前一交易日沒交易 = 不產生列(語意不同)。"""
    cal = allocation.prev_trading_day_map([D1, D2, D3])
    flow = _flow([("A", "s1", D1, 5.0), ("A", "s2", D2, 5.0),  # A:兩日皆有,無重疊
                  ("B", "s1", D2, 5.0)])                        # B:前一日無交易
    out = allocation.basket_self_similarity(flow, cal)
    a = out.filter(pl.col("broker") == "A")
    assert a.height == 1 and a[0, "basket_self_sim"] == pytest.approx(0.0)
    assert out.filter(pl.col("broker") == "B").height == 0


# ── rolling baseline / state anomaly ──────────────────────────────


def _series(vals: list[float]) -> pl.DataFrame:
    d0 = datetime.date(2026, 1, 5)
    return pl.DataFrame({
        "broker": ["A"] * len(vals),
        "date": [d0 + datetime.timedelta(days=i) for i in range(len(vals))],
        "x": vals})


def test_baseline_is_strictly_lagged_no_lookahead() -> None:
    df = baseline.rolling_baseline(_series([1.0, 2.0, 3.0, 4.0]), value_col="x",
                                   window=3, min_periods=1)
    means = df["x_mean"].to_list()
    assert means[0] is None                      # 第一天沒有歷史
    assert means[1] == pytest.approx(1.0)        # 只用第 1 天,不含自己
    assert means[3] == pytest.approx((1 + 2 + 3) / 3)


def test_baseline_min_periods_returns_null_not_short_window() -> None:
    df = baseline.rolling_baseline(_series([1.0, 2.0, 3.0, 4.0, 5.0]),
                                   value_col="x", window=60, min_periods=3)
    assert df["x_mean"].to_list()[:3] == [None, None, None]
    assert df["x_mean"].to_list()[3] == pytest.approx(2.0)   # 用前 3 天
    assert df["x_n"].to_list()[3] == 3


def test_state_anomaly_null_when_zero_variance() -> None:
    df = baseline.rolling_baseline(_series([2.0] * 6), value_col="x",
                                   window=5, min_periods=2)
    out = baseline.state_anomaly(df, value_col="x")
    assert out["x_z"].drop_nulls().len() == 0          # sd=0 → null,不是 inf
    assert out["x_z_raw"].to_list()[-1] == pytest.approx(0.0)


def test_state_anomaly_hand_computed() -> None:
    df = baseline.rolling_baseline(_series([1.0, 3.0, 5.0, 11.0]),
                                   value_col="x", window=3, min_periods=3)
    out = baseline.state_anomaly(df, value_col="x").row(3, named=True)
    mean, sd = 3.0, pl.Series([1.0, 3.0, 5.0]).std()   # 樣本標準差 = 2.0
    assert out["x_mean"] == pytest.approx(mean) and out["x_sd"] == pytest.approx(sd)
    assert out["x_z"] == pytest.approx((11.0 - mean) / sd)


# ── 2026-09-21 複查補測:多分點交錯、靜默丟棄路徑 ─────────────────


def test_baseline_isolates_interleaved_groups() -> None:
    """巢狀 .over() 在多分點交錯、輸入亂序時仍須各算各的(首版只測單一分點)。"""
    d0 = datetime.date(2026, 1, 5)
    df = pl.DataFrame({
        "broker": ["B", "A", "B", "A", "B", "A", "B", "A"],
        "date": [d0 + datetime.timedelta(days=i // 2) for i in range(8)],
        "x": [100.0, 1.0, 100.0, 2.0, 100.0, 3.0, 100.0, 4.0]})
    out = baseline.rolling_baseline(df, value_col="x", window=3, min_periods=1)
    a = out.filter(pl.col("broker") == "A").sort("date")
    assert a["x_mean"].to_list() == [None, 1.0, 1.5, 2.0]   # 絕不混到 B 的 100
    b = out.filter(pl.col("broker") == "B").sort("date")
    assert b["x_mean"].drop_nulls().to_list() == [100.0] * 3


def test_baseline_raises_on_duplicate_keys() -> None:
    """重複 (broker, date) 會讓 row-based 窗把同一天數兩次且不報錯(家法#3)。"""
    df = _series([1.0, 2.0, 3.0])
    dup = pl.concat([df, df.head(1)])
    with pytest.raises(ValueError, match="重複"):
        baseline.rolling_baseline(dup, value_col="x", window=3, min_periods=1)


def test_basket_raises_when_calendar_does_not_cover_flow_dates() -> None:
    """flow 有日曆外的日期 = 日曆涵蓋不足;首版會靜默 inner-join 丟掉(家法#3)。"""
    cal = allocation.prev_trading_day_map([D1, D2])
    flow = _flow([("A", "s1", D2, 1.0), ("A", "s1", D3, 1.0)])   # D3 不在日曆
    with pytest.raises(ValueError, match="交易日曆涵蓋不足"):
        allocation.basket_self_similarity(flow, cal)


def test_cosine_is_deflated_by_out_of_universe_reference_rows() -> None:
    """記錄危害:reference 混入 universe 外的列會壓低所有 cosine 且不報錯——
    本函數無法自行辨識,呼叫端必須先 gate。此測試把危害寫成可執行的事實。"""
    flow = _flow([("A", "s1", D1, 3.0)])
    clean = pl.DataFrame({"symbol_id": ["s1", "s2"], "date": [D1, D1], "ref": [4.0, 3.0]})
    polluted = pl.concat([clean, pl.DataFrame(
        {"symbol_id": ["IX0001"], "date": [D1], "ref": [1_000.0]})])
    c = allocation.amount_cosine(flow, clean, amount_col="gross", ref_col="ref",
                                 out="c").row(0, named=True)["c"]
    d = allocation.amount_cosine(flow, polluted, amount_col="gross", ref_col="ref",
                                 out="c").row(0, named=True)["c"]
    assert c == pytest.approx(0.8) and d < 0.02
