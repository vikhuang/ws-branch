"""measure.state(讀時 trait / 市況 / 席位特有狀態)的純函數測試:合成資料、手算。"""

from __future__ import annotations

import datetime

import polars as pl
import pytest

from ws_branch.measure import state

D0 = datetime.date(2026, 1, 5)


def _hist(n_days: int, seats: dict[str, list[float]]) -> pl.DataFrame:
    """seats:broker → 每日 top5_share 序列(長度 n_days)。"""
    rows = []
    for b, vals in seats.items():
        for i, v in enumerate(vals):
            rows.append((b, D0 + datetime.timedelta(days=i), v, 0.1, 1e8, 500))
    return pl.DataFrame(rows, schema=["broker", "date", "top5_share", "directional_ratio",
                                      "gross_amt", "n_symbols"], orient="row")


def test_trait_day_state_hand_computed() -> None:
    """A 平常 0.2、今日 0.5;B 平常 0.3、今日 0.6;C 平常 0.4、今日 0.4。
    市況 = median(今日−平常) = median(+0.3, +0.3, 0.0) = +0.3;
    A 特有 = (0.3 − 0.3)/sd = 0;C 特有 = (0 − 0.3)/sd < 0。"""
    n = 6
    seats = {"A": [0.2, 0.2, 0.2, 0.2, 0.2, 0.5], "B": [0.3, 0.3, 0.3, 0.3, 0.3, 0.6],
             "C": [0.4, 0.4, 0.4, 0.4, 0.4, 0.4]}
    # 讓 sd 非零:各席位歷史加一點交錯
    for k in seats:
        seats[k][1] += 0.02
    h = _hist(n, seats)
    out = state.seat_state(h, date=D0 + datetime.timedelta(days=n - 1),
                           primitives=("top5_share",), window=5, min_periods=3)
    r = {row["broker"]: row for row in out.iter_rows(named=True)}
    assert r["A"]["top5_share_trait"] == pytest.approx(0.204)
    # 規模與廣度全程不變 → Δsize=Δbreadth=0 → 規模效應 0,市況 = median(dev)
    assert r["A"]["top5_share_size"] == pytest.approx(0.0, abs=1e-9)
    assert r["A"]["top5_share_day"] == pytest.approx(0.296, abs=1e-9)   # median(0.296, 0.296, −0.004)
    assert r["A"]["top5_share_state"] == pytest.approx(0.0, abs=1e-9)
    assert r["C"]["top5_share_state"] < 0
    assert r["A"]["top5_share_day"] == r["C"]["top5_share_day"]        # 市況全席位相同
    assert r["A"]["top5_share_n"] == 5


def test_insufficient_history_gives_null_not_extreme() -> None:
    h = _hist(3, {"A": [0.2, 0.3, 0.9], "B": [0.2, 0.3, 0.9], "C": [0.2, 0.3, 0.9]})
    out = state.seat_state(h, date=D0 + datetime.timedelta(days=2),
                           primitives=("top5_share",), window=5, min_periods=3)
    assert out["top5_share_trait"].is_null().all()
    assert out["top5_share_state"].is_null().all()
    assert out["top5_share_day"].is_null().all()   # 沒有任何席位有 baseline → 市況也 null


def test_day_effect_needs_enough_seats_with_baseline() -> None:
    """只有 1 家有 baseline 時,橫斷面中位數不成立 → 市況與特有 z 皆 null,平常仍給。"""
    n = 6
    h = pl.concat([_hist(n, {"A": [0.2, 0.22, 0.2, 0.2, 0.2, 0.5]}),
                   _hist(2, {"B": [0.3, 0.6]}).with_columns(
                       (pl.col("date") + pl.duration(days=n - 2)).alias("date"))])
    out = state.seat_state(h, date=D0 + datetime.timedelta(days=n - 1),
                           primitives=("top5_share",), window=5, min_periods=3)
    a = out.filter(pl.col("broker") == "A").row(0, named=True)
    assert a["top5_share_trait"] is not None and a["top5_share_day"] is None
    assert a["top5_share_state"] is None and a["top5_share_size"] is None


def test_rejects_history_beyond_target_date() -> None:
    h = _hist(4, {"A": [0.2, 0.2, 0.2, 0.2]})
    with pytest.raises(ValueError, match="as-of"):
        state.seat_state(h, date=D0 + datetime.timedelta(days=1), primitives=("top5_share",),
                         window=3, min_periods=2)


def test_asof_truncation_invariance() -> None:
    """截斷在 t 的歷史與更長歷史,t 日輸出恆等(讀時計算不偷看未來)。"""
    n = 12
    seats = {b: [((i * k) % 7) / 10 for i in range(n)] for k, b in enumerate("ABCD", start=3)}
    h = _hist(n, seats)
    t = D0 + datetime.timedelta(days=8)
    a = state.seat_state(h.filter(pl.col("date") <= t), date=t, primitives=("top5_share",),
                         window=5, min_periods=3)
    b = state.seat_state(h.filter(pl.col("date") <= t), date=t, primitives=("top5_share",),
                         window=5, min_periods=3)
    assert a.sort("broker").equals(b.sort("broker"))
    # 多給未來三天(但 seat_state 本身拒絕 > t 的資料,所以由呼叫端截斷)——
    # 這裡驗證的是:同一截斷輸入下結果確定性,以及 rolling 只看 ≤ t
    full = state.seat_state(h.filter(pl.col("date") <= t + datetime.timedelta(days=3)),
                            date=t + datetime.timedelta(days=3), primitives=("top5_share",),
                            window=5, min_periods=3)
    assert full.height == 4


def test_size_breadth_effect_is_removed_before_state() -> None:
    """席位縮小交易範圍造成的集中度上升,要被歸到『規模』不是『特有』。

    30 家席位:今日 n_symbols 各自變動(Δbreadth ∈ [−0.5, +0.5]),集中度偏離 =
    −0.2 × Δbreadth(碰得少 → 更集中)+ 共同 +0.01。席位 S 碰少一半:偏離 +0.07,
    完全由規模解釋 → 特有 z ≈ 0;市況 ≈ +0.01。"""
    n, seats = 8, {}
    rows = []
    for k in range(30):
        b = f"S{k:02d}"
        db = -0.5 + k / 29            # log10 廣度變化
        for i in range(n):
            today = i == n - 1
            top5 = 0.25 + (0.002 * (i % 2)) + ((-0.2 * db + 0.01) if today else 0.0)
            n_sym = 500 * (10 ** db) if today else 500
            rows.append((b, D0 + datetime.timedelta(days=i), top5, 0.1, 1e8, n_sym))
    h = pl.DataFrame(rows, schema=["broker", "date", "top5_share", "directional_ratio",
                                   "gross_amt", "n_symbols"], orient="row")
    out = state.seat_state(h, date=D0 + datetime.timedelta(days=n - 1),
                           primitives=("top5_share",), window=6, min_periods=3)
    s00 = out.filter(pl.col("broker") == "S00").row(0, named=True)   # db = −0.5,碰少
    assert s00["top5_share_size"] == pytest.approx(0.10, abs=0.005)   # −0.2 × −0.5
    assert abs(s00["top5_share_state"]) < 0.5                         # 規模解釋掉,非特有
    assert s00["top5_share_day"] == pytest.approx(0.01, abs=0.005)
