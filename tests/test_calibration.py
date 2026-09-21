"""calibration 的純函數測試:合成資料、已知答案。

重點是 §8 的三條方法論紀律:係數只在訓練期估、席位為單位 bootstrap、
時間切分不打散。
"""

from __future__ import annotations

import datetime

import polars as pl
import pytest

from ws_branch.measure import calibration as cal

D0 = datetime.date(2026, 1, 5)


def _panel(n_broker: int, n_day: int, *, pos_frac: float = 0.3,
           effect: float = 1.0, seed: int = 7) -> pl.DataFrame:
    import random
    rng = random.Random(seed)
    rows = []
    for b in range(n_broker):
        is_pos = b < int(n_broker * pos_frac)
        level = rng.gauss(0, 0.5)          # 席位固定效果(讓同席位連續日相關)
        for t in range(n_day):
            ctrl = rng.gauss(0, 1)
            rows.append({
                "broker": f"B{b:02d}", "date": D0 + datetime.timedelta(days=t),
                "label": is_pos, "ctrl": ctrl,
                "score": 0.5 * ctrl + level + (effect if is_pos else 0.0)
                + rng.gauss(0, 0.3)})
    return pl.DataFrame(rows)


def test_auc_hand_computed() -> None:
    pos, neg = pl.Series([3.0, 4.0]), pl.Series([1.0, 2.0])
    assert cal.auc(pos, neg) == pytest.approx(1.0)
    assert cal.auc(neg, pos) == pytest.approx(0.0)
    assert cal.auc(pl.Series([1.0, 2.0]), pl.Series([1.0, 2.0])) == pytest.approx(0.5)


def test_residual_coefficients_recovered() -> None:
    df = pl.DataFrame({"y": [1.0, 3.0, 5.0, 7.0], "x": [0.0, 1.0, 2.0, 3.0]})
    m = cal.fit_residual(df, target="y", controls=["x"])
    assert m.intercept == pytest.approx(1.0) and m.coefs[0] == pytest.approx(2.0)
    out = cal.apply_residual(df, m)
    assert out["y_resid"].abs().max() == pytest.approx(0.0, abs=1e-9)


def test_residual_uses_train_coefficients_on_test_not_refit() -> None:
    """§8:所有 baseline 與調參只在訓練資料完成;測試期重估 = 用到未見資料。"""
    train = pl.DataFrame({"y": [1.0, 2.0, 3.0, 4.0, 5.0],
                          "x": [0.0, 1.0, 2.0, 3.0, 4.0]})
    test = pl.DataFrame({"y": [10.0, 11.0, 12.0, 13.0],
                         "x": [0.0, 1.0, 2.0, 3.0]})   # 水準整個位移 +9
    m = cal.fit_residual(train, target="y", controls=["x"])
    applied = cal.apply_residual(test, m)["y_resid"].to_list()
    assert applied == pytest.approx([9.0, 9.0, 9.0, 9.0])   # 位移被保留 = 沒重估
    refit = cal.apply_residual(test, cal.fit_residual(test, target="y", controls=["x"]))
    assert refit["y_resid"].abs().max() == pytest.approx(0.0, abs=1e-9)  # 重估會抹平


def test_time_split_is_chronological_not_shuffled() -> None:
    df = _panel(4, 30)
    train, test = cal.time_split(df, train_frac=2 / 3)
    assert train["date"].max() < test["date"].min()
    assert train.height + test.height == df.height


def test_group_bootstrap_ci_is_wider_than_row_bootstrap() -> None:
    """§8 核心警語:同席位連續日不是獨立樣本,逐列重抽會低估不確定性。"""
    df = _panel(10, 40, effect=0.8)
    _, g_lo, g_hi = cal.auc_ci(df, score="score", label="label",
                               unit="group", n_boot=120)
    _, r_lo, r_hi = cal.auc_ci(df, score="score", label="label",
                               unit="row", n_boot=120)
    assert (g_hi - g_lo) > (r_hi - r_lo) * 2, (
        f"席位層區間 {g_hi - g_lo:.3f} 應遠寬於逐列 {r_hi - r_lo:.3f}")


def test_leave_one_group_out_detects_single_group_driving_signal() -> None:
    """若區分力靠單一席位撐起,拿掉它 AUC 必須明顯掉下來。"""
    import random
    rng = random.Random(3)
    rows = []
    for b in range(6):
        is_pos = b < 2
        # 只有 B00 有強訊號,B01 與負樣本同分佈
        lift = 3.0 if b == 0 else 0.0
        for t in range(20):
            rows.append({"broker": f"B{b:02d}",
                         "date": D0 + datetime.timedelta(days=t),
                         "label": is_pos, "score": rng.gauss(0, 1) + lift})
    df = pl.DataFrame(rows)
    out = cal.leave_one_group_out(df, score="score", label="label", group="broker")
    dropped_b00 = out.filter(pl.col("left_out") == "B00")["auc"][0]
    dropped_b01 = out.filter(pl.col("left_out") == "B01")["auc"][0]
    assert dropped_b00 < 0.65 < dropped_b01
