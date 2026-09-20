"""分解的純函數測試:合成資料、已知真值。

關鍵是「能不能把人造的 α_b / γ_t / f 各自還原」,以及非正交時順序敏感度
確實會出現(那是要如實報告的性質,不是 bug)。
"""

from __future__ import annotations

import datetime

import polars as pl
import pytest

from ws_branch.measure import decompose


def _panel(n_branch: int, n_day: int, alpha: dict, gamma: dict,
           cell_of=lambda b: 0, f_of=None, noise=0.0) -> pl.DataFrame:
    rows = []
    d0 = datetime.date(2026, 1, 5)
    for b in range(n_branch):
        for t in range(n_day):
            y = alpha[b] + gamma[t] + (f_of[cell_of(b)] if f_of else 0.0)
            if noise:
                y += noise * ((b * 7 + t * 13) % 11 - 5) / 5.0
            rows.append({"broker": f"B{b}", "date": d0 + datetime.timedelta(days=t),
                         "_cell": cell_of(b), "y": y})
    return pl.DataFrame(rows)


def test_recovers_branch_and_day_effects_exactly() -> None:
    alpha = {0: 1.0, 1: -0.5, 2: 2.0, 3: -2.5}
    gamma = {t: 0.1 * t - 0.2 for t in range(8)}
    df = _panel(4, 8, alpha, gamma)
    out = decompose.decompose(df, y="y")
    f = out.frame
    # 成分皆去均值,故比較「相對差」
    got_a = dict(f.group_by("broker").agg(pl.col("alpha").mean()).iter_rows())
    exp_a = {f"B{b}": v - sum(alpha.values()) / 4 for b, v in alpha.items()}
    for k, v in exp_a.items():
        assert got_a[k] == pytest.approx(v, abs=1e-6)
    got_g = dict(f.group_by("date").agg(pl.col("gamma").mean()).iter_rows())
    gmean = sum(gamma.values()) / 8
    for t, v in gamma.items():
        assert got_g[datetime.date(2026, 1, 5) + datetime.timedelta(days=t)] \
            == pytest.approx(v - gmean, abs=1e-6)
    assert f["resid"].abs().max() == pytest.approx(0.0, abs=1e-6)
    assert out.var_shares["resid"] == pytest.approx(0.0, abs=1e-9)


def test_recovers_cell_effect_when_confounded_with_branch() -> None:
    # cell 由 broker 決定(完全共線):f 與 α 不可分,但兩者之和必須正確
    alpha = {0: 0.5, 1: 0.5, 2: -0.5, 3: -0.5}
    gamma = {t: 0.0 for t in range(5)}
    f_of = {0: 2.0, 1: -2.0}
    df = _panel(4, 5, alpha, gamma, cell_of=lambda b: 0 if b < 2 else 1, f_of=f_of)
    out = decompose.decompose(df, y="y")
    mu = out.frame["y"].mean()
    # f 與 α 各自不可識別(完全共線),但可加性必須成立:y = μ+f+α+γ+ε
    err = out.frame.select(
        (pl.col("y") - (mu + pl.col("f_hat") + pl.col("alpha")
                        + pl.col("gamma") + pl.col("resid"))).abs().max()).item()
    assert err == pytest.approx(0.0, abs=1e-6)
    # 且「f+α」這個可識別的組合要還原到正確值(cell 0 的兩家 = 2.0+0.5 去均值)
    combo = dict(out.frame.group_by("broker").agg(
        (pl.col("f_hat") + pl.col("alpha")).mean().alias("c")).iter_rows())
    assert combo["B0"] == pytest.approx(2.5, abs=1e-6)
    assert combo["B3"] == pytest.approx(-2.5, abs=1e-6)


def test_additivity_holds_with_noise() -> None:
    alpha = {b: 0.3 * b for b in range(6)}
    gamma = {t: -0.05 * t for t in range(12)}
    df = _panel(6, 12, alpha, gamma, cell_of=lambda b: b % 3,
                f_of={0: 1.0, 1: 0.0, 2: -1.0}, noise=0.4)
    out = decompose.decompose(df, y="y")
    mu = out.frame["y"].mean()
    err = out.frame.select(
        (pl.col("y") - (mu + pl.col("f_hat") + pl.col("alpha")
                        + pl.col("gamma") + pl.col("resid"))).abs().max()).item()
    assert err == pytest.approx(0.0, abs=1e-8)
    assert out.iterations < 50  # 應收斂,不是跑滿上限


def test_sequential_r2_order_matters_when_collinear() -> None:
    # cell 與 broker 共線 → 兩個順序的增量 R² 必然不同,這是要報告的性質
    alpha = {0: 1.0, 1: 1.0, 2: -1.0, 3: -1.0}
    df = _panel(4, 6, alpha, {t: 0.0 for t in range(6)},
                cell_of=lambda b: 0 if b < 2 else 1, f_of={0: 1.0, 1: -1.0})
    a = decompose.sequential_r2(df, y="y", order=["f", "alpha", "gamma"])
    b = decompose.sequential_r2(df, y="y", order=["alpha", "f", "gamma"])
    assert a["f"] > 0.9 and a["alpha"] == pytest.approx(0.0, abs=1e-6)
    assert b["alpha"] > 0.9 and b["f"] == pytest.approx(0.0, abs=1e-6)


def test_binning_puts_out_of_range_into_end_bins_not_null() -> None:
    df = pl.DataFrame({"g": [-99.0, 0.5, 1.5, 2.5, 99.0], "n": [1.0] * 5})
    out = decompose.add_conditioning_bins(
        df, size_col="g", breadth_col="n", size_edges=[1.0, 2.0],
        breadth_edges=[10.0])
    assert out["_sbin"].to_list() == [0, 0, 1, 2, 2]
    assert out["_cell"].null_count() == 0


def test_raises_when_not_converged() -> None:
    """2026-09-20 實付事故的回歸測試。

    首版上限 50 次,真實面板需 ~345 次,未收斂仍靜默回傳 → α 的變異占比
    少算 6-7 個百分點(top5_share 71.9% vs 收斂值 78.3%)。**不平衡面板**
    才是收斂慢的真實情境(平衡面板一輪去中心化就收斂),故此處刻意挖洞。
    """
    alpha = {b: 0.1 * b for b in range(8)}
    df = _panel(8, 20, alpha, {t: 0.02 * t for t in range(20)},
                cell_of=lambda b: b % 4, f_of={i: 0.3 * i for i in range(4)},
                noise=0.3)
    # 挖成不平衡:每個席位隨機缺不同天數
    df = df.with_columns(
        ((pl.col("broker").str.slice(1).cast(pl.Int32) * 3
          + pl.col("date").dt.day()) % 7).alias("_k")).filter(pl.col("_k") > 0).drop("_k")
    with pytest.raises(RuntimeError, match="未收斂"):
        decompose.decompose(df, y="y", max_iter=2)
    # strict=False 時降為警示並回傳(研究階段探路用)
    out = decompose.decompose(df, y="y", max_iter=2, strict=False)
    assert out.iterations == 2
    # 給足迭代就要收斂,且與上限無關
    conv = decompose.decompose(df, y="y")
    assert conv.iterations < 1_000
