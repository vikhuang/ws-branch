"""會計界限與 universe gate 的純函數測試:合成資料、手算答案。

涵蓋架構文件 §12 點名的案例:界限退化為點、寬界限、F=0、cohort 外與未觀測
量的區分、輸入不一致不得靠 clipping 掩蓋。
"""

from __future__ import annotations

import datetime

import polars as pl
import pytest

from ws_branch.measure import accounting, universe

D = datetime.date(2026, 9, 14)


def _row(cohort: float, official: float, market: float) -> pl.DataFrame:
    return pl.DataFrame({"cohort_sh": [cohort], "official_foreign_sh": [official],
                         "market_total_sh": [market]})


def _bounds(cohort: float, official: float, market: float) -> dict:
    return accounting.flow_bounds(
        _row(cohort, official, market), cohort_col="cohort_sh",
        official_col="official_foreign_sh", market_col="market_total_sh",
        prefix="f").row(0, named=True)


def test_bounds_typical_case_聯鈞_914_buy() -> None:
    # 聯鈞 2026-09-14 買方(張):cohort 1,193、官方外資 3,142、全市場 11,908
    r = _bounds(1_193, 3_142, 11_908)
    assert r["f_x_lo"] == pytest.approx(0.0)          # S+F < V,下界退到 0
    assert r["f_x_hi"] == pytest.approx(1_193)        # min(S,F)=S
    assert r["f_y_lo"] == pytest.approx(3_142 - 1_193)  # 至少 1,949 張在 cohort 外
    assert r["f_y_hi"] == pytest.approx(3_142)        # min(F, V-S)=F
    assert r["f_cov_lo"] == pytest.approx(0.0)
    assert r["f_cov_hi"] == pytest.approx(1_193 / 3_142)  # 真 coverage 上界 ≈0.38
    assert r["f_bounds_ok"]


def test_bounds_degenerate_to_point() -> None:
    # S+F-V = S 時(F=V,官方外資吃下全部成交量)→ 下界=上界=S,唯一識別
    r = _bounds(400, 1_000, 1_000)
    assert r["f_x_lo"] == pytest.approx(400) and r["f_x_hi"] == pytest.approx(400)
    assert r["f_x_width"] == pytest.approx(0.0)
    assert r["f_cov_lo"] == pytest.approx(0.4) and r["f_cov_hi"] == pytest.approx(0.4)


def test_bounds_wide_case() -> None:
    # S 與 F 都是 V 的一半:X ∈ [0, 500],最寬的情況
    r = _bounds(500, 500, 1_000)
    assert r["f_x_lo"] == pytest.approx(0.0) and r["f_x_hi"] == pytest.approx(500)
    assert r["f_x_width"] == pytest.approx(500)


def test_bounds_no_official_foreign_gives_null_coverage() -> None:
    # F=0:X 只能是 0,coverage 未定義(不得回傳 0 或 1 混淆)
    r = _bounds(300, 0, 1_000)
    assert r["f_x_hi"] == pytest.approx(0.0)
    assert r["f_cov_lo"] is None and r["f_cov_hi"] is None


def test_bounds_inconsistent_input_flagged_not_clipped() -> None:
    # F > V(口徑不一致):界限照算,但 bounds_ok=False 供呼叫端擋下發布
    r = _bounds(100, 2_000, 1_000)
    assert not r["f_bounds_ok"]
    assert r["f_x_lo"] == pytest.approx(1_100)  # max(0, S+F-V) 沒被 clip 成 <= S


def test_residual_other_is_identified_not_retail() -> None:
    df = pl.DataFrame({"V": [11_908.0], "F": [3_142.0], "U": [74.0],
                       "P": [141.0], "H": [122.0]})
    out = accounting.actor_residual(df, market_col="V",
                                    bucket_cols=["F", "U", "P", "H"],
                                    out="other_actor_sh").row(0, named=True)
    assert out["other_actor_sh"] == pytest.approx(11_908 - 3_142 - 74 - 141 - 122)
    assert out["other_actor_sh_ok"]


def test_residual_negative_flags_bucket_inconsistency() -> None:
    df = pl.DataFrame({"V": [100.0], "F": [80.0], "U": [50.0]})
    out = accounting.actor_residual(df, market_col="V", bucket_cols=["F", "U"],
                                    out="other").row(0, named=True)
    assert out["other"] == pytest.approx(-30.0) and not out["other_ok"]


def test_unobserved_flow_separates_from_cohort_outside() -> None:
    # V=11,908、T1 觀測=11,908 → 未觀測 0(聯鈞 9/14 實測閉環)
    df = pl.DataFrame({"V": [11_908.0], "obs": [11_908.0]})
    out = accounting.unobserved_flow(df, market_col="V", observed_col="obs",
                                     out="unobs").row(0, named=True)
    assert out["unobs"] == pytest.approx(0.0) and out["unobs_ok"]
    # 未觀測量(鉅額)為正,與「cohort 外」是兩件事:此處 V 比 T1 多 5,000 股
    more = accounting.unobserved_flow(
        pl.DataFrame({"V": [105_000.0], "obs": [100_000.0]}), market_col="V",
        observed_col="obs", out="unobs").row(0, named=True)
    assert more["unobs"] == pytest.approx(5_000.0) and more["unobs_ok"]


# ── universe gate ──────────────────────────────────────────────────


def _stock_attr(rows: list[tuple[str, str]]) -> pl.DataFrame:
    return pl.DataFrame({"coid": [r[0] for r in rows],
                         "mdate": [D] * len(rows),
                         "stktp_c": [r[1] for r in rows]})


def test_universe_keeps_only_common_stock_types() -> None:
    uni = universe.stock_universe(_stock_attr([
        ("3450", "普通股"), ("9105", "普通股-海外"), ("0050", "ETF"),
        ("IX0001", "指數"), ("2881B", "特別股"), ("00919", "國外ETF"),
        ("9110", "台灣存託憑證"), ("01007T", "REIT"),
    ]))
    assert set(uni["symbol_id"]) == {"3450", "9105"}


def test_apply_universe_is_date_effective() -> None:
    # 同一檔股票在 D 在 universe、在 D+1 不在(下市)→ 只有 D 的列留下
    uni = pl.DataFrame({"symbol_id": ["3450"], "date": [D]})
    df = pl.DataFrame({"symbol_id": ["3450", "3450"],
                       "date": [D, D + datetime.timedelta(days=1)],
                       "amt": [1.0, 2.0]})
    kept = universe.apply_universe(df, uni)
    assert kept.height == 1 and kept[0, "date"] == D


def test_universe_exclusion_report_surfaces_dropped_amount() -> None:
    uni = pl.DataFrame({"symbol_id": ["3450"], "date": [D]})
    df = pl.DataFrame({"symbol_id": ["3450", "IX0001"], "date": [D, D],
                       "amt": [100.0, 12_765.0]})
    rep = universe.universe_exclusion_report(df, uni, "amt").row(0, named=True)
    assert rep["excluded_rows"] == 1 and rep["excluded_amount"] == pytest.approx(12_765.0)


def test_foreign_cohort_excludes_local_institutional_seat() -> None:
    # 國票-敦北法人(7790)是本土法人部,不得進外資 cohort
    assert "7790" not in universe.FOREIGN_BROKER_CODES
    assert len(universe.FOREIGN_BROKER_CODES) == 12
    assert "8440" in universe.FOREIGN_BROKER_CODES  # 摩根大通


def test_unobserved_flow_tolerates_odd_lot_remainder() -> None:
    # TEJ vol 只計整張、broker_tx 含零股:119,153 股 vs 119 張(A5 實例)
    # 差 −153 股是尾差不是破裂;2026 全年不加容差會誤判 96.8% 股票日
    df = pl.DataFrame({"V": [119_000.0], "obs": [119_153.0]})
    out = accounting.unobserved_flow(df, market_col="V", observed_col="obs",
                                     out="unobs").row(0, named=True)
    assert out["unobs"] == pytest.approx(-153.0)  # 差額照實存,不 clip
    assert out["unobs_ok"]
    # 超過 1 張才是真破裂
    broken = accounting.unobserved_flow(
        pl.DataFrame({"V": [119_000.0], "obs": [121_000.0]}), market_col="V",
        observed_col="obs", out="unobs").row(0, named=True)
    assert not broken["unobs_ok"]
