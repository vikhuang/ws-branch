"""factory 單元測試:合成資料、已知答案。"""

from __future__ import annotations

import datetime

import polars as pl

from factory import reconcile


def _totals(rows: list[tuple[str, str, int, int]]) -> pl.DataFrame:
    return pl.DataFrame(
        {"symbol_id": [r[0] for r in rows],
         "date": [datetime.date.fromisoformat(r[1]) for r in rows],
         "buy": [r[2] for r in rows], "sell": [r[3] for r in rows]})


def _px(rows: list[tuple[str, str, int]]) -> pl.DataFrame:
    return pl.DataFrame(
        {"symbol_id": [r[0] for r in rows],
         "date": [datetime.date.fromisoformat(r[1]) for r in rows],
         "vol": [r[2] for r in rows]})


def test_reconcile_pass_exact() -> None:
    t1 = _totals([("2330", "2026-01-05", 1_500_000, 1_500_000)])
    px = _px([("2330", "2026-01-05", 1500)])  # 千股
    assert reconcile(t1, px).height == 0


def test_reconcile_catches_mismatch() -> None:
    t1 = _totals([("2330", "2026-01-05", 1_600_000, 1_500_000)])  # 買方差 6.7%
    px = _px([("2330", "2026-01-05", 1500)])
    bad = reconcile(t1, px)
    assert bad.height == 1
    assert bad[0, "rel_b"] > 0.06 and bad[0, "rel_s"] < 0.001


def test_reconcile_tolerance_boundary() -> None:
    # 0.05% 差在預設容差(0.1%)內
    t1 = _totals([("2330", "2026-01-05", 1_500_750, 1_500_000)])
    px = _px([("2330", "2026-01-05", 1500)])
    assert reconcile(t1, px).height == 0
    assert reconcile(t1, px, tol=0.0001).height == 1


def test_reconcile_ignores_zero_vol_and_unmatched() -> None:
    t1 = _totals([("2330", "2026-01-05", 100_000, 100_000),
                  ("9999", "2026-01-05", 5, 5)])
    px = _px([("2330", "2026-01-05", 0)])  # 零量日排除;9999 無對應列
    assert reconcile(t1, px).height == 0
