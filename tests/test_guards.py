"""guards 的測試:每條都對應一次實付事故(見模組 docstring 的三分類)。"""

from __future__ import annotations

import datetime

import polars as pl
import pytest

from ws_branch.measure import guards

D = datetime.date(2026, 9, 14)


def test_require_columns_names_the_caller_and_missing() -> None:
    df = pl.DataFrame({"a": [1]})
    with pytest.raises(ValueError, match=r"myfn.*\['b', 'c'\]"):
        guards.require_columns(df, ["a", "b", "c"], who="myfn")


def test_require_unique_key_counts_duplicates() -> None:
    """2026-09-21:rolling_baseline 遇重複鍵 9 列進 9 列出,窗長悄悄縮短。"""
    df = pl.DataFrame({"b": ["A", "A", "B"], "d": [D, D, D]})
    with pytest.raises(ValueError, match="1 列重複鍵"):
        guards.require_unique_key(df, ["b", "d"], who="w")
    guards.require_unique_key(df.head(1), ["b", "d"], who="w")   # 不得誤報


def test_require_no_nulls_targets_flag_columns() -> None:
    """2026-09-20:t3b 有 112 列旗標為 null,下游 `~flag` 會靜默漏列。"""
    df = pl.DataFrame({"flag": [True, None]}, schema={"flag": pl.Boolean})
    with pytest.raises(ValueError, match="含 null"):
        guards.require_no_nulls(df, ["flag"], who="w")


def test_require_non_negative() -> None:
    df = pl.DataFrame({"amt": [1.0, -0.5]})
    with pytest.raises(ValueError, match="負值"):
        guards.require_non_negative(df, ["amt"], who="w")


def test_require_covered_catches_silent_inner_join_loss() -> None:
    """2026-09-21:basket 對交易日曆外的日期靜默丟掉,0 列輸出無訊息。"""
    flow = pl.DataFrame({"date": [D, D + datetime.timedelta(days=1)]})
    cal = pl.DataFrame({"date": [D]})
    with pytest.raises(ValueError, match="不在對照表中"):
        guards.require_covered(flow, cal, ["date"], who="w")
    guards.require_covered(flow.head(1), cal, ["date"], who="w")


def test_require_same_universe_is_covered_with_a_hint() -> None:
    """2026-09-21:Phase 3 分子含 ETF 而分母已 gate,salience 口徑不一致。"""
    num = pl.DataFrame({"symbol_id": ["3450", "0050"], "date": [D, D]})
    uni = pl.DataFrame({"symbol_id": ["3450"], "date": [D]})
    with pytest.raises(ValueError, match="universe gate"):
        guards.require_same_universe(num, uni, keys=["symbol_id", "date"], who="w")


def test_require_non_empty() -> None:
    with pytest.raises(ValueError, match="輸入為空"):
        guards.require_non_empty(pl.DataFrame({"a": []}), who="w")


def test_warn_if_lossy_join_both_directions() -> None:
    with pytest.raises(ValueError, match="列數增加"):
        guards.warn_if_lossy_join(10, 12, who="w")
    with pytest.raises(ValueError, match="列數減少"):
        guards.warn_if_lossy_join(10, 9, who="w")
    guards.warn_if_lossy_join(10, 9, who="w", tolerance=0.2)   # 容許範圍內
    guards.warn_if_lossy_join(10, 10, who="w")


def test_salience_rejects_ungated_numerator_when_universe_given() -> None:
    """B 類降級成 A 類的實例:把 universe 當參數傳進來,錯就變可檢。"""
    from ws_branch.measure import salience
    t1 = pl.DataFrame({"broker": ["A", "A"], "symbol_id": ["3450", "0050"],
                       "date": [D, D], "buy_dollar": [1.0, 1.0],
                       "sell_dollar": [0.0, 0.0]})
    bd = pl.DataFrame({"broker": ["A"], "date": [D], "gross_amt": [10.0]})
    uni = pl.DataFrame({"symbol_id": ["3450"], "date": [D]})
    with pytest.raises(ValueError, match="universe gate"):
        salience.daily_salience(t1, bd, universe=uni)
    # 不傳 universe 時維持舊行為(不強制,但 docstring 標明風險)
    assert salience.daily_salience(t1, bd).height == 2
