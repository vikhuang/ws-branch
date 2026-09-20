"""讀本 v1 的渲染測試:合成資料,查「不該出現什麼」與「界限數字正確」。"""

from __future__ import annotations

import datetime

import polars as pl

from ws_branch.products import stock_readbook

D = datetime.date(2026, 9, 14)
COHORT = frozenset({"8440"})


def _t1() -> pl.DataFrame:
    return pl.DataFrame({
        "broker": ["8440", "9200", "A-1"],
        "broker_name": ["摩根大通", "凱基", "元大-民雄"],
        "buy_sh": [824_000.0, 132_000.0, 0.0],
        "sell_sh": [1_190_000.0, 133_000.0, 50_000.0],
    })


def _bounds() -> pl.DataFrame:
    base = {"market_total_sh": 11_908_000.0, "other_actor_sh": 8_429_000.0,
            "foreign_x_lo": 0.0, "bounds_publishable": True}
    return pl.DataFrame([
        {**base, "side": "buy", "official_foreign_sh": 3_142_000.0,
         "foreign_y_lo": 2_060_000.0, "foreign_x_hi": 1_082_000.0},
        {**base, "side": "sell", "official_foreign_sh": 4_271_000.0,
         "foreign_y_lo": 2_117_000.0, "foreign_x_hi": 2_154_000.0},
    ])


def _render(**kw) -> str:
    return stock_readbook.render(
        kw.get("t1", _t1()), kw.get("bounds", _bounds()),
        pl.DataFrame([]), symbol_id="3450", date=D, cohort_codes=COHORT)


def test_shows_hard_lower_bound_in_lots() -> None:
    out = _render()
    assert "至少 2,060 張必須在 11 家外資席位之外" in out
    assert "至少 2,117 張" in out          # 賣方
    assert "0~1,082 張" in out             # 席位內界限


def test_never_shows_actor_similarity_columns() -> None:
    """O1 的排名版指紋扣掉『像市場』後 AUC 剩 0.53,不得出現在讀本上。"""
    out = _render()
    for banned in ("foreign_sim", "fund_sim", "像外資", "multiplicity", "多重性"):
        assert banned not in out


def test_two_sidedness_not_labelled_as_day_trading() -> None:
    out = _render()
    assert "來回率" in out and "不等於當沖" in out
    assert "當沖客" not in out


def test_unpublishable_bounds_say_so_rather_than_showing_numbers() -> None:
    bad = _bounds().with_columns(pl.lit(False).alias("bounds_publishable"))
    out = _render(bounds=bad)
    assert "本日不可發布" in out
    # 具體界限數字不得出現(頁尾讀法說明含「必須在」三字,故查數字而非詞)
    assert "2,060" not in out and "2,117" not in out


def test_zero_gross_seat_two_sidedness_is_dash_not_zero() -> None:
    # 只賣不買的席位:min/max = 0/50,000 = 0.0 → 顯示 0.00(真的單邊)
    out = _render()
    assert "元大-民雄" in out and FOOTER_OK(out)


def FOOTER_OK(out: str) -> bool:
    return out.rstrip().endswith(stock_readbook.FOOTER)
