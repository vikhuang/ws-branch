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
    assert "至少 2,060 張必須在 1 家外資席位之外" in out   # 測試 cohort 只有 8440
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


def test_one_sided_seat_shows_zero_two_sidedness() -> None:
    # 只賣不買:min/max = 0/50,000 = 0.00(真的單邊),不是 null
    out = _render()
    row = next(l for l in out.splitlines() if l.startswith("元大-民雄"))
    assert row.rstrip().endswith("0.00")


def test_footer_is_always_last_line() -> None:
    assert _render().rstrip().endswith(stock_readbook.FOOTER)


def test_cohort_size_in_text_follows_declared_cohort() -> None:
    # 寫死「11 家」會在名單改版時失真;必須跟 cohort_codes 走
    out = stock_readbook.render(_t1(), _bounds(), pl.DataFrame([]), symbol_id="3450",
                                date=D, cohort_codes=frozenset({"8440", "1480", "1440"}))
    assert "3 家外資席位之外" in out


def test_buy_side_listed_before_sell_side() -> None:
    out = _render()
    assert out.index("買方:") < out.index("賣方:")


def _salience_day() -> pl.DataFrame:
    return pl.DataFrame({
        "broker": ["8440", "9200"],
        "salience": [0.0146, 0.0064], "sal_mean": [0.0031, 0.0045],
        "salience_z": [3.1, 0.5], "stock_gross_z": [2.4, -0.02],
        "participation_rate": [1.0, 1.0],
        "denominator_effect": [False, True]})


def test_salience_columns_appear_only_when_supplied() -> None:
    plain = _render()
    assert "佔席位本子" not in plain
    withsal = stock_readbook.render(
        _t1(), _bounds(), pl.DataFrame([]), symbol_id="3450", date=D,
        cohort_codes=COHORT, salience_day=_salience_day())
    assert "佔席位本子" in withsal and "相對z" in withsal and "金額z" in withsal


def test_denominator_effect_is_called_out_inline() -> None:
    """§7:salience 上升不得直接稱為新增資金;分母效應必須就地標註。"""
    out = stock_readbook.render(
        _t1(), _bounds(), pl.DataFrame([]), symbol_id="3450", date=D,
        cohort_codes=COHORT, salience_day=_salience_day())
    kaiji = next(l for l in out.splitlines() if l.startswith("凱基"))
    assert "席位本子縮水" in kaiji
    jpm = next(l for l in out.splitlines() if l.startswith("摩根大通"))
    assert "席位本子縮水" not in jpm


def test_missing_salience_history_shows_dash_not_blank() -> None:
    """z 為 null = 過去沒碰過,顯示『—』並保留參與率,不留空白。"""
    sal = _salience_day().with_columns(
        pl.lit(None, dtype=pl.Float64).alias("salience_z"),
        pl.lit(None, dtype=pl.Float64).alias("stock_gross_z"))
    out = stock_readbook.render(
        _t1(), _bounds(), pl.DataFrame([]), symbol_id="3450", date=D,
        cohort_codes=COHORT, salience_day=sal)
    jpm = next(l for l in out.splitlines() if l.startswith("摩根大通"))
    assert "—" in jpm and "1.00" in jpm      # z 為破折號,參與率仍在


def test_known_hidden_foreign_seat_is_marked_not_as_domestic_hq() -> None:
    """9A81 永豐金-匯立:不在 cohort,但不得被標成「總公司/分行」——user 2026-09-21
    決定不入 cohort 的代價(其量落在下界內)必須在讀本上看得見。"""
    t1 = pl.DataFrame({
        "broker": ["8440", "9A81"], "broker_name": ["摩根大通", "永豐金-匯立"],
        "buy_sh": [400_000.0, 100_000.0], "sell_sh": [0.0, 0.0]})
    out = _render(t1=t1)
    assert "外資客戶*" in out and "*外資客戶:" in out
    assert "*外資客戶:" not in _render()   # 沒有這類席位時不印註腳


def _state_day() -> pl.DataFrame:
    return pl.DataFrame({
        "broker": ["8440", "9200"],
        "top5_share": [0.213, 0.270], "top5_share_trait": [0.289, None],
        "top5_share_day": [-0.002, -0.002], "top5_share_state": [-1.4, None],
        "top5_share_n": [60, 5],
        "directional_ratio": [0.139, 0.068], "directional_ratio_trait": [0.072, None],
        "directional_ratio_day": [0.002, 0.002], "directional_ratio_state": [1.0, None],
        "directional_ratio_n": [60, 5]})


def _state_block(out: str) -> list[str]:
    lines = out.splitlines()
    start = next(i for i, l in enumerate(lines) if l.startswith("席位性格"))
    return lines[start:]


def test_seat_state_block_appears_only_when_supplied() -> None:
    assert "席位性格" not in _render()
    out = stock_readbook.render(_t1(), _bounds(), pl.DataFrame([]), symbol_id="3450",
                                date=D, cohort_codes=COHORT, seat_state_day=_state_day())
    assert "席位性格 / 市況 / 狀態" in out and "只用 ≤ 當日資料" in out
    row = next(l for l in _state_block(out) if l.startswith("摩根大通"))
    assert "0.213" in row and "0.289" in row and "-1.4" in row and "+1.0" in row


def test_seat_state_missing_history_shows_dash_not_extreme() -> None:
    """歷史不足的席位:平常/特有z 顯示『—』,不能出現數字或空白。"""
    out = stock_readbook.render(_t1(), _bounds(), pl.DataFrame([]), symbol_id="3450",
                                date=D, cohort_codes=COHORT, seat_state_day=_state_day())
    row = next(l for l in _state_block(out) if l.startswith("凱基"))
    assert row.count("—") >= 4 and "0.270" in row
    assert "『—』= 歷史不足" in out
