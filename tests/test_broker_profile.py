"""分點 profile 的渲染測試:合成 T4 v3 歷史;查每個區塊在、認識論標籤在、身份用語不在。"""

from __future__ import annotations

import datetime

import polars as pl
import pytest

from ws_branch.products import broker_profile

D0 = datetime.date(2026, 6, 1)
COHORT = frozenset({"8440"})


def _hist(n_days: int = 40, n_seats: int = 60) -> pl.DataFrame:   # 60×39 列 ≥ residual_percentile 的 min_fit
    rows = []
    for k in range(n_seats):
        b = "8440" if k == 0 else ("9A81" if k == 1 else f"B{k:02d}")
        name = "摩根大通" if k == 0 else ("永豐金-匯立" if k == 1 else f"券商{k}-分行")
        for i in range(n_days):
            d = D0 + datetime.timedelta(days=i)
            g = 1e9 * (1 + 0.1 * k) * (1 + 0.05 * ((i * 7) % 5))
            n = 300 + 10 * k + 3 * ((i * 3) % 7)
            rows.append(dict(
                broker=b, broker_name=name, date=d,
                gross_buy_amt=g * 0.6, gross_sell_amt=g * 0.4, gross_amt=g, net_amt=g * 0.2,
                n_symbols=n, top1_share=0.08 + 0.001 * (i % 4), top5_share=0.2 + 0.002 * (i % 5),
                directional_ratio=0.05 + 0.001 * (i % 3), basket_self_sim=0.6 + 0.01 * (i % 2),
                cos_market_buy=0.7 + 0.001 * (i % 6), cos_market_sell=0.65 + 0.001 * (i % 6),
                cos_foreign_buy=(0.9 if k == 0 else 0.5) + 0.001 * (i % 6),
                cos_foreign_sell=(0.9 if k == 0 else 0.5) + 0.001 * (i % 6),
                cos_fund_buy=0.3 + 0.001 * (i % 6), cos_fund_sell=0.3 + 0.001 * (i % 6),
                cos_prop_self_buy=0.2, cos_prop_self_sell=0.2,
                cos_prop_hedge_buy=0.25, cos_prop_hedge_sell=0.25,
                official_missing_share=0.0,
                available_at=datetime.datetime(d.year, d.month, d.day, 21, 45,
                                               tzinfo=datetime.timezone(datetime.timedelta(hours=8))),
            ))
    return pl.DataFrame(rows)


def _render(broker: str = "8440", **kw) -> str:
    h = _hist()
    return broker_profile.render(h, broker=broker, date=h["date"].max(), cohort_codes=COHORT,
                                 window=10, min_periods=5, **kw)


def test_sections_and_epistemic_labels_present() -> None:
    out = _render()
    assert "席位 profile  [外資席位]" in out
    assert "raw(observed)" in out
    assert "性格 / 規模 / 市況 / 特有(讀時代理" in out
    assert "校準卡" in out and "anchored" in out and "unanchored" in out
    assert "身份界線" in out and "非下界" in out
    assert out.splitlines()[-1].startswith("本頁為描述性觀測")


def test_never_uses_identity_probability_language() -> None:
    out = _render()
    for banned in ("外資機率", "posterior 機率", "是外資投資人", "屬於外資"):
        assert banned not in out


def test_residual_percentile_only_with_enough_history() -> None:
    """殘差係數只用 ≤ 當日前的歷史估;樣本不足 → 百分位顯示 —,不硬給。"""
    h = _hist(n_days=3, n_seats=5)
    out = broker_profile.render(h, broker="8440", date=h["date"].max(), cohort_codes=COHORT,
                                window=10, min_periods=2)
    row = next(l for l in out.splitlines() if l.startswith("foreign     buy"))
    assert row.rstrip().endswith("anchored") and "—" in row
    full = _render()
    row = next(l for l in full.splitlines() if l.startswith("foreign     buy"))
    # 8440 的 cos_foreign 0.9 遠高於其他 0.5 → 殘差百分位接近 1
    pct = float(row.split()[4])
    assert pct > 0.9


def test_hidden_foreign_seat_gets_its_own_class_and_footnote() -> None:
    out = _render(broker="9A81")
    assert "[外資客戶*]" in out and "*外資客戶:" in out


def test_unknown_seat_day_says_so() -> None:
    h = _hist()
    out = broker_profile.render(h, broker="ZZZZ", date=h["date"].max(), cohort_codes=COHORT)
    assert "無此席位日" in out


def test_stock_block_appears_only_when_supplied() -> None:
    assert "本子裡最重的股票" not in _render()
    rows = pl.DataFrame({"symbol_id": ["3450"], "stock_gross": [2.5e8], "salience": [0.0146],
                         "sal_mean": [0.0031], "salience_z": [3.1], "stock_gross_z": [2.4],
                         "participation_rate": [1.0], "denominator_effect": [False]})
    out = _render(stock_rows=rows)
    assert "本子裡最重的股票" in out and "3450" in out and "+3.1" in out
