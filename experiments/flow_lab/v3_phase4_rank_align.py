"""v3 Phase 4 §3.4:market_rank_alignment 的增量(§8 點名)——可重現版。

2026-09-21 複查抓到:首版 §3.4 的 0.838 / 0.832 是 REPL 一次性算的,repo 內
無程式;且只在 7-9 月(= 測試期)上比較,等於**在 holdout 上挑欄位**,違反
§8「避免在 holdout 上挑欄位」。本版:

1. rank_align 全年逐月重算(席位買/賣金額 rank vs 市場成交額 rank 的 Spearman,
   在席位 support 上;O1 舊定義的市場版,`actor._spearman_by_broker_day`)。
2. **收不收的決定在訓練期做**:兩個線性分數模型(is_foreign ~ 特徵)在訓練期
   擬合、在訓練期比 AUC。測試期只事後報一次作確認,不參與決定。
"""

from __future__ import annotations

import datetime
import os

import numpy as np
import polars as pl
from ws_core import stock_attr

from ws_branch.measure import calibration as cal, universe
from ws_branch.measure.actor import _spearman_by_broker_day
from ws_branch.tables import io
from v3_phase4_calibration import CACHE, _labelled

YEAR = 2026
RANK_CACHE = "/tmp/v3_phase4_rank_align.parquet"


def build_rank_align(year: int) -> pl.DataFrame:
    if os.path.exists(RANK_CACHE):
        return pl.read_parquet(RANK_CACHE)
    uni = universe.stock_universe(stock_attr(
        start=f"{year}-01-01", end=f"{year}-12-31", columns=["coid", "mdate", "stktp_c"]))
    parts = []
    for m in range(1, 13):
        start = datetime.date(year, m, 1)
        end = (datetime.date(year + 1, 1, 1) if m == 12
               else datetime.date(year, m + 1, 1)) - datetime.timedelta(days=1)
        raw = (io.scan("t1_broker_daily", start=str(start), end=str(end))
               .select("broker", "symbol_id", "date", "buy_dollar", "sell_dollar").collect())
        if raw.height == 0:
            continue
        sl = universe.apply_universe(raw, uni)
        mkt = (sl.group_by("symbol_id", "date")
               .agg((pl.col("buy_dollar") + pl.col("sell_dollar")).sum().alias("m")))
        j = sl.join(mkt, on=["symbol_id", "date"], how="inner")
        out = (_spearman_by_broker_day(j.filter(pl.col("buy_dollar") > 0),
                                       "buy_dollar", "m", "rank_align_buy")
               .join(_spearman_by_broker_day(j.filter(pl.col("sell_dollar") > 0),
                                             "sell_dollar", "m", "rank_align_sell"),
                     on=["broker", "date"], how="full", coalesce=True))
        parts.append(out)
        print(f"  rank_align {year}-{m:02d}: {out.height:,}", flush=True)
    df = pl.concat(parts)
    df.write_parquet(RANK_CACHE)
    return df


def linear_score_auc(train: pl.DataFrame, test: pl.DataFrame, feats: list[str],
                     label: str = "is_foreign") -> tuple[float, float]:
    """is_foreign ~ 1 + feats 的最小平方分數;回傳 (訓練期 AUC, 測試期 AUC)。"""
    def design(d: pl.DataFrame) -> np.ndarray:
        return np.column_stack([np.ones(d.height), *[d[f].to_numpy() for f in feats]])
    beta, *_ = np.linalg.lstsq(design(train), train[label].cast(pl.Float64).to_numpy(),
                               rcond=None)
    out = []
    for d in (train, test):
        s = d.with_columns(pl.Series("_s", design(d) @ beta))
        out.append(cal.auc(s.filter(pl.col(label))["_s"], s.filter(~pl.col(label))["_s"]))
    return out[0], out[1]


def main() -> None:
    base = _labelled(pl.read_parquet(CACHE)).join(build_rank_align(YEAR),
                                                  on=["broker", "date"], how="left")
    print("=" * 78 + "\n[§8 E4] rank_align 增量——決定在訓練期,測試期只確認\n" + "=" * 78)
    for side in ("buy", "sell"):
        cos, rank = f"cos_foreign_{side}", f"rank_align_{side}"
        controls = [f"cos_market_{side}", "log_gross", "log_n"]
        sub = base.filter(pl.all_horizontal(
            [pl.col(c).is_not_null() for c in [cos, rank, *controls]]))
        tr, te = cal.time_split(sub, train_frac=2 / 3)
        # 先把兩個候選都對三個控制項殘差化(係數只在訓練期估),再比「殘差之上
        # 還有沒有增量」。直接用原始特徵做監督分數會學到規模——11 家外資席位
        # 全在規模上半,is_foreign ~ log_gross + log_n 的線性分數 AUC 0.995,
        # 那是「外資席位 = 大席位」,不是配置形狀(本腳本首版踩到,留作警語)。
        m_cos = cal.fit_residual(tr, target=cos, controls=controls)
        m_rank = cal.fit_residual(tr, target=rank, controls=controls)
        tr_r = cal.apply_residual(cal.apply_residual(tr, m_cos, out="r_cos"), m_rank, out="r_rank")
        te_r = cal.apply_residual(cal.apply_residual(te, m_cos, out="r_cos"), m_rank, out="r_rank")
        one = lambda d, c: cal.auc(d.filter(pl.col("is_foreign"))[c],  # noqa: E731
                                   d.filter(~pl.col("is_foreign"))[c])
        a_tr, a_te = linear_score_auc(tr_r, te_r, ["r_cos"])
        b_tr, b_te = linear_score_auc(tr_r, te_r, ["r_cos", "r_rank"])
        rho = sub.select(pl.corr(rank, "log_n")).item()
        print(f"  {side}: n={sub.height:,}(train {tr.height:,} / test {te.height:,});"
              f" corr(rank_align, log_n)={rho:.2f}")
        print(f"    單獨殘差 AUC  cos_foreign {one(tr_r, 'r_cos'):.3f} / rank_align "
              f"{one(tr_r, 'r_rank'):.3f}(訓練期)")
        print(f"    訓練期 AUC  cos 殘差 {a_tr:.3f} → +rank 殘差 {b_tr:.3f}"
              f"(增量 {b_tr - a_tr:+.3f})  ← 決定依據")
        print(f"    測試期 AUC  {a_te:.3f} → {b_te:.3f}(增量 {b_te - a_te:+.3f})  ← 事後確認")
        naive_tr, naive_te = linear_score_auc(tr, te, ["log_gross", "log_n"])
        print(f"    [警語] is_foreign ~ log_gross + log_n 監督分數 AUC 訓練 {naive_tr:.3f} /"
              f" 測試 {naive_te:.3f} = 規模,不可當增量比較的底")


if __name__ == "__main__":
    main()
