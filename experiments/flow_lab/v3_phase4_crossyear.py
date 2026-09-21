"""v3 Phase 4 跨年:訓練 2025 → 測試 2026(真正的跨年 OOS;規格 §8/reframed §2.4)。

首版 Phase 4 只有 2026 年內 2/3 時序切分。2026 的市場量能是 2025 的 3-4 倍
(外資前五席位單日 gross 300-475 億 → 700-1,665 億),兩年是不同市況。本腳本
回答:**在 2025 估的殘差係數,拿到 2026 還分得開 cohort 嗎?**

cohort 是時變的(1570 法興至 2025-07-31、1380 匯立至 2025-10-17;2026 為 11 家),
正樣本按 (broker, date) 逐列解析,不是固定名單。
"""

from __future__ import annotations

import polars as pl

from ws_branch.measure import calibration as cal, universe
from v3_common import cosines
from v3_phase4_calibration import BUCKETS

TRAIN_YEAR, TEST_YEAR = 2025, 2026


def _labelled(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        universe.cohort_expr().alias("is_foreign"),
        pl.col("gross_amt").log10().alias("log_gross"),
        pl.col("n_symbols").log10().alias("log_n"))


def _auc(d: pl.DataFrame, col: str) -> float:
    return cal.auc(d.filter(pl.col("is_foreign"))[col], d.filter(~pl.col("is_foreign"))[col])


def main() -> None:
    tr_all, te_all = _labelled(cosines([TRAIN_YEAR])), _labelled(cosines([TEST_YEAR]))
    for tag, d in ((TRAIN_YEAR, tr_all), (TEST_YEAR, te_all)):
        print(f"{tag}:席位日 {d.height:,};席位 {d['broker'].n_unique()};"
              f"外資席位 {d.filter(pl.col('is_foreign'))['broker'].n_unique()};"
              f"交易日 {d['date'].n_unique()};log_gross 中位 {d['log_gross'].median():.2f}")

    print("\n" + "=" * 78 + "\n[跨年] 四桶 × 買賣:係數在 2025 估,AUC 在 2026 讀\n" + "=" * 78)
    rows = []
    for bucket in BUCKETS:
        for side in ("buy", "sell"):
            target = f"cos_{bucket}_{side}"
            controls = [f"cos_market_{side}", "log_gross", "log_n"]
            ok = pl.all_horizontal([pl.col(c).is_not_null() for c in [target, *controls]])
            tr, te = tr_all.filter(ok), te_all.filter(ok)
            m25 = cal.fit_residual(tr, target=target, controls=controls)
            m26 = cal.fit_residual(te, target=target, controls=controls)   # 只為比係數
            te_r = cal.apply_residual(te, m25, out="resid")
            tr_r = cal.apply_residual(tr, m25, out="resid")
            pt, lo, hi = cal.auc_ci(te_r, score="resid", label="is_foreign", n_boot=200)
            # 對照:2025 年內 2/3 時序切分(與首版 2026 年內切分同構)
            a, b = cal.time_split(tr, train_frac=2 / 3)
            m_in = cal.fit_residual(a, target=target, controls=controls)
            within = _auc(cal.apply_residual(b, m_in, out="r"), "r")
            rows.append({
                "bucket": bucket, "side": side,
                "raw_2026": _auc(te, target), "resid_2026(係數2025)": pt, "ci_lo": lo, "ci_hi": hi,
                "resid_2025(自身)": _auc(tr_r, "resid"), "2025年內切分": within,
                "β_mkt_2025": m25.coefs[0], "β_mkt_2026": m26.coefs[0],
                "β_gross_2025": m25.coefs[1], "β_gross_2026": m26.coefs[1],
            })
    pl.Config.set_tbl_width_chars(200)
    pl.Config.set_tbl_cols(20)
    print(pl.DataFrame(rows).with_columns(pl.col(pl.Float64).round(3)))

    print("\n" + "=" * 78 + "\n[跨年] foreign:2026 逐月(係數 2025)與 leave-one-seat-out\n" + "=" * 78)
    for side in ("buy", "sell"):
        target = f"cos_foreign_{side}"
        controls = [f"cos_market_{side}", "log_gross", "log_n"]
        ok = pl.all_horizontal([pl.col(c).is_not_null() for c in [target, *controls]])
        m25 = cal.fit_residual(tr_all.filter(ok), target=target, controls=controls)
        te_r = cal.apply_residual(te_all.filter(ok), m25, out="resid").with_columns(
            pl.col("date").dt.month().alias("mo"))
        monthly = [f"{mo:02d}:{_auc(te_r.filter(pl.col('mo') == mo), 'resid'):.3f}"
                   for mo in sorted(te_r["mo"].unique().to_list())]
        print(f"  {side} 逐月  " + "  ".join(monthly))
        loo = cal.leave_one_group_out(te_r, score="resid", label="is_foreign", group="broker")
        print(f"  {side} 留一  min {loo['auc'].min():.3f}(拿掉 {loo.sort('auc')['left_out'][0]})"
              f" / max {loo['auc'].max():.3f}(拿掉 {loo.sort('auc', descending=True)['left_out'][0]})")

    print("\n" + "=" * 78 + "\n[反向] 係數在 2026 估、AUC 在 2025 讀(對稱檢查,含 1570/1380)\n" + "=" * 78)
    for side in ("buy", "sell"):
        target = f"cos_foreign_{side}"
        controls = [f"cos_market_{side}", "log_gross", "log_n"]
        ok = pl.all_horizontal([pl.col(c).is_not_null() for c in [target, *controls]])
        m26 = cal.fit_residual(te_all.filter(ok), target=target, controls=controls)
        tr_r = cal.apply_residual(tr_all.filter(ok), m26, out="resid")
        print(f"  {side}: raw_2025 {_auc(tr_all.filter(ok), target):.3f} → resid_2025(係數2026) "
              f"{_auc(tr_r, 'resid'):.3f}")
        for code, name in (("1570", "港商法國興業"), ("1380", "台灣匯立")):
            s = tr_r.filter(pl.col("broker") == code)
            if s.height:
                neg = tr_r.filter(~pl.col("is_foreign"))["resid"]
                print(f"      {name}({code})單席位 vs 本土 AUC {cal.auc(s['resid'], neg):.3f}"
                      f"(n={s.height})")


if __name__ == "__main__":
    main()
