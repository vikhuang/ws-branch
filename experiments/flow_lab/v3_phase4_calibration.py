"""v3 Phase 4:官方配置關聯與 cohort 驗證(架構文件 §8)。

預期見 findings/v3_phase4_calibration.md(已凍結)。

與 O1 探針的四處不同(每處都是 §8 明令或 Phase 1-3 的實證逼出來的):
1. **全 universe cosine**(O1 是 support-restricted,對小籃子席位膨脹)
2. **全年**(O1 只有 9 月一個月)
3. **控制同側 market cosine + gross + breadth**(O1 只控 market cosine)
4. **時間 holdout + leave-one-family-out + 席位層 bootstrap**(O1 全無)

逐月切塊建 cosine;T3 與 universe 皆為小表。
"""

from __future__ import annotations

import datetime

import polars as pl
from ws_core import stock_attr

from ws_branch.measure import allocation, calibration as cal, guards, universe
from ws_branch.tables import io

YEAR = 2026
CACHE = "/tmp/v3_phase4_cosines.parquet"   # 舊研究快取(缺 T3 當 0);robustness/rank_align 仍讀它,主線改讀 t4_broker_measure
BUCKETS = {  # 官方桶 → (買金額欄, 賣金額欄, 有無席位 anchor)
    "foreign": ("foreign_buy_amt", "foreign_sell_amt", True),
    "fund": ("fund_buy_amt", "fund_sell_amt", False),
    "prop_self": ("prop_self_buy_amt", "prop_self_sell_amt", False),
    "prop_hedge": ("prop_hedge_buy_amt", "prop_hedge_sell_amt", False),
}
WARRANT_ISSUER_HQ = {   # 代號自資料查得,不得憑印象(2026-09-18 事故)
    "9200": "凱基", "9100": "群益金鼎", "9800": "元大", "5850": "統一"}


def build_cosines(year: int) -> pl.DataFrame:
    """席位×日 × {四官方桶 + 市場} 的全 universe 金額 cosine(買賣分開)。"""
    uni = universe.stock_universe(stock_attr(
        start=f"{year}-01-01", end=f"{year}-12-31",
        columns=["coid", "mdate", "stktp_c"]))
    t3 = io.scan("t3_official_daily", start=f"{year}-01-01",
                 end=f"{year}-12-31").collect()
    t3 = universe.apply_universe(t3, uni)   # 參考向量必須同 gate(C 類危害)
    parts = []
    for m in range(1, 13):
        start = datetime.date(year, m, 1)
        end = (datetime.date(year + 1, 1, 1) if m == 12
               else datetime.date(year, m + 1, 1)) - datetime.timedelta(days=1)
        raw = (io.scan("t1_broker_daily", start=str(start), end=str(end))
               .select("broker", "symbol_id", "date", "buy_dollar", "sell_dollar")
               .collect())
        if raw.height == 0:
            continue
        sl = universe.apply_universe(raw, uni)
        guards.require_unique_key(sl, ["broker", "symbol_id", "date"],
                                  who=f"phase4 T1 {year}-{m:02d}")
        mkt = (sl.with_columns((pl.col("buy_dollar") + pl.col("sell_dollar"))
                               .alias("m"))
               .group_by("symbol_id", "date").agg(pl.col("m").sum()))
        t3m = t3.filter((pl.col("date") >= start) & (pl.col("date") <= end))
        base = sl.group_by("broker", "date").agg(
            (pl.col("buy_dollar") + pl.col("sell_dollar")).sum().alias("gross_amt"),
            pl.col("symbol_id").n_unique().alias("n_symbols"))
        out = base
        for side, col in (("buy", "buy_dollar"), ("sell", "sell_dollar")):
            out = out.join(allocation.amount_cosine(
                sl, mkt, amount_col=col, ref_col="m",
                out=f"cos_market_{side}"), on=["broker", "date"], how="left")
            for name, (bcol, scol, _) in BUCKETS.items():
                ref = t3m.select("symbol_id", "date",
                                 pl.col(bcol if side == "buy" else scol).alias("r"))
                out = out.join(allocation.amount_cosine(
                    sl, ref, amount_col=col, ref_col="r",
                    out=f"cos_{name}_{side}"), on=["broker", "date"], how="left")
        parts.append(out)
        print(f"  {year}-{m:02d}: {out.height:,} 席位日", flush=True)
    return pl.concat(parts)


def _labelled(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        universe.cohort_expr().alias("is_foreign"),   # 時變 cohort(1570/1380 在 2025 內退出)
        pl.col("gross_amt").log10().alias("log_gross"),
        pl.col("n_symbols").log10().alias("log_n"))


def evaluate(df: pl.DataFrame, bucket: str, side: str) -> dict:
    """§8 的五項控制 + 時間 holdout,回傳一張證據卡的內容。"""
    target = f"cos_{bucket}_{side}"
    controls = [f"cos_market_{side}", "log_gross", "log_n"]
    sub = df.filter(pl.all_horizontal(
        [pl.col(c).is_not_null() for c in [target, *controls]]))
    train, test = cal.time_split(sub, train_frac=2 / 3)
    model = cal.fit_residual(train, target=target, controls=controls)
    train_r = cal.apply_residual(train, model, out="resid")
    test_r = cal.apply_residual(test, model, out="resid")   # 用訓練期係數
    raw_auc = cal.auc(sub.filter(pl.col("is_foreign"))[target],
                      sub.filter(~pl.col("is_foreign"))[target])
    pt, lo, hi = cal.auc_ci(test_r, score="resid", label="is_foreign",
                            unit="group", n_boot=200)
    _, rlo, rhi = cal.auc_ci(test_r, score="resid", label="is_foreign",
                             unit="row", n_boot=200)
    return {
        "bucket": bucket, "side": side, "n": sub.height,
        "auc_raw": raw_auc,
        "auc_resid_train": cal.auc(train_r.filter(pl.col("is_foreign"))["resid"],
                                   train_r.filter(~pl.col("is_foreign"))["resid"]),
        "auc_resid_test": pt, "ci_lo": lo, "ci_hi": hi,
        "ci_width_group": hi - lo, "ci_width_row": rhi - rlo,
        "beta_market": model.coefs[0], "n_train": model.n_train,
    }


def main() -> None:
    from v3_common import cosines
    df = cosines([YEAR])
    print(f"cosines {YEAR}:{df.height:,} 席位日")
    df = _labelled(df)
    print(f"席位 {df['broker'].n_unique()};外資席位 "
          f"{df.filter(pl.col('is_foreign'))['broker'].n_unique()};"
          f"交易日 {df['date'].n_unique()}")

    print("\n" + "=" * 78 + "\n[E1/E2] 四桶 × 買賣:raw vs 殘差 vs 時間 holdout\n" + "=" * 78)
    cards = [evaluate(df, b, s) for b in BUCKETS for s in ("buy", "sell")]
    c = pl.DataFrame(cards)
    print(c.select("bucket", "side", "n",
                   pl.col("auc_raw").round(3), pl.col("auc_resid_train").round(3),
                   pl.col("auc_resid_test").round(3),
                   pl.col("ci_lo").round(3), pl.col("ci_hi").round(3),
                   pl.col("beta_market").round(2)))
    print("\n[E5] 不確定性:席位層 vs 逐列 bootstrap 的區間寬度")
    print(c.select("bucket", "side", pl.col("ci_width_group").round(3),
                   pl.col("ci_width_row").round(3),
                   (pl.col("ci_width_group") / pl.col("ci_width_row")).round(1)
                   .alias("倍數")))

    print("\n" + "=" * 78 + "\n[E2] leave-one-family-out(cos_foreign,買側)\n" + "=" * 78)
    target, side = "cos_foreign_buy", "buy"
    controls = [f"cos_market_{side}", "log_gross", "log_n"]
    sub = df.filter(pl.all_horizontal(
        [pl.col(c).is_not_null() for c in [target, *controls]]))
    tr, te = cal.time_split(sub, train_frac=2 / 3)
    m = cal.fit_residual(tr, target=target, controls=controls)
    te_r = cal.apply_residual(te, m, out="resid")
    loo = cal.leave_one_group_out(te_r, score="resid", label="is_foreign",
                                  group="broker")
    names = (io.scan("t1_broker_daily", start=f"{YEAR}-01-01", end=f"{YEAR}-12-31")
             .select("broker", "broker_name").unique().collect())
    print(loo.join(names, left_on="left_out", right_on="broker", how="left")
          .select("broker_name", pl.col("auc").round(3), "n_pos")
          .sort("auc"))
    print("\n[E2b] leave-one-seat-out **refit**(係數排除該席位再估;該席位 vs 全部負樣本)")
    loo2 = cal.leave_one_group_out_refit(tr, te, target=target, controls=controls,
                                         label="is_foreign", group="broker")
    print(loo2.join(names, left_on="held_out", right_on="broker", how="left")
          .select("broker_name", pl.col("auc").round(3), "n_pos",
                  pl.col("max_coef_shift").round(5)).sort("auc"))

    print("\n" + "=" * 78 + "\n[E3] 自營兩桶:無席位 anchor,只報間接證據\n" + "=" * 78)
    for side in ("buy", "sell"):
        col = f"cos_prop_hedge_{side}"
        d = df.filter(pl.col(col).is_not_null()).with_columns(
            pl.col("broker").is_in(list(WARRANT_ISSUER_HQ)).alias("issuer"))
        iss = d.filter(pl.col("issuer"))[col]
        oth = d.filter(~pl.col("issuer") & ~pl.col("is_foreign"))[col]
        print(f"  {col}:權證發行商總公司 mean={iss.mean():.3f}(n={iss.len():,})"
              f" vs 其他本土 mean={oth.mean():.3f};差 {iss.mean() - oth.mean():+.3f}"
              f";AUC={cal.auc(iss, oth):.3f}")


if __name__ == "__main__":
    main()
