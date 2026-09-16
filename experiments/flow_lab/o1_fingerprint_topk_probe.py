"""Q3b 探針:top-K 大單版 / 金額份額 cosine 版指紋(2026-09 單月)。
預期見 findings/o1_fingerprint_read.md Q3b;純讀表 + 印出。"""

from __future__ import annotations

import polars as pl

from o1_fingerprint_read import _auc, _cohens_d, _corr, _labelled_t4, _residualize
from ws_branch.measure.actor import _spearman_by_broker_day
from ws_branch.tables import io

START, END = "2026-09-01", "2026-09-30"
KEY = ["broker", "date"]


def _cosine_shares(df: pl.DataFrame, a: str, b: str, out: str) -> pl.DataFrame:
    """支撐集合內 (broker,date) 的金額份額 cosine:Σ a_i b_i / (|a||b|),份額正規化
    在 cosine 裡自動抵銷,直接用金額即可。"""
    return (df.group_by(KEY)
            .agg(((pl.col(a) * pl.col(b)).sum()
                  / ((pl.col(a) ** 2).sum().sqrt() * (pl.col(b) ** 2).sum().sqrt()))
                 .alias(out), pl.len().alias("_n"))
            .with_columns(pl.when(pl.col("_n") >= 5).then(pl.col(out)).otherwise(None)
                          .alias(out)).select(*KEY, out))


def _gap(df: pl.DataFrame, col: str, label: str) -> None:
    sub = df.filter(pl.col(col).is_not_null())
    pos = sub.filter(pl.col("cohort") == "institutional")[col]
    neg = sub.filter(pl.col("cohort") != "institutional")[col]
    print(f"  {label:<34} n={sub.height:>6,} inst={pos.mean():+.3f} 其他={neg.mean():+.3f} "
          f"d={_cohens_d(pos, neg):+.2f} AUC={_auc(pos, neg):.3f}")


def main() -> None:
    t4 = _labelled_t4().filter((pl.col("date") >= pl.lit(START).str.to_date())
                               & (pl.col("date") <= pl.lit(END).str.to_date()))
    labels = t4.select(*KEY, "cohort")
    t3 = io.scan("t3_official_daily", start=START, end=END).select(
        "symbol_id", "date", "foreign_buy_sh", "fund_buy_sh", "foreign_buy_amt", "fund_buy_amt").collect()
    sl = (io.scan("t1_broker_daily", start=START, end=END)
          .select("broker", "symbol_id", "date", "buy_dollar", "sell_dollar").collect())
    mkt = sl.group_by("symbol_id", "date").agg(
        (pl.col("buy_dollar") + pl.col("sell_dollar")).sum().alias("mkt_gross"))
    buy = (sl.filter(pl.col("buy_dollar") > 0).join(t3, on=["symbol_id", "date"])
           .join(mkt, on=["symbol_id", "date"])
           .with_columns(pl.col("buy_dollar").rank(descending=True).over(KEY).alias("_rk")))
    print(f"2026-09:分點日 {labels.height:,};buy 列 {buy.height:,}")

    variants: list[tuple[str, pl.DataFrame]] = []
    for k in (10, 20, 50):
        top = buy.filter(pl.col("_rk") <= k)
        f = _spearman_by_broker_day(top, "buy_dollar", "foreign_buy_sh", f"foreign_top{k}")
        u = _spearman_by_broker_day(top, "buy_dollar", "fund_buy_sh", f"fund_top{k}")
        p = _spearman_by_broker_day(top, "buy_dollar", "mkt_gross", f"pop_top{k}")
        variants.append((f"top{k}", f.join(u, on=KEY).join(p, on=KEY)))
    cf = _cosine_shares(buy, "buy_dollar", "foreign_buy_amt", "foreign_cos")
    cu = _cosine_shares(buy, "buy_dollar", "fund_buy_amt", "fund_cos")
    cp = _cosine_shares(buy, "buy_dollar", "mkt_gross", "pop_cos")
    variants.append(("cos", cf.join(cu, on=KEY).join(cp, on=KEY)))
    # 對照:現行全支撐 rank corr(t4 內建)+ 同月熱門度
    p_full = _spearman_by_broker_day(buy, "buy_dollar", "mkt_gross", "pop_full")
    base = labels.join(t4.select(*KEY, "foreign_sim_buy", "fund_sim_buy"), on=KEY).join(p_full, on=KEY, how="left")
    variants.insert(0, ("full(現行)", base.select(*KEY, pl.col("foreign_sim_buy").alias("foreign_full"),
                                                  pl.col("fund_sim_buy").alias("fund_full"), "pop_full")))

    for name, v in variants:
        df = labels.join(v, on=KEY, how="left")
        tag = name.replace("(現行)", "")
        fcol, ucol, pcol = f"foreign_{tag}", f"fund_{tag}", f"pop_{tag}"
        print(f"\n=== 變體 {name} ===")
        sub = df.filter(pl.col(fcol).is_not_null() & pl.col(pcol).is_not_null())
        print(f"  corr(foreign, popularity)={_corr(sub[fcol], sub[pcol]):.3f}  "
              f"corr(fund, popularity)={_corr(sub[ucol], sub[pcol]):.3f}  "
              f"非 null 比例 {sub.height / df.height:.1%}")
        for y in (fcol, ucol):
            df, b = _residualize(df, y, pcol)
            _gap(df, y, f"{y} 原版")
            _gap(df, f"{y}_resid", f"{y} 殘差版(b={b:.2f})")
        _gap(df, pcol, f"{pcol} 熱門度本身")


if __name__ == "__main__":
    main()
