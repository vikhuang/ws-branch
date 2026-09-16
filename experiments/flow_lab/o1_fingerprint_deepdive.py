"""O1 指紋解讀補證:把 findings 裡幾句「推論」拿去對數據(2026-09-16)。

A(t4 全年):分數的 between/within 變異分解、滾動欄冗餘度、3450@9/14 的
「相對自身基線偏離」示範、官方向量的台積電佔比與集中度(HHI)。
B(T1 9 月):金額 cosine 剔除 2330 / 剔除當日成交前五 / 賣向量 的穩健性。
純讀表 + 印出。"""

from __future__ import annotations

import datetime

import polars as pl

from o1_fingerprint_read import _auc, _corr, _labelled_t4, _residualize
from o1_fingerprint_topk_probe import END, KEY, START, _cosine_shares
from ws_branch.tables import io


def part_a(t4: pl.DataFrame) -> None:
    print("=" * 70 + "\nA1 變異分解:分數有多少是「分點是誰」(between)vs「今天發生什麼」(within)\n" + "=" * 70)
    act = t4.with_columns(pl.len().over("broker").alias("_d")).filter(pl.col("_d") >= 60)
    for c in ["foreign_sim_buy", "fund_sim_buy", "multiplicity", "basket_self_sim"]:
        s = act.filter(pl.col(c).is_not_null())
        total = s[c].var()
        within = s.group_by("broker").agg(pl.col(c).var().alias("v"), pl.len().alias("n"))
        within_var = (within["v"] * within["n"]).sum() / within["n"].sum()
        print(f"  {c:<18} between 佔 {1 - within_var / total:.1%}  within 佔 {within_var / total:.1%}"
              f"  (n={s.height:,})")

    print("\nA2 滾動欄冗餘:foreign_sim_buy_60d 與分點全年均值的相關")
    m = act.group_by("broker").agg(pl.col("foreign_sim_buy").mean().alias("_mean"))
    j = act.join(m, on="broker").filter(pl.col("foreign_sim_buy_60d").is_not_null())
    print(f"  corr(60d 滾動, 全年均值)={_corr(j['foreign_sim_buy_60d'], j['_mean']):.3f}  "
          f"corr(單日, 全年均值)={_corr(j['foreign_sim_buy'], j['_mean']):.3f}")

    print("\nA3 3450@2026-09-14:foreign_sim_sell 相對自身基線的偏離(gross≥1 億的分點)")
    d = datetime.date(2026, 9, 14)
    t1 = (io.scan("t1_broker_daily", start=str(d), end=str(d))
          .filter(pl.col("symbol_id") == "3450").collect()
          .with_columns((pl.col("buy_dollar") + pl.col("sell_dollar")).alias("g"),
                        (pl.col("buy_dollar") - pl.col("sell_dollar")).alias("net")))
    base = (t4.filter(pl.col("date") != d).group_by("broker")
            .agg(pl.col("foreign_sim_sell").mean().alias("mu"), pl.col("foreign_sim_sell").std().alias("sd"),
                 pl.col("multiplicity").mean().alias("mu_mult")))
    today = t4.filter(pl.col("date") == d).select("broker", "foreign_sim_sell", "multiplicity", "n_symbols")
    z = (t1.filter(pl.col("g") >= 1e8).join(today, on="broker").join(base, on="broker")
         .with_columns(((pl.col("foreign_sim_sell") - pl.col("mu")) / pl.col("sd")).alias("z_sell"),
                       (pl.col("multiplicity") - pl.col("mu_mult")).alias("d_mult"))
         .select("broker_name", (pl.col("g") / 1e8).round(1).alias("gross億"), (pl.col("net") / 1e8).round(1).alias("net億"),
                 pl.col("foreign_sim_sell").round(2).alias("today"), pl.col("mu").round(2).alias("own_mean"),
                 pl.col("z_sell").round(1), pl.col("d_mult").round(2), "n_symbols")
         .sort("z_sell", descending=True))
    pl.Config.set_tbl_rows(40)
    print(z)

    print("\nA4 官方向量的集中度(全年逐日,取中位數)")
    t3 = io.scan("t3_official_daily", start="2026-01-01", end="2026-12-31").select(
        "symbol_id", "date", "foreign_buy_amt", "fund_buy_amt").collect()
    tot = t3.group_by("date").agg(pl.col("foreign_buy_amt").sum().alias("F"), pl.col("fund_buy_amt").sum().alias("U"))
    sh = t3.join(tot, on="date").with_columns((pl.col("foreign_buy_amt") / pl.col("F")).alias("sf"),
                                              (pl.col("fund_buy_amt") / pl.col("U")).alias("su"))
    tsmc = sh.filter(pl.col("symbol_id") == "2330").select("date", "sf", "su")
    hhi = sh.group_by("date").agg((pl.col("sf") ** 2).sum().alias("hhi_f"), (pl.col("su") ** 2).sum().alias("hhi_u"),
                                  pl.col("sf").sort(descending=True).head(5).sum().alias("top5_f"),
                                  pl.col("su").sort(descending=True).head(5).sum().alias("top5_u"))
    print(f"  台積電佔官方外資買入金額:中位 {tsmc['sf'].median():.1%}(p90 {tsmc['sf'].quantile(0.9):.1%});"
          f"佔投信買入:中位 {tsmc['su'].median():.1%}")
    print(f"  前五檔佔比:外資 {hhi['top5_f'].median():.1%} / 投信 {hhi['top5_u'].median():.1%}")
    print(f"  HHI(有效檔數 1/HHI):外資 {hhi['hhi_f'].median():.4f}({1 / hhi['hhi_f'].median():.0f} 檔) / "
          f"投信 {hhi['hhi_u'].median():.4f}({1 / hhi['hhi_u'].median():.0f} 檔)")


def part_b(t4: pl.DataFrame) -> None:
    print("\n" + "=" * 70 + "\nB 金額 cosine 穩健性(2026-09)\n" + "=" * 70)
    labels = t4.filter((pl.col("date") >= pl.lit(START).str.to_date()) & (pl.col("date") <= pl.lit(END).str.to_date())
                       ).select(*KEY, "broker_name", "cohort")
    t3 = io.scan("t3_official_daily", start=START, end=END).select(
        "symbol_id", "date", "foreign_buy_amt", "foreign_sell_amt").collect()
    sl = io.scan("t1_broker_daily", start=START, end=END).select(
        "broker", "symbol_id", "date", "buy_dollar", "sell_dollar").collect()
    mkt = sl.group_by("symbol_id", "date").agg((pl.col("buy_dollar") + pl.col("sell_dollar")).sum().alias("mkt_gross"))
    top5 = mkt.with_columns(pl.col("mkt_gross").rank(descending=True).over("date").alias("_r")).filter(pl.col("_r") <= 5)
    top5_set = top5.select("symbol_id", "date").with_columns(pl.lit(True).alias("_top5"))
    base = sl.join(t3, on=["symbol_id", "date"]).join(mkt, on=["symbol_id", "date"]).join(
        top5_set, on=["symbol_id", "date"], how="left").with_columns(pl.col("_top5").fill_null(False))

    def run(df: pl.DataFrame, side: str, label: str) -> pl.DataFrame:
        amt, off = ("buy_dollar", "foreign_buy_amt") if side == "buy" else ("sell_dollar", "foreign_sell_amt")
        d = df.filter(pl.col(amt) > 0)
        out = (labels.join(_cosine_shares(d, amt, off, "fc"), on=KEY, how="left")
               .join(_cosine_shares(d, amt, "mkt_gross", "pc"), on=KEY, how="left"))
        out, b = _residualize(out, "fc", "pc")
        s = out.filter(pl.col("fc_resid").is_not_null())
        pos = s.filter(pl.col("cohort") == "institutional")
        neg = s.filter(pl.col("cohort") != "institutional")
        print(f"  {label:<28} corr(fc,pc)={_corr(s['fc'], s['pc']):.3f}  原版 AUC={_auc(pos['fc'], neg['fc']):.3f}  "
              f"殘差 AUC={_auc(pos['fc_resid'], neg['fc_resid']):.3f}  pc 本身 AUC={_auc(pos['pc'], neg['pc']):.3f}  "
              f"(inst n={pos.height})")
        return out

    run(base, "buy", "買:全支撐(對照)")
    ex = run(base.filter(pl.col("symbol_id") != "2330"), "buy", "買:剔除 2330")
    run(base.filter(~pl.col("_top5")), "buy", "買:剔除當日成交前五")
    run(base, "sell", "賣:全支撐")
    run(base.filter(pl.col("symbol_id") != "2330"), "sell", "賣:剔除 2330")
    print("\n  剔除 2330 後外資逐席 cos 殘差(買):")
    print(ex.filter(pl.col("cohort") == "institutional").group_by("broker_name")
          .agg(pl.col("fc_resid").mean().round(3).alias("resid_ex2330"), pl.len().alias("days"))
          .sort("resid_ex2330", descending=True))
    dom = ex.filter((pl.col("cohort") != "institutional") & pl.col("fc_resid").is_not_null())
    print(f"  本土分點 resid_ex2330 p50/p90/p99: "
          f"{[round(dom['fc_resid'].quantile(q), 3) for q in (0.5, 0.9, 0.99)]}")
    print("  本土前 8(剔除 2330,≥8 日):")
    print(dom.group_by("broker_name").agg(pl.col("fc_resid").mean().round(3).alias("resid"), pl.len().alias("days"))
          .filter(pl.col("days") >= 8).sort("resid", descending=True).head(8))


if __name__ == "__main__":
    t4 = _labelled_t4()
    part_a(t4)
    part_b(t4)
