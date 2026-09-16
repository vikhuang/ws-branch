"""O1 指紋解讀:四個描述性問題(預期見 findings/o1_fingerprint_read.md,
先凍結後跑)。純讀表 + 印出,不寫回任何表;popularity_sim 為本腳本的
臨時量,未進 t4 schema。

Q1 分數形狀與高分者 / Q2 穩定性 / Q3 熱門度基線與殘差版 / Q4 多重性分群。
"""

from __future__ import annotations

import datetime
import math

import polars as pl

from ws_branch.measure.actor import _spearman_by_broker_day
from ws_branch.measure.identity import classify_broker_cohort, load_clusters
from ws_branch.tables import io

YEAR = 2026
KEY = ["broker", "date"]


def _cohens_d(a: pl.Series, b: pl.Series) -> float:
    n1, n2 = a.len(), b.len()
    pooled = math.sqrt(((n1 - 1) * a.var() + (n2 - 1) * b.var()) / (n1 + n2 - 2))
    return (a.mean() - b.mean()) / pooled


def _corr(a: pl.Series, b: pl.Series) -> float:
    return pl.DataFrame({"a": a, "b": b}).select(pl.corr("a", "b")).item()


def _auc(pos: pl.Series, neg: pl.Series) -> float:
    """Mann-Whitney AUC:隨機抽一正一負,正 > 負 的機率(= 分佈可分性)。"""
    both = pl.concat([
        pl.DataFrame({"v": pos, "is_pos": [True] * pos.len()}),
        pl.DataFrame({"v": neg, "is_pos": [False] * neg.len()}),
    ]).with_columns(pl.col("v").rank(method="average").alias("r"))
    rank_sum = both.filter(pl.col("is_pos"))["r"].sum()
    return (rank_sum - pos.len() * (pos.len() + 1) / 2) / (pos.len() * neg.len())


def _labelled_t4() -> pl.DataFrame:
    t4 = io.scan("t4_broker_features", start=f"{YEAR}-01-01", end=f"{YEAR}-12-31").collect()
    names = t4.select("broker_name").unique().with_columns(
        pl.col("broker_name").map_elements(classify_broker_cohort, return_dtype=pl.String)
        .alias("cohort"))
    clusters = load_clusters()
    return (t4.join(names, on="broker_name", how="left")
            .join(clusters, on="broker_name", how="left"))


def _popularity_sim() -> tuple[pl.DataFrame, pl.DataFrame]:
    """(分點日 popularity_sim_buy/sell, 官方向量 vs 成交金額的逐日 Spearman)。

    月切塊:T1 全年 1 億列,逐月 collect(同 t4 build 的紀律)。
    """
    t3 = io.scan("t3_official_daily", start=f"{YEAR}-01-01", end=f"{YEAR}-12-31").collect()
    broker_parts, official_parts = [], []
    for m in range(1, 13):
        start = datetime.date(YEAR, m, 1)
        end = (datetime.date(YEAR + 1, 1, 1) if m == 12
               else datetime.date(YEAR, m + 1, 1)) - datetime.timedelta(days=1)
        sl = (io.scan("t1_broker_daily", start=str(start), end=str(end))
              .select("broker", "symbol_id", "date", "buy_dollar", "sell_dollar")
              .collect())
        if sl.height == 0:
            continue
        mkt = (sl.group_by("symbol_id", "date")
               .agg((pl.col("buy_dollar") + pl.col("sell_dollar")).sum().alias("mkt_gross")))
        buy = sl.filter(pl.col("buy_dollar") > 0).join(mkt, on=["symbol_id", "date"])
        sell = sl.filter(pl.col("sell_dollar") > 0).join(mkt, on=["symbol_id", "date"])
        broker_parts.append(
            _spearman_by_broker_day(buy, "buy_dollar", "mkt_gross", "popularity_sim_buy")
            .join(_spearman_by_broker_day(sell, "sell_dollar", "mkt_gross",
                                          "popularity_sim_sell"),
                  on=KEY, how="full", coalesce=True))
        # 官方向量本身有多「大眾臉」:逐日跨股票的 Spearman
        off = (t3.filter((pl.col("date") >= start) & (pl.col("date") <= end))
               .join(mkt, on=["symbol_id", "date"]))
        official_parts.append(
            off.with_columns([pl.col(c).rank().over("date").alias(f"_r_{c}") for c in
                              ["mkt_gross", "foreign_buy_sh", "fund_buy_sh",
                               "foreign_sell_sh", "fund_sell_sh"]])
            .group_by("date").agg(
                pl.corr("_r_foreign_buy_sh", "_r_mkt_gross").alias("foreign_buy_vs_mkt"),
                pl.corr("_r_fund_buy_sh", "_r_mkt_gross").alias("fund_buy_vs_mkt"),
                pl.corr("_r_foreign_sell_sh", "_r_mkt_gross").alias("foreign_sell_vs_mkt"),
                pl.corr("_r_fund_sell_sh", "_r_mkt_gross").alias("fund_sell_vs_mkt")))
        print(f"  popularity {YEAR}-{m:02d} done", flush=True)
    return pl.concat(broker_parts), pl.concat(official_parts)


def _residualize(df: pl.DataFrame, y: str, x: str) -> pl.DataFrame:
    """OLS 殘差 y − (a + b·x),全母體一條線(描述用,不分群擬合)。"""
    sub = df.filter(pl.col(y).is_not_null() & pl.col(x).is_not_null())
    b = (((sub[y] - sub[y].mean()) * (sub[x] - sub[x].mean())).sum()
         / ((sub[x] - sub[x].mean()) ** 2).sum())
    a = sub[y].mean() - b * sub[x].mean()
    return df.with_columns((pl.col(y) - (a + b * pl.col(x))).alias(f"{y}_resid")), b


def _gap_report(df: pl.DataFrame, col: str, label: str) -> None:
    sub = df.filter(pl.col(col).is_not_null())
    pos = sub.filter(pl.col("cohort") == "institutional")[col]
    neg = sub.filter(pl.col("cohort") != "institutional")[col]
    print(f"  {label:<28} inst mean={pos.mean():+.3f} 其他 mean={neg.mean():+.3f} "
          f"gap={pos.mean() - neg.mean():+.3f}  d={_cohens_d(pos, neg):.2f}  "
          f"AUC={_auc(pos, neg):.3f}")


def q1_shape(t4: pl.DataFrame) -> None:
    print("\n" + "=" * 70 + "\nQ1 分數形狀與高分者\n" + "=" * 70)
    col = "foreign_sim_buy"
    for cohort in ["retail", "mixed", "institutional"]:
        s = t4.filter((pl.col("cohort") == cohort) & pl.col(col).is_not_null())[col]
        hist = (pl.DataFrame({"v": s})
                .with_columns(((pl.col("v") + 1) * 5).floor().clip(0, 9).cast(pl.Int8).alias("bin"))
                .group_by("bin").len().sort("bin"))
        bars = " ".join(f"{int(r['len']) / s.len():.2f}" for r in hist.iter_rows(named=True))
        print(f"  {cohort:<14} n={s.len():>7,} mean={s.mean():.3f} sd={s.std():.3f} "
              f"median={s.median():.3f}\n    直方圖(-1→1 十箱佔比):{bars}")

    non_foreign = t4.filter((pl.col("cohort") != "institutional") & pl.col(col).is_not_null())
    cut = non_foreign[col].quantile(0.9)
    top = non_foreign.filter(pl.col(col) >= cut)
    print(f"\n  非外資名字分點,{col} 前 10%(≥{cut:.3f})的組成 vs 全體:")
    comp = (top.group_by("cohort").len().rename({"len": "top10"})
            .join(non_foreign.group_by("cohort").len().rename({"len": "all"}), on="cohort")
            .with_columns((pl.col("top10") / top.height).alias("top10_share"),
                          (pl.col("all") / non_foreign.height).alias("all_share"))
            .sort("top10", descending=True))
    print(comp)
    print(f"  前 10% 的 n_symbols 中位數 {top['n_symbols'].median():.0f} vs "
          f"全體 {non_foreign['n_symbols'].median():.0f}")

    print("\n  本土名字 60 活躍日均 foreign_sim_buy 榜(≥60 活躍日;描述性,非嫌疑名單):")
    board = (non_foreign.group_by("broker_name", "cohort", "cluster")
             .agg(pl.col(col).mean().alias("mean_sim"), pl.len().alias("days"),
                  pl.col("n_symbols").median().alias("n_sym_med"))
             .filter(pl.col("days") >= 60).sort("mean_sim", descending=True))
    print(board.head(20))
    print("  榜尾 5:")
    print(board.tail(5))


def q2_stability(t4: pl.DataFrame) -> None:
    print("\n" + "=" * 70 + "\nQ2 穩定性\n" + "=" * 70)
    col = "foreign_sim_buy"
    s = t4.filter(pl.col(col).is_not_null()).sort("broker", "date")
    s = s.with_columns(pl.col(col).shift(1).over("broker").alias("_lag"),
                       pl.len().over("broker").alias("_days"))
    active = s.filter(pl.col("_days") >= 60)
    lag1 = _corr(active[col], active["_lag"])
    print(f"  日層級 lag-1 自相關(≥60 活躍日分點,pooled):{lag1:.3f}")
    bins = [(1, 5), (5, 10), (10, 30), (30, 100), (100, 10_000)]
    for lo, hi in bins:
        b = active.filter((pl.col("n_symbols") >= lo) & (pl.col("n_symbols") < hi))
        if b.height > 100:
            print(f"    n_symbols [{lo},{hi}): lag-1={_corr(b[col], b['_lag']):.3f} "
                  f"(n={b.height:,})")
    mid = datetime.date(YEAR, 5, 15)
    halves = (active.with_columns((pl.col("date") < mid).alias("h1"))
              .group_by("broker", "h1").agg(pl.col(col).mean().alias("m"), pl.len().alias("n"))
              .filter(pl.col("n") >= 20)
              .pivot(values="m", index="broker", on="h1").drop_nulls())
    if halves.height:
        rho = _corr(halves["true"].rank(), halves["false"].rank())
        print(f"  分點層級上下半年均值 Spearman(兩半各 ≥20 日,n={halves.height}):{rho:.3f}")


def q3_popularity(t4: pl.DataFrame) -> pl.DataFrame:
    print("\n" + "=" * 70 + "\nQ3 熱門度基線\n" + "=" * 70)
    pop, official = _popularity_sim()
    print("  官方向量 vs 市場成交金額排名(逐日 Spearman 均值):")
    for c in ["foreign_buy_vs_mkt", "fund_buy_vs_mkt", "foreign_sell_vs_mkt", "fund_sell_vs_mkt"]:
        print(f"    {c:<22} {official[c].mean():.3f}  (sd {official[c].std():.3f})")
    t4 = t4.join(pop, on=KEY, how="left")
    print("\n  分點日層級 popularity_sim 分佈:mean="
          f"{t4['popularity_sim_buy'].mean():.3f}  median={t4['popularity_sim_buy'].median():.3f}")
    for y in ["foreign_sim_buy", "fund_sim_buy", "foreign_sim_sell", "fund_sim_sell"]:
        x = "popularity_sim_buy" if y.endswith("buy") else "popularity_sim_sell"
        sub = t4.filter(pl.col(y).is_not_null() & pl.col(x).is_not_null())
        print(f"  corr({y}, {x}) = {_corr(sub[y], sub[x]):.3f}")
    print("\n  institutional vs 其他 —— 原版 / 殘差版(扣掉 popularity 的 OLS 線):")
    for y in ["foreign_sim_buy", "fund_sim_buy", "foreign_sim_sell", "fund_sim_sell"]:
        x = "popularity_sim_buy" if y.endswith("buy") else "popularity_sim_sell"
        t4, slope = _residualize(t4, y, x)
        _gap_report(t4, y, f"{y} 原版")
        _gap_report(t4, f"{y}_resid", f"{y} 殘差版(b={slope:.2f})")
    _gap_report(t4, "popularity_sim_buy", "popularity_sim_buy 本身")
    return t4


def q4_multiplicity(t4: pl.DataFrame) -> None:
    print("\n" + "=" * 70 + "\nQ4 多重性分群\n" + "=" * 70)
    m = t4.filter(pl.col("multiplicity").is_not_null())
    print("  按 cohort:")
    print(m.group_by("cohort").agg(
        pl.len().alias("n"), pl.col("multiplicity").median().alias("median"),
        pl.col("multiplicity").quantile(0.25).alias("q25"),
        pl.col("multiplicity").quantile(0.75).alias("q75"),
        pl.col("n_symbols").median().alias("n_sym_med")).sort("median", descending=True))
    print("  按 taxonomy cluster(舊窗口標籤):")
    print(m.filter(pl.col("cluster").is_not_null()).group_by("cluster").agg(
        pl.len().alias("n"), pl.col("multiplicity").median().alias("median"),
        pl.col("multiplicity").quantile(0.25).alias("q25"),
        pl.col("multiplicity").quantile(0.75).alias("q75")).sort("cluster"))
    retail = m.filter(pl.col("cohort") == "retail")["multiplicity"]
    for other in ["institutional", "mixed"]:
        o = m.filter(pl.col("cohort") == other)["multiplicity"]
        print(f"  AUC(retail > {other}) = {_auc(retail, o):.3f}")
    print("\n  top5_share 機械效應:按 n_symbols 分箱的 multiplicity 中位數(retail vs institutional)")
    bins = [(1, 3), (3, 6), (6, 11), (11, 31), (31, 101), (101, 10_000)]
    for lo, hi in bins:
        b = m.filter((pl.col("n_symbols") >= lo) & (pl.col("n_symbols") < hi))
        r = b.filter(pl.col("cohort") == "retail")["multiplicity"]
        i = b.filter(pl.col("cohort") == "institutional")["multiplicity"]
        print(f"    n_symbols [{lo:>3},{hi:>5}): retail med={r.median() if r.len() else float('nan'):.3f} "
              f"(n={r.len():>6,})  inst med={i.median() if i.len() else float('nan'):.3f} (n={i.len():>4,})")


def main() -> None:
    t4 = _labelled_t4()
    print(f"t4 {YEAR}: {t4.height:,} 分點日;cluster 標籤覆蓋 "
          f"{t4.filter(pl.col('cluster').is_not_null()).height / t4.height:.1%}")
    q1_shape(t4)
    q2_stability(t4)
    t4 = q3_popularity(t4)
    q4_multiplicity(t4)


if __name__ == "__main__":
    main()
