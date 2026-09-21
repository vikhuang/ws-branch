"""v3 Phase 1:measurement geometry(架構文件 §5.4 的八項,前七項在此)。

與 O1 的差別有三:
1. 套 universe gate(普通股),t4 v2 沒套——非股票產品佔 T1 金額 7.6%。
2. cos_market 用**全 universe 定義**:兩側 norm 都在共同 universe 上,分點
   未交易的股票視為零(市場向量的 norm 因此含分點沒碰的股票)。O1 探針用的
   是 support-restricted,兩者差一個 ||m_support||/||m_full|| 因子,會改變
   跨分點排序。
3. 不碰 alpha、不做 actor 校準——只回答哪些是 scale、哪些是 shape。

逐月切塊;分點日層級聚合後才做跨日統計(百萬列等級,不受月切塊限制)。
"""

from __future__ import annotations

import datetime
import sys

import polars as pl
from ws_core import stock_attr, tradedays

from ws_branch.measure import allocation, universe
from ws_branch.tables import io

YEAR = int(sys.argv[1]) if len(sys.argv) > 1 else 2026   # `python v3_phase1_geometry.py 2025`


def _gated_primitives(year: int) -> pl.DataFrame:
    """從 T1 逐月重算分點日 primitives(套 universe gate),含:

    - 規模/廣度:gross_amt、n_symbols(**僅計 gross>0 的股票**,§4.2 定義)
    - 形狀:top1/top5_share、directional_ratio
    - 配置:cos_market_buy/sell(全 universe 定義,measure.allocation)
    - 延續:basket_self_sim(前一**交易日**,用 ws-core 交易日曆而非固定曆日
      緩衝——2026 農曆年連休 12 個曆日)

    逐月切塊;每月多取前一交易日供 basket_self_sim 用。
    """
    uni = universe.stock_universe(
        stock_attr(start=f"{year}-01-01", end=f"{year}-12-31",
                   columns=["coid", "mdate", "stktp_c"]))
    cal_raw = tradedays(start=f"{year - 1}-12-01", end=f"{year}-12-31")
    cal = allocation.prev_trading_day_map(
        cal_raw.filter(pl.col("is_trading_day"))["zdate"].to_list())
    parts, excl = [], []
    for m in range(1, 13):
        month_start = datetime.date(year, m, 1)
        month_end = (datetime.date(year + 1, 1, 1) if m == 12
                     else datetime.date(year, m + 1, 1)) - datetime.timedelta(days=1)
        # 多取前一交易日(由日曆決定,不是固定天數)
        prior = cal.filter(pl.col("date") >= month_start)["prev_date"].min()
        read_from = min(prior, month_start) if prior is not None else month_start
        raw = (io.scan("t1_broker_daily", start=str(read_from), end=str(month_end))
               .select("broker", "symbol_id", "date", "buy_dollar", "sell_dollar")
               .collect())
        if raw.height == 0:
            continue
        sl = universe.apply_universe(raw, uni).with_columns(
            (pl.col("buy_dollar") + pl.col("sell_dollar")).alias("gross"))
        in_month = sl.filter(pl.col("date") >= month_start)
        excl.append(universe.universe_exclusion_report(
            raw.filter(pl.col("date") >= month_start).with_columns(
                (pl.col("buy_dollar") + pl.col("sell_dollar")).alias("g")), uni, "g"))

        # §4.2:n_symbols = 當日 gross>0 的不重複股票數(dash-only 列不算)
        traded = in_month.filter(pl.col("gross") > 0)
        base = (traded.group_by("broker", "date").agg(
            pl.col("buy_dollar").sum().alias("gross_buy_amt"),
            pl.col("sell_dollar").sum().alias("gross_sell_amt"),
            pl.col("gross").sum().alias("gross_amt"),
            pl.col("symbol_id").n_unique().alias("n_symbols"),
            pl.col("gross").max().alias("_t1"),
            pl.col("gross").sort(descending=True).head(5).sum().alias("_t5"))
            .with_columns(
                ((pl.col("gross_buy_amt") - pl.col("gross_sell_amt")).abs()
                 / pl.col("gross_amt")).fill_nan(None).alias("directional_ratio"),
                (pl.col("_t1") / pl.col("gross_amt")).fill_nan(None).alias("top1_share"),
                (pl.col("_t5") / pl.col("gross_amt")).fill_nan(None).alias("top5_share"))
            .drop("_t1", "_t5"))

        mkt = (traded.group_by("symbol_id", "date")
               .agg(pl.col("gross").sum().alias("m")))
        part = base
        for side, col in (("buy", "buy_dollar"), ("sell", "sell_dollar")):
            part = part.join(
                allocation.amount_cosine(in_month, mkt, amount_col=col,
                                         ref_col="m", out=f"cos_market_{side}"),
                on=["broker", "date"], how="left")
        part = part.join(
            allocation.basket_self_similarity(sl, cal).filter(
                pl.col("date") >= month_start),
            on=["broker", "date"], how="left")
        parts.append(part)
        print(f"  {year}-{m:02d}: {part.height:,} 分點日", flush=True)
    rep = pl.concat(excl).select(pl.col("excluded_rows").sum(),
                                 pl.col("excluded_amount").sum())
    print(f"universe gate 擋掉:{rep[0,'excluded_rows']:,} 列 / "
          f"{rep[0,'excluded_amount']/1e8:,.0f} 億")
    return pl.concat(parts)


def _corr(a: pl.Series, b: pl.Series) -> float:
    return pl.DataFrame({"a": a, "b": b}).select(pl.corr("a", "b")).item()


def _cond_table(df: pl.DataFrame, y: str, by: str, bins: list[float]) -> None:
    labels = [f"[{lo:g},{hi:g})" for lo, hi in zip(bins[:-1], bins[1:])]
    out = []
    for (lo, hi), lab in zip(zip(bins[:-1], bins[1:]), labels):
        b = df.filter((pl.col(by) >= lo) & (pl.col(by) < hi) & pl.col(y).is_not_null())
        if b.height < 50:
            continue
        out.append({by: lab, "n": b.height, "p25": round(b[y].quantile(.25), 3),
                    "p50": round(b[y].median(), 3), "p75": round(b[y].quantile(.75), 3)})
    print(f"  {y} | {by}:"); print(pl.DataFrame(out))


def main() -> None:
    df = _gated_primitives(YEAR)
    df = df.with_columns(pl.col("gross_amt").log10().alias("log_gross"),
                         pl.col("n_symbols").log10().alias("log_n"))
    print(f"\n分點日 {df.height:,};分點 {df['broker'].n_unique()};"
          f"交易日 {df['date'].n_unique()}")

    print("\n" + "=" * 70 + "\n[1] primitive 分布\n" + "=" * 70)
    for c in ["gross_amt", "n_symbols", "top1_share", "top5_share",
              "directional_ratio", "cos_market_buy", "cos_market_sell",
              "basket_self_sim"]:
        s = df[c].drop_nulls()
        qs = [round(s.quantile(q), 4) for q in (.05, .25, .5, .75, .95)]
        print(f"  {c:<18} n={s.len():>7,} p05/p25/p50/p75/p95 = {qs}")

    print("\n" + "=" * 70 + "\n[2] gross × breadth\n" + "=" * 70)
    print(f"  corr(log gross, log n_symbols) = {_corr(df['log_gross'], df['log_n']):.3f}")
    sl = df.filter(pl.col("n_symbols") >= 10)
    slope = (((sl["log_n"] - sl["log_n"].mean()) * (sl["log_gross"] - sl["log_gross"].mean())).sum()
             / ((sl["log_gross"] - sl["log_gross"].mean()) ** 2).sum())
    print(f"  log-log 斜率(OLS,n_symbols>=10)= {slope:.3f}"
          f"  → gross 每漲 10 倍,碰股數 ×{10 ** slope:.1f}")

    print("\n" + "=" * 70 + "\n[3-5] 條件分布(對規模/廣度)\n" + "=" * 70)
    gbins = [6, 7, 8, 9, 10, 11, 12]
    nbins = [1, 10, 100, 300, 600, 900, 1200, 3000]
    for y in ["top5_share", "directional_ratio", "cos_market_buy"]:
        _cond_table(df, y, "log_gross", gbins)
        _cond_table(df, y, "n_symbols", nbins)

    print("\n" + "=" * 70 + "\n[6] primitives 相關矩陣(Spearman)\n" + "=" * 70)
    cols = ["log_gross", "log_n", "top1_share", "top5_share", "directional_ratio",
            "cos_market_buy", "cos_market_sell"]
    d = df.select(cols).drop_nulls()
    print("        " + "".join(f"{c[:9]:>10}" for c in cols))
    for a in cols:
        row = "".join(f"{_corr(d[a].rank(), d[b].rank()):>10.2f}" for b in cols)
        print(f"  {a[:7]:<7}" + row)

    print("\n" + "=" * 70 + "\n[7] between / within 分點(≥60 活躍日)\n" + "=" * 70)
    act = df.with_columns(pl.len().over("broker").alias("_d")).filter(pl.col("_d") >= 60)
    for c in ["log_gross", "log_n", "top5_share", "directional_ratio",
              "cos_market_buy"]:
        s = act.filter(pl.col(c).is_not_null())
        w = s.group_by("broker").agg(pl.col(c).var().alias("v"), pl.len().alias("n"))
        within = (w["v"] * w["n"]).sum() / w["n"].sum()
        print(f"  {c:<18} between {1 - within / s[c].var():.0%}  within {within / s[c].var():.0%}")

    out = f"/tmp/v3_phase1_primitives_{YEAR}.parquet"
    df.write_parquet(out)
    print(f"\nprimitives → {out}")


if __name__ == "__main__":
    main()
