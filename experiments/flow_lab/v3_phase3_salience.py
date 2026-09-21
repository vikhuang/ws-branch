"""v3 Phase 3:stock salience 與局部流量(架構文件 §7)。

預期見 findings/v3_phase3_salience.md(已凍結)。

首版不物化 branch×stock×day(全交叉 ≈2.95 億列,§7 明令)。故:
- 幾何形狀用**隨機抽樣的股票**(種子固定),誠實揭露這是抽樣不是全市場。
- 聯鈞 3450 單股全窗跑完,供讀本驗收。
"""

from __future__ import annotations

import datetime
import random

import polars as pl

from ws_core import stock_attr

from ws_branch.measure import salience, universe
from ws_branch.tables import io

YEAR = 2026
AS_OF = datetime.date(2026, 9, 14)
WINDOW, MIN_PERIODS = 60, 20
SAMPLE_N, SEED = 40, 20260921


def _corr(a: pl.Series, b: pl.Series) -> float:
    return pl.DataFrame({"a": a, "b": b}).select(pl.corr("a", "b")).item()


def _branch_day() -> pl.DataFrame:
    """席位日總額(salience 的分母)——Phase 1 的 gated primitives。"""
    return pl.read_parquet("/tmp/v3_phase1_primitives.parquet").select(
        "broker", "date", "gross_amt")


def _pairs_for(symbols: list[str], bd: pl.DataFrame, start: str,
               end: str, uni: pl.DataFrame | None = None) -> pl.DataFrame:
    """對指定股票清單建 pair panel + 歷史 + 異常(逐股,不做全交叉)。"""
    t1 = (io.scan("t1_broker_daily", start=start, end=end)
          .filter(pl.col("symbol_id").is_in(symbols))
          .select("broker", "symbol_id", "date", "buy_dollar", "sell_dollar")
          .collect())
    daily = salience.daily_salience(t1, bd, universe=uni)
    out = []
    for s in symbols:
        panel = salience.build_pair_panel(daily, bd, symbol_id=s, universe=uni)
        if panel.height == 0:
            continue
        h = salience.pair_history(panel, window=WINDOW, min_periods=MIN_PERIODS)
        out.append(salience.pair_anomaly(h))
    return pl.concat(out) if out else pl.DataFrame()


def main() -> None:
    bd = _branch_day()
    # 抽樣母體必須套 universe gate(2026-09-21 複查抓到:首版從 T1 原始代號抽,
    # 40 檔混入 12 檔 ETF/特別股/興櫃——分子未 gate、分母已 gate,salience 定義
    # 不一致,E1-E4 全污染)
    uni = universe.stock_universe(stock_attr(
        start=str(AS_OF), end=str(AS_OF), columns=["coid", "mdate", "stktp_c"]))
    traded_today = (io.scan("t1_broker_daily", start=str(AS_OF), end=str(AS_OF))
                    .select("symbol_id", "date").unique().collect())
    uni_syms = sorted(universe.apply_universe(traded_today, uni)["symbol_id"].to_list())
    rng = random.Random(SEED)
    sample = sorted(rng.sample(uni_syms, SAMPLE_N))
    start = str(AS_OF - datetime.timedelta(days=150))
    print(f"抽樣 {SAMPLE_N} 檔(seed={SEED}),窗 {WINDOW} 活躍日 / "
          f"min_periods={MIN_PERIODS},as-of {AS_OF}")

    uni_window = universe.stock_universe(stock_attr(
        start=start, end=str(AS_OF), columns=["coid", "mdate", "stktp_c"]))
    df = _pairs_for(sample, bd, start, str(AS_OF), uni_window)
    print(f"pair-day 觀測 {df.height:,};席位 {df['broker'].n_unique()};"
          f"股票 {df['symbol_id'].n_unique()}")

    print("\n" + "=" * 70 + "\n[E1] 幾何形狀\n" + "=" * 70)
    traded = df.filter(pl.col("traded"))
    print(f"  有交易的 pair-day {traded.height:,} / {df.height:,} "
          f"({traded.height / df.height:.1%})")
    print(f"  salience(有交易)p25/p50/p75/p95 = "
          f"{[round(traded['salience'].quantile(q), 5) for q in (.25, .5, .75, .95)]}")
    pr = traded.filter(pl.col("participation_rate").is_not_null())
    print(f"  參與率(有交易 pair 的落後 60 日)p25/p50/p75 = "
          f"{[round(pr['participation_rate'].quantile(q), 3) for q in (.25, .5, .75)]}")

    print("\n  [補零機制在真資料上的驗證] 挑一個稀疏 pair:")
    sparse = (traded.filter(pl.col("participation_rate").is_between(0.05, 0.3))
              .sort("date").head(1))
    if sparse.height:
        r = sparse.row(0, named=True)
        win = df.filter((pl.col("broker") == r["broker"]) & (pl.col("symbol_id") == r["symbol_id"])
                        & (pl.col("date") < r["date"])).sort("date").tail(WINDOW)
        print(f"    {r['broker']}/{r['symbol_id']}:前 {win.height} 活躍日中有交易 "
              f"{win['traded'].sum()} 天、補零 {(~win['traded']).sum()} 天;"
              f"含零均值 {win['salience'].mean()*100:.4f}% = 模組 sal_mean "
              f"{r['sal_mean']*100:.4f}% {'✓' if abs(win['salience'].mean()-r['sal_mean'])<1e-9 else '✗'};"
              f"只算有交易日的均值 {win.filter(pl.col('traded'))['salience'].mean()*100:.4f}%(倖存者偏誤版)")

    print("\n" + "=" * 70 + "\n[E2] 三個量的獨立性\n" + "=" * 70)
    both = traded.filter(pl.col("salience_z").is_not_null()
                         & pl.col("stock_gross_z").is_not_null())
    print(f"  n={both.height:,}")
    print(f"  corr(salience_z, stock_gross_z) = "
          f"{_corr(both['salience_z'], both['stock_gross_z']):.3f}")
    pp = both.filter(pl.col("participation_rate").is_not_null())
    print(f"  corr(participation_rate, salience_z) = "
          f"{_corr(pp['participation_rate'], pp['salience_z']):.3f}")
    print(f"  corr(participation_rate, |salience_z|) = "
          f"{_corr(pp['participation_rate'], pp['salience_z'].abs()):.3f}")

    print("\n" + "=" * 70 + "\n[E3] 歷史不足的規模(§7 明令揭露)\n" + "=" * 70)
    n_null = traded.filter(pl.col("salience_z").is_null()).height
    print(f"  有交易但 salience_z 為 null:{n_null:,} / {traded.height:,} "
          f"({n_null / traded.height:.1%})")
    short = traded.filter(pl.col("history_n") < MIN_PERIODS).height
    zero_sd = traded.filter((pl.col("history_n") >= MIN_PERIODS)
                            & (pl.col("sal_sd") <= 1e-12)).height
    print(f"    其中歷史筆數不足 {short:,};歷史無變異(sd=0){zero_sd:,}")
    zs = traded.filter((pl.col("history_n") >= MIN_PERIODS) & (pl.col("sal_sd") <= 1e-12))
    print(f"    sd=0 者的參與率 = 0(窗內從沒碰過)的比例:"
          f"{(zs['participation_rate'] == 0).mean():.1%}")
    print(f"  history_n p10/p50 = "
          f"{[int(traded['history_n'].quantile(q)) for q in (.1, .5)]}")

    print("\n" + "=" * 70 + "\n[E4] 分母效應(§7 警語的實證)\n" + "=" * 70)
    hi = both.filter(pl.col("salience_z") > 2)
    if hi.height:
        de = hi.filter(pl.col("denominator_effect")).height
        print(f"  salience_z > 2 的 pair-day:{hi.height:,};其中絕對金額其實"
              f"下降(分母效應)的 {de:,}({de / hi.height:.1%})")
        print(f"  這些案例的 stock_gross_z p50 = "
              f"{hi.filter(pl.col('denominator_effect'))['stock_gross_z'].median():.2f}"
              if de else "")

    print("\n" + "=" * 70 + f"\n[E5] 聯鈞 3450 @ {AS_OF}\n" + "=" * 70)
    lj = _pairs_for(["3450"], bd, start, str(AS_OF), uni_window).filter(
        pl.col("date") == AS_OF)
    top = lj.sort("stock_gross", descending=True).head(10)
    names = (io.scan("t1_broker_daily", start=str(AS_OF), end=str(AS_OF))
             .select("broker", "broker_name").unique().collect())
    pl.Config.set_tbl_width_chars(170)
    print(top.join(names, on="broker", how="left").select(
        "broker_name",
        (pl.col("stock_gross") / 1e8).round(2).alias("該股億"),
        (pl.col("branch_gross") / 1e8).round(0).alias("席位當日億"),
        (pl.col("salience") * 100).round(2).alias("salience%"),
        (pl.col("sal_mean") * 100).round(2).alias("平常%"),
        pl.col("salience_z").round(1),
        pl.col("stock_gross_z").round(1),
        pl.col("participation_rate").round(2).alias("參與率"),
        pl.col("two_sidedness").round(2).alias("來回率"),
        pl.col("denominator_effect").alias("分母效應")))


if __name__ == "__main__":
    main()
