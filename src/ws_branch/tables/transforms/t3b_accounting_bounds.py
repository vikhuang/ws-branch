"""T3b 會計界限表:股票×日×側的硬上下界(不含任何行為模型)。

架構文件 §4.4/§5.1。這張表回答「公開資料的會計約束最多能說到什麼程度」:
官方外資量有多少**必須**在外資席位之外、cohort 內的外資量能被夾到多窄、
未屬於四個官方桶的餘額有多大、以及有多少市場成交量根本不在分點資料裡。

粒度:symbol_id × date × side(buy/sell 長表,不用寬表塞雙側)。
universe:僅普通股(measure.universe;T3 的指數彙總列會讓外資買入虛增 4.7 倍)。
cohort:外資券商席位,以**代號 + 有效期**宣告(measure.universe.FOREIGN_COHORT,
        時變:1520/1570/1380 已退出,按日解析);
        不用 classify_broker_cohort 的 institutional 桶——它含本土法人席位。

三個口徑刻意分開存,不能互相取代:
- `market_total_sh`  = TEJ vol(全市場成交量 V)
- `observed_total_sh`= T1 分點加總(⊆ V,A5)
- `unobserved_sh`    = V − T1(鉅額/特殊交易,不得分配給任何可見席位)
界限用 V 當總量約束;若改用 T1 總量會把未觀測交易默默算成可見席位的配額。

記憶體:T1 逐月切塊後先聚合到股票日(1 億列 → 40 萬列),再與小表 join。
"""

from __future__ import annotations

import datetime

import polars as pl
from ws_core import prices, stock_attr

from ws_branch.measure import accounting, universe
from ws_branch.tables import io, windows

def _t1_stock_day(year: int, month: int) -> pl.DataFrame:
    """T1 逐月 → 股票日 × (全市場, 外資 cohort) 的買賣股數。"""
    start, end = windows.month_bounds(year, month)
    lf = io.scan("t1_broker_daily", start=str(start), end=str(end))
    is_cohort = universe.cohort_expr()   # 時變 cohort:按 (broker, date) 解析
    return (lf.group_by("symbol_id", "date")
            .agg(pl.col("buy_sh").sum().alias("observed_total_buy_sh"),
                 pl.col("sell_sh").sum().alias("observed_total_sell_sh"),
                 pl.col("buy_sh").filter(is_cohort).sum().alias("cohort_buy_sh"),
                 pl.col("sell_sh").filter(is_cohort).sum().alias("cohort_sell_sh"),
                 pl.col("broker").n_unique().alias("n_brokers"))
            .collect())


def build_year(year: int) -> pl.LazyFrame:
    # cohort 代號必須與當期資料的名稱對得上,對不上當場炸掉(2026-09-18
    # 實付教訓:誤填 9200/9100 使本土 HQ 混入 cohort,整年表失真)
    absent = universe.assert_cohort_names(
        io.scan("t1_broker_daily", start=f"{year}-01-01", end=f"{year}-12-31")
        .select("broker", "broker_name").unique().collect(), year=year)
    if absent:
        print(f"  cohort 代號 {absent} 於 {year} 未出現(未交易或名單過期)")
    uni = universe.stock_universe(
        stock_attr(start=f"{year}-01-01", end=f"{year}-12-31",
                   columns=["coid", "mdate", "stktp_c"]))
    t3 = io.scan("t3_official_daily", start=f"{year}-01-01", end=f"{year}-12-31").collect()
    # TEJ vol 單位=張,轉股與 T1/T3 對齊
    px = (prices(start=f"{year}-01-01", end=f"{year}-12-31",
                 columns=["coid", "mdate", "vol"])
          .select(pl.col("coid").alias("symbol_id"),
                  pl.col("mdate").cast(pl.Date).alias("date"),
                  (pl.col("vol") * 1000).cast(pl.Float64).alias("market_total_sh")))

    parts: list[pl.DataFrame] = []
    for month in range(1, 13):
        t1 = _t1_stock_day(year, month)
        if t1.height == 0:
            continue
        # 母體 = universe ∩ 當日有分點成交的股票日。T3 或行情缺列的股票日**必須進表**
        # 並以 input_complete=False 擋下發布——inner join 會讓它們無聲消失,
        # 表內發布率 100% 而母體覆蓋只有 91%(2025-07-01 實測 167 檔;A9 複查抓到)
        wide = (universe.apply_universe(t1, uni)
                .join(t3.with_columns(pl.lit(True).alias("t3_present")),
                      on=["symbol_id", "date"], how="left")
                .join(px.with_columns(pl.lit(True).alias("vol_present")),
                      on=["symbol_id", "date"], how="left")
                .with_columns(pl.col("t3_present").fill_null(False),
                              pl.col("vol_present").fill_null(False)))
        if wide.height == 0:
            continue
        rows = []
        for side in ("buy", "sell"):
            df = (wide
                  .with_columns(pl.lit(side).alias("side"))
                  .rename({f"observed_total_{side}_sh": "observed_total_sh",
                           f"cohort_{side}_sh": "cohort_sh",
                           f"foreign_{side}_sh": "official_foreign_sh",
                           f"fund_{side}_sh": "official_fund_sh",
                           f"prop_self_{side}_sh": "official_prop_self_sh",
                           f"prop_hedge_{side}_sh": "official_prop_hedge_sh"}))
            df = accounting.flow_bounds(
                df, cohort_col="cohort_sh", official_col="official_foreign_sh",
                market_col="market_total_sh", prefix="foreign")
            df = accounting.actor_residual(
                df, market_col="market_total_sh",
                bucket_cols=["official_foreign_sh", "official_fund_sh",
                             "official_prop_self_sh", "official_prop_hedge_sh"],
                out="other_actor_sh")
            df = accounting.unobserved_flow(
                df, market_col="market_total_sh",
                observed_col="observed_total_sh", out="unobserved_sh")
            # 界限只在「V 可信」時可發布:V 來自 TEJ vol,而 2026-05-05 /
            # 07-17 兩天全市場性地 T1 > TEJ vol(見 audit_ledger A6),那種
            # 日子的 V 偏低會讓 L=max(0,S+F−V) 虛高。三個旗標合成一欄,
            # 下游不必自己記得要 AND。
            # 輸入有 null(T3/行情缺列,或 T3 有列但值缺)時,三個旗標都會是 null。
            # **旗標不得為 null**——下游用 `~flag` 篩選會靜默漏掉這些列
            # (2026-09-20 查到 112 列)。缺值一律視為不可發布,並另立
            # `input_complete` 欄區分「輸入缺值」與「輸入互相矛盾」。
            inputs = ["market_total_sh", "observed_total_sh", "cohort_sh",
                      "official_foreign_sh", "official_fund_sh",
                      "official_prop_self_sh", "official_prop_hedge_sh"]
            df = df.with_columns(
                pl.all_horizontal([pl.col(c).is_not_null() for c in inputs])
                .alias("input_complete"))
            df = df.with_columns(
                (pl.col("input_complete")
                 & pl.col("foreign_bounds_ok").fill_null(False)
                 & pl.col("unobserved_sh_ok").fill_null(False)
                 & pl.col("other_actor_sh_ok").fill_null(False))
                .alias("bounds_publishable"))
            rows.append(df.select(
                "symbol_id", "date", "side", "market_total_sh", "observed_total_sh",
                "unobserved_sh", "unobserved_sh_ok", "n_brokers", "cohort_sh",
                "official_foreign_sh", "official_fund_sh", "official_prop_self_sh",
                "official_prop_hedge_sh", "other_actor_sh", "other_actor_sh_ok",
                "foreign_x_lo", "foreign_x_hi", "foreign_x_width",
                "foreign_y_lo", "foreign_y_hi",
                "foreign_cov_lo", "foreign_cov_hi", "foreign_bounds_ok",
                "t3_present", "vol_present", "input_complete", "bounds_publishable"))
        parts.append(pl.concat(rows))
    if not parts:
        raise ValueError(f"t3b_accounting_bounds {year}: 無任何月份有資料")
    # cohort_id / actor_bucket 現為常數,但必須是欄位:§4.4 的長表契約要求
    # 擴充多 cohort／多 actor 桶時「不悄悄增加重複列」——鍵欄先在位,
    # 之後加列才不會破壞既有消費端對唯一鍵的假設。
    return (pl.concat(parts)
            .with_columns(
                pl.lit("foreign_seat").alias("cohort_id"),
                pl.lit("foreign").alias("actor_bucket"),
                pl.lit(universe.UNIVERSE_VERSION).alias("universe_version"),
                pl.lit(universe.FOREIGN_BROKER_COHORT_VERSION).alias("cohort_version"))
            .lazy())
