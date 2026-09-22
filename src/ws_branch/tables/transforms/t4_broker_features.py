"""T4 **v2**(frozen,Step F 2026-09-22):分點×日行為特徵,rank 版指紋 + multiplicity。

已退役的現行產出——可 `build` 重建供歷史重現(runner 印警語),不隨年份更新、
不原地改語意(§11);新消費端一律接 `t4_broker_measure`(v3)。以下為原文。

v1 特徵(金額口徑,dollar;股數版特徵留待需求):
- gross_buy/sell_amt, net_amt, gross_amt
- directional_ratio = |buy−sell| / (buy+sell)  ∈[0,1](散戶混合≈0,單向資金≈1)
- n_symbols(當日碰幾檔)
- top1_share / top5_share(火力集中度:最重倉 1/5 檔佔自身 gross 比)

v2 特徵(2026-09-16,docs/OBSERVATORY_2026-09.md §1 L-A,O1 期;純計算邏輯見
measure/actor.py,此檔只負責 IO 組裝):
- multiplicity(決策者多重性,0-1 複合)、basket_self_sim(今日 vs 前一
  交易日購物籃 cosine)
- foreign_sim_buy/sell、fund_sim_buy/sell(+信賴帶)——分點購物籃 vs T3
  官方外資/投信日向量的 rank correlation
- sector_hhi / top_sector(產業碼:ws-core tickers)
- daytrade_assoc(所持股票 T3 day_trade_pct 以 gross 加權)
- foreign_sim_*_20d/60d、fund_sim_*_20d/60d(滾動活躍交易日平均)

描述先行(觀測站家法):以上全部是「看得懂什麼」的欄位,不做任何「能
預測報酬」的宣稱——任何此類想法留給 O6,見 docs/OBSERVATORY_2026-09.md §6。

記憶體紀律:逐月切塊(group 鍵含 date,月切無損)——單年單程序全量 group
含 list 排序會重演 30GB 壓縮頁事故。basket_self_sim 需要「前一交易日」,
每月切片向前多帶 7 個月曆天當緩衝(足以跨過連假),算完再篩掉緩衝部分;
T3/tickers 為小型參考資料,不受此限制。20/60D 滾動窗在年度聚合後、以
小型(分點×日,非分點×股票×日)DataFrame 一次計算,不逐月做——年度
聚合後的資料量僅百萬列等級,遠低於月切塊要防的億級規模。
"""

from __future__ import annotations

import datetime

import polars as pl
from ws_core.tickers import tickers_df

from ws_branch.measure import actor
from ws_branch.tables import io

_BUFFER_DAYS = 14  # 月初「前一交易日」可能落在上個月;過年是最長連假
# (2026 實測 2/23 前連休 12 個月曆天,恰好整段落在 2 月內未跨月界,但過年
# 哪一年落在哪個月邊界純看農曆,不能假設「這次沒跨月界」就是永遠安全——
# 14 天留出安全邊際,不得隨便縮小)


def compute_broker_day(t1_slice: pl.DataFrame) -> pl.DataFrame:
    """純計算:T1 切片(分點×股票×日)→ 分點×日 v1 特徵。可用合成資料單測。"""
    g = t1_slice.with_columns(
        (pl.col("buy_dollar") + pl.col("sell_dollar")).alias("gross"))
    return (g.group_by("broker", "broker_name", "date")
            .agg(
                pl.col("buy_dollar").sum().alias("gross_buy_amt"),
                pl.col("sell_dollar").sum().alias("gross_sell_amt"),
                pl.len().alias("n_symbols"),
                pl.col("gross").sum().alias("gross_amt"),
                pl.col("gross").max().alias("_top1"),
                pl.col("gross").sort(descending=True).head(5).sum().alias("_top5"),
            )
            .with_columns(
                (pl.col("gross_buy_amt") - pl.col("gross_sell_amt")).alias("net_amt"),
                ((pl.col("gross_buy_amt") - pl.col("gross_sell_amt")).abs()
                 / (pl.col("gross_buy_amt") + pl.col("gross_sell_amt"))
                 ).fill_nan(None).alias("directional_ratio"),
                (pl.col("_top1") / pl.col("gross_amt")).fill_nan(None)
                .alias("top1_share"),
                (pl.col("_top5") / pl.col("gross_amt")).fill_nan(None)
                .alias("top5_share"),
            )
            .drop("_top1", "_top5"))


def _tickers_industry() -> pl.DataFrame:
    return (tickers_df("tw")
            .select(pl.col("ticker_local").alias("symbol_id"), "industry_local"))


def build_year(year: int) -> pl.LazyFrame:
    tickers = _tickers_industry()
    t3_year = io.scan("t3_official_daily", start=f"{year}-01-01",
                       end=f"{year}-12-31").collect()
    months: list[pl.DataFrame] = []
    for m in range(1, 13):
        nxt_y, nxt_m = (year + 1, 1) if m == 12 else (year, m + 1)
        month_start = datetime.date(year, m, 1)
        month_end = datetime.date(nxt_y, nxt_m, 1)  # exclusive
        buffer_start = month_start - datetime.timedelta(days=_BUFFER_DAYS)
        month_end_incl = month_end - datetime.timedelta(days=1)

        # io.scan 跨年份 glob:買一天緩衝可能跨到上一年(1 月)也照樣抓得到
        buffered = (io.scan("t1_broker_daily", start=str(buffer_start),
                             end=str(month_end_incl))
                    .select("broker", "broker_name", "symbol_id", "date",
                            "buy_dollar", "sell_dollar")
                    .collect())
        if buffered.height == 0:
            continue
        month_sl = buffered.filter(pl.col("date") >= month_start)
        if month_sl.height == 0:
            continue

        base = compute_broker_day(month_sl)
        self_sim = actor.basket_self_similarity(buffered).filter(
            pl.col("date") >= month_start)
        t3_month = t3_year.filter((pl.col("date") >= month_start)
                                   & (pl.col("date") < month_end))
        idsim = actor.identity_similarity(month_sl, t3_month)
        sector = actor.sector_concentration(month_sl, tickers)
        dta = actor.daytrade_association(month_sl, t3_month)

        merged = (base
                  .join(self_sim, on=["broker", "date"], how="left")
                  .join(idsim, on=["broker", "date"], how="left")
                  .join(sector, on=["broker", "date"], how="left")
                  .join(dta, on=["broker", "date"], how="left"))
        months.append(merged)

    if not months:
        raise ValueError(f"t4_broker_features {year}: 無任何月份有資料")

    year_df = pl.concat(months, how="diagonal")
    year_df = actor.compute_multiplicity(year_df)
    year_df = actor.rolling_identity_similarity(year_df, windows=(20, 60))
    return year_df.lazy()
