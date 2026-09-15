"""Broker taxonomy v2 Q4:broker × symbol 專營性偵測。

user 直覺:「這就是他們在做的股票」— 特定 broker 在特定 symbol 上進出量是
其他 broker 的數倍到數十倍,而且集中。用資料驗證這個直覺,產出:
- broker → [做的 symbols] map(高集中度 + 高 market share + 持續性)
- 各 cluster「做的股票」分布(C1 虎尾幫是否有特徵性 stock list?)

方法:
1. 對每 (broker, symbol) pair 跨 114 sample days 聚合:total dollar vol
2. 相對於 broker 總量的集中度 & 相對於 symbol 總量的 market share
3. 持續性(該 pair 有交易的天數 / broker 活躍天數)
4. 三個門檻同時滿足 → 「做」

輸出:experiments/broker_taxonomy/broker_symbol_pairs.parquet
      experiments/broker_taxonomy/making_stocks_by_cluster.md
"""
from __future__ import annotations

from pathlib import Path

import polars as pl

from src.data_layer.broker_loader import BROKER_TX_DIR
from src.data_layer.calendar import trading_dates
import datetime

REPO = Path(__file__).resolve().parents[2]
OUT_DIR = REPO / "experiments/broker_taxonomy"
PAIRS_PATH = OUT_DIR / "broker_symbol_pairs.parquet"
MAKING_MD = OUT_DIR / "making_stocks_by_cluster.md"

WINDOW_LO = datetime.date(2021, 1, 4)
WINDOW_HI = datetime.date(2025, 8, 29)

# 「做」門檻(pre-declare exploration,可迭代)
CONCENTRATION_THRESHOLD = 0.03    # symbol 佔 broker 至少 3%
MARKET_SHARE_THRESHOLD = 0.01     # broker 佔 symbol 交易至少 1%
PERSISTENCE_THRESHOLD = 0.30      # 至少 30% 活躍天有交易此 symbol


def _load_and_aggregate_day(day: datetime.date) -> pl.DataFrame | None:
    """單日 broker_tx → (broker, symbol) 級 dollar vol。"""
    path = BROKER_TX_DIR / f"broker_tx_{day.strftime('%Y%m%d')}.parquet"
    if not path.exists():
        return None
    lf = pl.scan_parquet(str(path))
    lf = lf.filter((pl.col("price") != "-")
                   & ((pl.col("buy") + pl.col("sell")) > 0))
    lf = lf.with_columns(
        pl.col("price").str.replace_all(",", "").cast(pl.Float64, strict=False).alias("_price"),
    ).filter(pl.col("_price").is_not_null())
    lf = lf.with_columns(
        (pl.col("_price") * (pl.col("buy") + pl.col("sell")) * 0.5).alias("dollar_vol_row")
    )
    per_pair = (lf
        .group_by(["broker_name", "symbol_id"])
        .agg(pl.col("dollar_vol_row").sum().alias("dollar_vol"))
        .collect())
    return per_pair.with_columns(pl.lit(day).alias("date"))


def main() -> None:
    all_dates = trading_dates(WINDOW_LO, WINDOW_HI)
    sample_dates = all_dates[::10]
    print(f"Processing {len(sample_dates)} sample days ({sample_dates[0]} ~ {sample_dates[-1]})")

    day_frames = []
    for i, d in enumerate(sample_dates):
        frame = _load_and_aggregate_day(d)
        if frame is not None:
            day_frames.append(frame)
        if (i + 1) % 20 == 0:
            print(f"  ... {i+1}/{len(sample_dates)}")
    all_days = pl.concat(day_frames)
    print(f"Total (broker, symbol, date) rows: {len(all_days)}")

    # broker's total per day (for concentration)
    broker_day_total = (all_days.group_by(["broker_name", "date"])
                        .agg(pl.col("dollar_vol").sum().alias("broker_day_total")))
    # symbol's total per day (for market share)
    symbol_day_total = (all_days.group_by(["symbol_id", "date"])
                         .agg(pl.col("dollar_vol").sum().alias("symbol_day_total")))

    joined = (all_days
              .join(broker_day_total, on=["broker_name", "date"])
              .join(symbol_day_total, on=["symbol_id", "date"])
              .with_columns(
                  (pl.col("dollar_vol") / pl.col("broker_day_total")).alias("day_concentration"),
                  (pl.col("dollar_vol") / pl.col("symbol_day_total")).alias("day_market_share"),
              ))

    # 聚合到 (broker, symbol) pair 跨 days:median 集中度 & 市佔 & 持續性
    broker_active_days = (all_days.group_by("broker_name")
                          .agg(pl.col("date").n_unique().alias("broker_active_days")))
    pair_agg = (joined
                .group_by(["broker_name", "symbol_id"])
                .agg(
                    pl.col("dollar_vol").sum().alias("pair_total_dollar_vol"),
                    pl.col("date").n_unique().alias("pair_days"),
                    pl.col("day_concentration").median().alias("median_day_concentration"),
                    pl.col("day_market_share").median().alias("median_day_market_share"),
                    pl.col("day_market_share").max().alias("max_day_market_share"),
                )
                .join(broker_active_days, on="broker_name")
                .with_columns(
                    (pl.col("pair_days") / pl.col("broker_active_days")).alias("persistence"),
                ))

    # 加 cluster label
    try:
        clusters = pl.read_parquet(OUT_DIR / "broker_clusters.parquet").select(
            ["broker_name", "cluster"])
        pair_agg = pair_agg.join(clusters, on="broker_name", how="left")
    except Exception:
        pass

    # 標記「做」= concentration + market_share + persistence 三門檻
    pair_agg = pair_agg.with_columns(
        ((pl.col("median_day_concentration") >= CONCENTRATION_THRESHOLD)
         & (pl.col("median_day_market_share") >= MARKET_SHARE_THRESHOLD)
         & (pl.col("persistence") >= PERSISTENCE_THRESHOLD))
        .alias("is_making")
    )
    pair_agg.write_parquet(PAIRS_PATH)

    n_making_pairs = pair_agg["is_making"].sum()
    n_brokers_with_making = pair_agg.filter(pl.col("is_making"))["broker_name"].n_unique()
    print(f"\n總 pairs: {len(pair_agg)}")
    print(f"「做」pairs 數: {n_making_pairs}")
    print(f"有「做」股票的 brokers 數: {n_brokers_with_making}")

    # 前 20 「做」pair by market share
    top = (pair_agg.filter(pl.col("is_making"))
           .sort("median_day_market_share", descending=True)
           .select(["broker_name", "symbol_id", "cluster",
                    "median_day_market_share", "median_day_concentration",
                    "persistence", "pair_days"])
           .head(30))
    print(f"\n=== Top 30 「做」pairs by market share ===")
    print(top)

    # 各 cluster 有多少 broker 有「做」股票、平均「做」幾支
    if "cluster" in pair_agg.columns:
        by_cluster = (pair_agg.filter(pl.col("is_making"))
                       .group_by(["broker_name", "cluster"])
                       .agg(pl.len().alias("n_making_stocks"))
                       .group_by("cluster")
                       .agg(
                           pl.len().alias("n_brokers"),
                           pl.col("n_making_stocks").mean().alias("avg_making_per_broker"),
                           pl.col("n_making_stocks").median().alias("median_making_per_broker"),
                           pl.col("n_making_stocks").max().alias("max_making_per_broker"),
                       ).sort("cluster"))
        print(f"\n=== 各 cluster 的「做」stock 統計 ===")
        print(by_cluster)


if __name__ == "__main__":
    main()
