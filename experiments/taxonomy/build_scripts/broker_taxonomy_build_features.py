"""Broker taxonomy exploration Q1:建 broker × day 行為特徵矩陣。

Descriptive/exploratory research(見 [[feedback_exploration_vs_confirmation]]):
- 目的:描述台股分點行為的 fingerprint,而非測試 predictive claim
- 資料窗:2021-01-04(broker_tx 起) ~ 2025-08-29(frenzy OOS 起始前)
- 不 governed_study(不是 predictive claim family);但明確 window 避開 EMBARGO

輸出:experiments/broker_taxonomy/broker_day_features.parquet
- 每 (broker, date) 一行,多維行為特徵
- 首輪跑 sample(每 ~10 交易日取一天)先看 clustering 可行性
"""
from __future__ import annotations

import datetime
from pathlib import Path

import polars as pl

from src.data_layer.broker_loader import BROKER_TX_DIR
from src.data_layer.calendar import trading_dates

REPO = Path(__file__).resolve().parents[2]
OUT_DIR = REPO / "experiments/broker_taxonomy"
OUT_FEATURES = OUT_DIR / "broker_day_features.parquet"

# 資料窗:broker_tx 起始 ~ frenzy OOS 前(避開 confirmation phase 使用的 OOS)
WINDOW_LO = datetime.date(2021, 1, 4)
WINDOW_HI = datetime.date(2025, 8, 29)


def _compute_broker_day_features(day: datetime.date) -> pl.DataFrame | None:
    """單日 broker_tx → broker × day features。返回 None 若檔案缺。

    v2 追加特徵:aggregate_share = aggregate_rows / (aggregate + per_price rows)
    → 補 v1 遺漏的 -自營 分點(全走 aggregate 行,原本被 filter 掉)
    """
    path = BROKER_TX_DIR / f"broker_tx_{day.strftime('%Y%m%d')}.parquet"
    if not path.exists():
        return None
    df = pl.scan_parquet(str(path))
    df = df.filter((pl.col("buy") + pl.col("sell")) > 0)

    # v2:先算 aggregate_share per broker(包含 aggregate + per_price)
    all_rows = df.group_by("broker_name").agg(
        pl.len().alias("total_rows"),
        (pl.col("price") == "-").sum().alias("aggregate_rows"),
    ).with_columns(
        (pl.col("aggregate_rows") / pl.col("total_rows")).alias("aggregate_share")
    ).select(["broker_name", "aggregate_share"])

    non_agg = df.filter(pl.col("price") != "-")
    non_agg = non_agg.with_columns(
        (pl.col("buy") + pl.col("sell")).alias("row_shares")
    )
    features = (non_agg
        .group_by("broker_name")
        .agg(
            pl.len().alias("n_rows"),
            pl.col("row_shares").sum().alias("total_shares"),
            pl.col("row_shares").mean().alias("mean_row_shares"),
            pl.col("row_shares").median().alias("median_row_shares"),
            pl.col("row_shares").max().alias("max_row_shares"),
            pl.col("symbol_id").n_unique().alias("n_symbols"),
            pl.col("row_shares").sum().alias("_total_for_gini"),
        )
        .with_columns(
            (pl.col("total_shares") / pl.col("n_symbols")).alias("shares_per_symbol"),
            (pl.col("max_row_shares") / pl.col("total_shares")).alias("max_row_concentration"),
        )
    )
    top_sym = (non_agg
        .group_by(["broker_name", "symbol_id"])
        .agg(pl.col("row_shares").sum().alias("sym_shares"))
        .group_by("broker_name")
        .agg(
            pl.col("sym_shares").max().alias("top1_symbol_shares"),
            pl.col("sym_shares").top_k(5).sum().alias("top5_symbol_shares"),
        ))
    features = features.join(top_sym, on="broker_name", how="left").with_columns(
        (pl.col("top1_symbol_shares") / pl.col("total_shares")).alias("top1_symbol_concentration"),
        (pl.col("top5_symbol_shares") / pl.col("total_shares")).alias("top5_symbol_concentration"),
    ).drop(["_total_for_gini", "top1_symbol_shares", "top5_symbol_shares"])

    # v2:merge aggregate_share(此欄含 aggregate-only brokers,需 outer join)
    features = all_rows.join(features, on="broker_name", how="left")
    features = features.with_columns(pl.lit(day).alias("date")).collect()
    return features


def main(mode: str = "sample") -> None:
    """mode='sample' 每 ~10 交易日取一天;mode='full' 每天。"""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    all_dates = trading_dates(WINDOW_LO, WINDOW_HI)
    if mode == "sample":
        dates = all_dates[::10]  # 每 10 交易日一取
    elif mode == "full":
        dates = all_dates
    else:
        raise ValueError(f"unknown mode: {mode}")
    print(f"[taxonomy] mode={mode}, {len(dates)} days: {dates[0]} ~ {dates[-1]}")

    all_features = []
    missing = 0
    for i, d in enumerate(dates):
        feat = _compute_broker_day_features(d)
        if feat is None:
            missing += 1
        else:
            all_features.append(feat)
        if (i + 1) % 20 == 0:
            print(f"  ... {i+1}/{len(dates)} days processed, {missing} missing")

    if not all_features:
        print("No features computed.")
        return
    combined = pl.concat(all_features)
    print(f"\nTotal rows: {len(combined)}, brokers: {combined['broker_name'].n_unique()}, "
          f"days: {combined['date'].n_unique()}")
    print(f"Missing files: {missing}")
    print(f"\nSchema: {combined.schema}")
    print(f"\nSample:")
    print(combined.head(5))
    combined.write_parquet(OUT_FEATURES)
    print(f"\nWritten to {OUT_FEATURES}")


if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "sample"
    main(mode)
