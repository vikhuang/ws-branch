"""Churn ratio: measure directional consensus among brokers.

Low churn (≈1) = all activity in one direction → strong consensus.
High churn (>>1) = lots of buying AND selling → disagreement/turnover.

    churn_ratio = gross / (net + ε)
    where gross = buy_shares + sell_shares
          net   = |buy_shares - sell_shares|

Pure functions — input/output are polars DataFrames.
"""

import polars as pl

# Epsilon to prevent division by zero
_EPS = 1.0


def compute_daily_churn(
    trade_df: pl.DataFrame,
    brokers: list[str],
) -> pl.DataFrame:
    """Compute per-date churn ratio for given brokers.

    Args:
        trade_df: Trade DataFrame with columns: broker, date, buy_shares, sell_shares.
        brokers: Broker list to compute churn for.

    Returns:
        DataFrame[date: Date, churn_ratio: Float64, gross: Float64, net_abs: Float64]
        sorted by date.
    """
    filtered = trade_df.filter(
        pl.col("broker").cast(pl.Utf8).is_in(brokers)
    )
    if len(filtered) == 0:
        return pl.DataFrame(schema={
            "date": pl.Date, "churn_ratio": pl.Float64,
            "gross": pl.Float64, "net_abs": pl.Float64,
        })

    return (
        filtered
        .group_by("date")
        .agg(
            (pl.col("buy_shares") + pl.col("sell_shares")).sum().alias("gross"),
            (pl.col("buy_shares") - pl.col("sell_shares")).sum().abs().alias("net_abs"),
        )
        .with_columns(
            (pl.col("gross") / (pl.col("net_abs") + _EPS)).alias("churn_ratio"),
        )
        .sort("date")
    )


def compute_rolling_churn(
    trade_df: pl.DataFrame,
    brokers: list[str],
    window: int = 5,
) -> pl.DataFrame:
    """Compute rolling N-day churn ratio for given brokers.

    Rolling churn = Σgross(N days) / (|Σnet(N days)| + ε).
    Captures sustained directional consensus over a window.

    Args:
        trade_df: Trade DataFrame with columns: broker, date, buy_shares, sell_shares.
        brokers: Broker list to compute churn for.
        window: Rolling window in trading days.

    Returns:
        DataFrame[date: Date, rolling_churn: Float64] sorted by date.
        First (window-1) rows will be null.
    """
    filtered = trade_df.filter(
        pl.col("broker").cast(pl.Utf8).is_in(brokers)
    )
    if len(filtered) == 0:
        return pl.DataFrame(schema={
            "date": pl.Date, "rolling_churn": pl.Float64,
        })

    daily = (
        filtered
        .group_by("date")
        .agg(
            (pl.col("buy_shares") + pl.col("sell_shares")).sum().alias("gross"),
            (pl.col("buy_shares") - pl.col("sell_shares")).sum().alias("net"),
        )
        .sort("date")
    )

    return (
        daily
        .with_columns(
            pl.col("gross")
            .rolling_sum(window_size=window, min_periods=window)
            .alias("rolling_gross"),
            pl.col("net")
            .rolling_sum(window_size=window, min_periods=window)
            .abs()
            .alias("rolling_net_abs"),
        )
        .with_columns(
            (pl.col("rolling_gross") / (pl.col("rolling_net_abs") + _EPS))
            .alias("rolling_churn"),
        )
        .select("date", "rolling_churn")
        .drop_nulls("rolling_churn")
    )
