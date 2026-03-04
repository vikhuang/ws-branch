"""Large trade detection: per-broker 2σ threshold.

For each broker, computes mean/std of net_buy across all dates.
Flags days where |z-score| exceeds the threshold as large trades.

Used by: event_detection, signal_report, market_scan
"""

import polars as pl


def flag_large_trades(
    trade_df: pl.DataFrame,
    sigma: float = 2.0,
) -> pl.DataFrame:
    """Flag per-broker large trades using z-score threshold.

    Args:
        trade_df: DataFrame with columns [broker, date, buy_shares, sell_shares].
        sigma: Z-score threshold for flagging (default 2.0).

    Returns:
        DataFrame[date, broker, net_buy, z_score, large_dir, mean_nb, std_nb]
        large_dir: +1 (large buy), -1 (large sell), 0 (normal).
        Only includes brokers with non-zero std.
    """
    trades_with_net = trade_df.with_columns(
        pl.col("broker").cast(pl.Utf8),
        (pl.col("buy_shares") - pl.col("sell_shares")).alias("net_buy"),
    )

    broker_stats = (
        trades_with_net
        .group_by("broker")
        .agg(
            pl.col("net_buy").mean().alias("mean_nb"),
            pl.col("net_buy").std().alias("std_nb"),
        )
        .filter(pl.col("std_nb") > 0)
    )

    return (
        trades_with_net
        .join(broker_stats, on="broker")
        .with_columns(
            ((pl.col("net_buy") - pl.col("mean_nb")) / pl.col("std_nb")).alias("z_score")
        )
        .with_columns(
            pl.when(pl.col("z_score") > sigma)
            .then(pl.lit(1, dtype=pl.Int8))
            .when(pl.col("z_score") < -sigma)
            .then(pl.lit(-1, dtype=pl.Int8))
            .otherwise(pl.lit(0, dtype=pl.Int8))
            .alias("large_dir")
        )
        .select("date", "broker", "net_buy", "z_score", "large_dir", "mean_nb", "std_nb")
    )
