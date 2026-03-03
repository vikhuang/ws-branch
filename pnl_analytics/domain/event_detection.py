"""Event Detection: Identify smart money accumulation/distribution events.

Detects when PNL top-K brokers' cumulative net buying breaks above or
below a threshold (measured in standard deviations). These events form
the basis for event study analysis.

Pure functions — input/output are polars DataFrames and dataclasses.
"""

from dataclasses import dataclass

import polars as pl


# =============================================================================
# Configuration
# =============================================================================

@dataclass(frozen=True, slots=True)
class EventConfig:
    """Parameters for event detection.

    Attributes:
        top_k: Number of top PNL brokers to track.
        window_days: Rolling window for cumulative net buy.
        threshold_sigma: Event threshold in standard deviations.
    """
    top_k: int = 20
    window_days: int = 5
    threshold_sigma: float = 2.0


# =============================================================================
# Core Detection
# =============================================================================

def detect_smart_money_events(
    trade_df: pl.DataFrame,
    ranking_df: pl.DataFrame,
    config: EventConfig = EventConfig(),
) -> pl.DataFrame:
    """Detect smart money accumulation/distribution events.

    An event is triggered when the rolling N-day net buy of PNL top-K
    brokers exceeds ±threshold_sigma standard deviations from the mean.
    Consecutive same-direction events are deduplicated (first day only).

    Args:
        trade_df: Daily trade summary with columns:
                  broker (Utf8/Categorical), date (Date),
                  buy_shares (Int32/Int64), sell_shares (Int32/Int64).
        ranking_df: PNL ranking with columns:
                    broker (Utf8), rank (UInt32), total_pnl (Float64).
        config: Detection parameters.

    Returns:
        DataFrame with columns:
            date (Date): Event date
            signal_value (Float64): Standardized signal strength
            direction (Int8): +1 (accumulation) or -1 (distribution)
    """
    # 1. Select top-K brokers by PNL
    top_brokers = (
        ranking_df
        .sort("total_pnl", descending=True)
        .head(config.top_k)
        .select("broker")
    )

    # 2. Filter trades to top-K brokers (cast to Utf8 for join compatibility)
    top_trades = (
        trade_df
        .with_columns(pl.col("broker").cast(pl.Utf8))
        .join(top_brokers, on="broker", how="inner")
    )

    if len(top_trades) == 0:
        return pl.DataFrame(schema={
            "date": pl.Date,
            "signal_value": pl.Float64,
            "direction": pl.Int8,
        })

    # 3. Daily aggregate net buy across top-K brokers
    daily_net = (
        top_trades
        .group_by("date")
        .agg(
            (pl.col("buy_shares").sum() - pl.col("sell_shares").sum())
            .alias("net_buy")
        )
        .sort("date")
    )

    # 4. Rolling sum over window
    daily_net = daily_net.with_columns(
        pl.col("net_buy")
        .rolling_sum(window_size=config.window_days, min_periods=config.window_days)
        .alias("rolling_net_buy")
    )

    # Drop rows before window is filled
    daily_net = daily_net.drop_nulls("rolling_net_buy")

    if len(daily_net) < 2:
        return pl.DataFrame(schema={
            "date": pl.Date,
            "signal_value": pl.Float64,
            "direction": pl.Int8,
        })

    # 5. Standardize: z = (rolling - mean) / std
    mean_val = daily_net["rolling_net_buy"].mean()
    std_val = daily_net["rolling_net_buy"].std()

    if std_val is None or std_val == 0:
        return pl.DataFrame(schema={
            "date": pl.Date,
            "signal_value": pl.Float64,
            "direction": pl.Int8,
        })

    daily_net = daily_net.with_columns(
        ((pl.col("rolling_net_buy") - mean_val) / std_val).alias("z_score")
    )

    # 6. Flag events exceeding threshold
    events = daily_net.filter(
        pl.col("z_score").abs() > config.threshold_sigma
    ).with_columns(
        pl.when(pl.col("z_score") > 0)
        .then(pl.lit(1, dtype=pl.Int8))
        .otherwise(pl.lit(-1, dtype=pl.Int8))
        .alias("direction")
    )

    if len(events) == 0:
        return pl.DataFrame(schema={
            "date": pl.Date,
            "signal_value": pl.Float64,
            "direction": pl.Int8,
        })

    # 7. Dedup: consecutive same-direction events → keep first
    events = events.with_columns(
        (pl.col("direction") != pl.col("direction").shift(1)).alias("new_event")
    ).filter(
        pl.col("new_event")
    ).select(
        "date",
        pl.col("z_score").alias("signal_value"),
        "direction",
    )

    return events


def detect_placebo_events(
    trade_df: pl.DataFrame,
    ranking_df: pl.DataFrame,
    config: EventConfig = EventConfig(),
    seed: int = 42,
) -> pl.DataFrame:
    """Detect events using random (non-top-K) brokers as placebo.

    Same logic as detect_smart_money_events but with randomly selected
    brokers instead of PNL top-K. Used for robustness testing.

    Args:
        trade_df: Daily trade summary.
        ranking_df: PNL ranking (used to determine K and exclude top-K).
        config: Detection parameters (top_k determines sample size).
        seed: Random seed for reproducibility.

    Returns:
        Same schema as detect_smart_money_events.
    """
    import numpy as np

    # Exclude top-K brokers, sample K random ones
    top_brokers = set(
        ranking_df
        .sort("total_pnl", descending=True)
        .head(config.top_k)["broker"]
        .to_list()
    )
    all_brokers = ranking_df["broker"].to_list()
    eligible = [b for b in all_brokers if b not in top_brokers]

    if len(eligible) < config.top_k:
        return pl.DataFrame(schema={
            "date": pl.Date,
            "signal_value": pl.Float64,
            "direction": pl.Int8,
        })

    rng = np.random.default_rng(seed)
    sampled = rng.choice(eligible, size=config.top_k, replace=False).tolist()

    # Build a fake ranking with sampled brokers
    fake_ranking = pl.DataFrame({
        "broker": sampled,
        "rank": list(range(1, config.top_k + 1)),
        "total_pnl": [1.0] * config.top_k,  # Dummy values
    })

    return detect_smart_money_events(trade_df, fake_ranking, config)
