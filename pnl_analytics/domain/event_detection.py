"""Event Detection: Identify smart money accumulation/distribution events.

Detects when PNL top-K brokers show abnormal individual large trades
(per-broker 2σ, like signal_report). An event is triggered when the
rolling count of large trades among top-K exceeds the aggregate threshold.

CRITICAL: Uses rolling PNL ranking — for each date T, top-K is defined
using only PNL data up to T. No look-ahead bias.

Pure functions — input/output are polars DataFrames and dataclasses.
"""

from dataclasses import dataclass
from datetime import date

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
        ranking_window_years: Rolling window (years) for PNL ranking.
                              PNL only meaningful from 2023+, so a 3-year
                              window avoids diluting with position-building noise.
        min_history_days: Minimum trading days of PNL history before
                          detecting events. Prevents ranking on noisy
                          early data.
    """
    top_k: int = 20
    window_days: int = 5
    threshold_sigma: float = 2.0
    ranking_window_years: int = 3
    min_history_days: int = 250


_EMPTY_EVENTS = {"date": pl.Date, "signal_value": pl.Float64, "direction": pl.Int8}


# =============================================================================
# Core Detection (Rolling — no look-ahead)
# =============================================================================

def detect_smart_money_events(
    trade_df: pl.DataFrame,
    pnl_daily_df: pl.DataFrame,
    config: EventConfig = EventConfig(),
) -> pl.DataFrame:
    """Detect smart money events using rolling PNL ranking + per-broker large trades.

    For each trading day T:
      1. Rank brokers by cumulative PNL up to T (no future data)
      2. Take top-K as that day's "smart money"
      3. Flag individual large trades per broker (|net_buy - mean| > 2σ)
      4. Count: how many top-K have large buys vs large sells

    Then apply rolling window, standardization, and threshold detection.

    Args:
        trade_df: Daily trade summary with columns:
                  broker (Utf8/Categorical), date (Date),
                  buy_shares (Int32/Int64), sell_shares (Int32/Int64).
        pnl_daily_df: Daily PNL with columns:
                      broker (Utf8), date (Date),
                      realized_pnl (Float64), unrealized_pnl (Float64).
        config: Detection parameters.

    Returns:
        DataFrame[date, signal_value, direction]
        direction: +1 (accumulation) or -1 (distribution)
    """
    # 1. Compute rolling-window total_pnl per broker per date
    window_days_str = f"{365 * config.ranking_window_years}d"
    daily_pnl = (
        pnl_daily_df
        .sort("date")
        .with_columns(
            pl.col("realized_pnl")
            .rolling_sum_by("date", window_size=window_days_str)
            .over("broker")
            .alias("cum_realized")
        )
        .with_columns(
            (pl.col("cum_realized") + pl.col("unrealized_pnl")).alias("total_pnl")
        )
        .select("date", "broker", "total_pnl")
    )

    # 2. For each date, rank brokers → flag top-K
    daily_ranked = (
        daily_pnl
        .with_columns(
            pl.col("total_pnl")
            .rank(method="ordinal", descending=True)
            .over("date")
            .alias("rank")
        )
        .filter(pl.col("rank") <= config.top_k)
        .select("date", "broker")
    )

    # 3. Enforce min history: skip early dates
    all_dates = sorted(daily_pnl["date"].unique().to_list())
    if len(all_dates) <= config.min_history_days:
        return pl.DataFrame(schema=_EMPTY_EVENTS)
    earliest_event_date = all_dates[config.min_history_days]
    daily_ranked = daily_ranked.filter(pl.col("date") >= earliest_event_date)

    if len(daily_ranked) == 0:
        return pl.DataFrame(schema=_EMPTY_EVENTS)

    # 4. Flag per-broker large trades (like signal_report: |net_buy - mean| > 2σ)
    large_trades = _flag_broker_large_trades(trade_df, config.threshold_sigma)

    # 5. Join top-K with large trade flags → daily count
    topk_large = daily_ranked.join(
        large_trades, on=["date", "broker"], how="inner",
    )

    if len(topk_large) == 0:
        return pl.DataFrame(schema=_EMPTY_EVENTS)

    daily_net = (
        topk_large
        .group_by("date")
        .agg(pl.col("large_dir").sum().alias("net_buy"))
        .sort("date")
    )

    # 6. Rolling sum → standardize → threshold → dedup
    return _signal_from_daily_net(daily_net, config)


def detect_placebo_events(
    trade_df: pl.DataFrame,
    pnl_daily_df: pl.DataFrame,
    config: EventConfig = EventConfig(),
    seed: int = 42,
) -> pl.DataFrame:
    """Detect events using random (non-top-K) brokers as placebo.

    Picks K random brokers (excluding the actual top-K at the END of
    the period) and uses their trades as a static set. Simpler than
    rolling placebo, but sufficient for a null check.

    Args:
        trade_df: Daily trade summary.
        pnl_daily_df: Daily PNL data.
        config: Detection parameters.
        seed: Random seed.

    Returns:
        Same schema as detect_smart_money_events.
    """
    import numpy as np

    # Compute end-of-period ranking to exclude actual top-K
    end_ranking = _compute_final_ranking(pnl_daily_df)
    if end_ranking is None or len(end_ranking) < config.top_k * 2:
        return pl.DataFrame(schema=_EMPTY_EVENTS)

    top_brokers = set(
        end_ranking.sort("total_pnl", descending=True)
        .head(config.top_k)["broker"].to_list()
    )
    all_brokers = end_ranking["broker"].to_list()
    eligible = [b for b in all_brokers if b not in top_brokers]

    if len(eligible) < config.top_k:
        return pl.DataFrame(schema=_EMPTY_EVENTS)

    rng = np.random.default_rng(seed)
    sampled = set(rng.choice(eligible, size=config.top_k, replace=False).tolist())

    # Flag per-broker large trades, filter to sampled brokers
    large_trades = _flag_broker_large_trades(trade_df, config.threshold_sigma)
    placebo_large = large_trades.filter(pl.col("broker").is_in(sampled))

    daily_net = (
        placebo_large
        .group_by("date")
        .agg(pl.col("large_dir").sum().alias("net_buy"))
        .sort("date")
    )

    # Skip min_history_days
    if len(daily_net) <= config.min_history_days:
        return pl.DataFrame(schema=_EMPTY_EVENTS)
    earliest = daily_net["date"].sort()[config.min_history_days]
    daily_net = daily_net.filter(pl.col("date") >= earliest)

    return _signal_from_daily_net(daily_net, config)


# =============================================================================
# Internal Helpers
# =============================================================================

def _flag_broker_large_trades(
    trade_df: pl.DataFrame,
    sigma: float = 2.0,
) -> pl.DataFrame:
    """Flag per-broker large trades (like signal_report).

    For each broker, compute mean/std of net_buy across all dates.
    Flag days where (net_buy - mean) / std exceeds ±sigma.

    Returns:
        DataFrame[date, broker, large_dir]
        large_dir: +1 (large buy), -1 (large sell), 0 (normal)
    """
    trades_with_net = (
        trade_df
        .with_columns(
            pl.col("broker").cast(pl.Utf8),
            (pl.col("buy_shares") - pl.col("sell_shares")).alias("net_buy"),
        )
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
            ((pl.col("net_buy") - pl.col("mean_nb")) / pl.col("std_nb")).alias("z_broker")
        )
        .with_columns(
            pl.when(pl.col("z_broker") > sigma)
            .then(pl.lit(1, dtype=pl.Int8))
            .when(pl.col("z_broker") < -sigma)
            .then(pl.lit(-1, dtype=pl.Int8))
            .otherwise(pl.lit(0, dtype=pl.Int8))
            .alias("large_dir")
        )
        .select("date", "broker", "large_dir")
    )


def _signal_from_daily_net(
    daily_net: pl.DataFrame,
    config: EventConfig,
) -> pl.DataFrame:
    """Rolling sum → z-score → threshold → dedup. Shared by main and placebo."""
    daily_net = daily_net.with_columns(
        pl.col("net_buy")
        .rolling_sum(window_size=config.window_days, min_periods=config.window_days)
        .alias("rolling_net_buy")
    )
    daily_net = daily_net.drop_nulls("rolling_net_buy")

    if len(daily_net) < 2:
        return pl.DataFrame(schema=_EMPTY_EVENTS)

    mean_val = daily_net["rolling_net_buy"].mean()
    std_val = daily_net["rolling_net_buy"].std()

    if std_val is None or std_val == 0:
        return pl.DataFrame(schema=_EMPTY_EVENTS)

    daily_net = daily_net.with_columns(
        ((pl.col("rolling_net_buy") - mean_val) / std_val).alias("z_score")
    )

    events = daily_net.filter(
        pl.col("z_score").abs() > config.threshold_sigma
    ).with_columns(
        pl.when(pl.col("z_score") > 0)
        .then(pl.lit(1, dtype=pl.Int8))
        .otherwise(pl.lit(-1, dtype=pl.Int8))
        .alias("direction")
    )

    if len(events) == 0:
        return pl.DataFrame(schema=_EMPTY_EVENTS)

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


def _compute_final_ranking(pnl_daily_df: pl.DataFrame) -> pl.DataFrame | None:
    """Compute end-of-period ranking from pnl_daily. Used by placebo."""
    if len(pnl_daily_df) == 0:
        return None
    return (
        pnl_daily_df
        .sort("date")
        .group_by("broker")
        .agg([
            pl.col("realized_pnl").sum(),
            pl.col("unrealized_pnl").last(),
        ])
        .with_columns(
            (pl.col("realized_pnl") + pl.col("unrealized_pnl")).alias("total_pnl")
        )
    )
