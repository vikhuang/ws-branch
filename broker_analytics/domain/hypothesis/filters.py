"""Step 2: Event filter functions.

Each function identifies specific event dates and their direction.
Output schema: DataFrame[date: Date, direction: Int8]

Signature: (SymbolData, list[str], params: dict) -> pl.DataFrame
"""

import polars as pl

from broker_analytics.domain.large_trade import flag_large_trades
from broker_analytics.domain.hypothesis.types import SymbolData
from broker_analytics.domain.hypothesis.position import derive_positions


_EMPTY_EVENTS = pl.DataFrame(schema={"date": pl.Date, "direction": pl.Int8})


def filter_large_trades(
    data: SymbolData, brokers: list[str], params: dict,
) -> pl.DataFrame:
    """Filter to large trade dates by selected brokers.

    Used by strategies 1, 2, 5, 6.
    params: sigma (float, default 2.0)
    """
    sigma = params.get("sigma", 2.0)
    trade_df = data.trade_df.filter(
        pl.col("broker").cast(pl.Utf8).is_in(brokers)
    )
    if len(trade_df) == 0:
        return _EMPTY_EVENTS.clone()

    large = flag_large_trades(trade_df, sigma)
    events = (
        large
        .filter(pl.col("large_dir") != 0)
        .group_by("date")
        .agg(pl.col("large_dir").sum().alias("net_dir"))
        .filter(pl.col("net_dir") != 0)
        .with_columns(
            pl.when(pl.col("net_dir") > 0)
            .then(pl.lit(1, dtype=pl.Int8))
            .otherwise(pl.lit(-1, dtype=pl.Int8))
            .alias("direction")
        )
        .select("date", "direction")
        .sort("date")
    )
    return events


def filter_conviction_signals(
    data: SymbolData, brokers: list[str], params: dict,
) -> pl.DataFrame:
    """Strategy 3: Adding to winning position (unrealized_pnl > 0 AND net_buy > 0).

    Uses Plan A position derivation.
    params: min_brokers (int, default 3)
    """
    min_brokers = params.get("min_brokers", 3)
    positions = derive_positions(
        data.trade_df, data.pnl_daily_df, data.prices, data.symbol
    )
    positions = positions.filter(pl.col("broker").is_in(brokers))

    if len(positions) == 0:
        return _EMPTY_EVENTS.clone()

    # Join with daily trades to get net_buy
    daily_net = (
        data.trade_df
        .with_columns(
            pl.col("broker").cast(pl.Utf8),
            (pl.col("buy_shares") - pl.col("sell_shares")).alias("net_buy"),
        )
        .select("broker", "date", "net_buy")
    )

    conv = (
        positions
        .join(daily_net, on=["broker", "date"], how="inner")
        .filter(
            (pl.col("unrealized_pnl") > 0)
            & (pl.col("net_buy") > 0)
            & (pl.col("net_shares") > 0)
        )
        .group_by("date")
        .agg(pl.len().alias("n_conviction"))
        .filter(pl.col("n_conviction") >= min_brokers)
        .with_columns(pl.lit(1, dtype=pl.Int8).alias("direction"))
        .select("date", "direction")
        .sort("date")
    )
    return conv


def filter_collective_exodus(
    data: SymbolData, brokers: list[str], params: dict,
) -> pl.DataFrame:
    """Strategy 4: Multiple top brokers simultaneously reducing positions.

    params: min_brokers (int, default 5)
    """
    min_brokers = params.get("min_brokers", 5)

    daily_net = (
        data.trade_df
        .with_columns(
            pl.col("broker").cast(pl.Utf8),
            (pl.col("buy_shares") - pl.col("sell_shares")).alias("net_buy"),
        )
        .filter(pl.col("broker").is_in(brokers))
        .select("broker", "date", "net_buy")
    )

    if len(daily_net) == 0:
        return _EMPTY_EVENTS.clone()

    daily_agg = (
        daily_net
        .group_by("date")
        .agg(
            (pl.col("net_buy") < 0).sum().alias("n_sellers"),
        )
    )

    exodus = (
        daily_agg
        .filter(pl.col("n_sellers") >= min_brokers)
        .with_columns(pl.lit(-1, dtype=pl.Int8).alias("direction"))
        .select("date", "direction")
        .sort("date")
    )
    return exodus


def filter_contrarian_on_panic(
    data: SymbolData, brokers: list[str], params: dict,
) -> pl.DataFrame:
    """Strategy 7: Top brokers buying on panic days (stock drop > threshold).

    params: drop_pct (float, default -0.02), min_brokers (int, default 3)
    """
    drop_pct = params.get("drop_pct", -0.02)
    min_brokers = params.get("min_brokers", 3)

    sym_prices = (
        data.prices
        .filter(pl.col("symbol_id") == data.symbol)
        .sort("date")
        .select("date", "close_price")
    )
    if len(sym_prices) < 2:
        return _EMPTY_EVENTS.clone()

    sym_prices = sym_prices.with_columns(
        (pl.col("close_price") / pl.col("close_price").shift(1) - 1.0)
        .alias("daily_return")
    )

    panic_dates = set(
        sym_prices
        .filter(pl.col("daily_return") < drop_pct)
        ["date"].to_list()
    )

    if not panic_dates:
        return _EMPTY_EVENTS.clone()

    daily_net = (
        data.trade_df
        .with_columns(
            pl.col("broker").cast(pl.Utf8),
            (pl.col("buy_shares") - pl.col("sell_shares")).alias("net_buy"),
        )
        .filter(pl.col("broker").is_in(brokers))
        .filter(pl.col("date").is_in(panic_dates))
        .filter(pl.col("net_buy") > 0)
        .group_by("date")
        .agg(pl.len().alias("n_buyers"))
        .filter(pl.col("n_buyers") >= min_brokers)
        .with_columns(pl.lit(1, dtype=pl.Int8).alias("direction"))
        .select("date", "direction")
        .sort("date")
    )
    return daily_net


def filter_hhi_breakout(
    data: SymbolData, brokers: list[str], params: dict,
) -> pl.DataFrame:
    """Strategy 8: HHI of position concentration breakout.

    params: z_threshold (float, default 2.0)
    """
    z_threshold = params.get("z_threshold", 2.0)

    positions = derive_positions(
        data.trade_df, data.pnl_daily_df, data.prices, data.symbol
    )
    if len(positions) == 0:
        return _EMPTY_EVENTS.clone()

    # Compute daily HHI of position weights
    daily_hhi = (
        positions
        .with_columns(pl.col("net_shares").abs().alias("abs_pos"))
        .with_columns(
            (pl.col("abs_pos") / pl.col("abs_pos").sum().over("date"))
            .alias("weight")
        )
        .with_columns(
            (pl.col("weight") ** 2).alias("w_sq")
        )
        .group_by("date")
        .agg(pl.col("w_sq").sum().alias("hhi"))
        .sort("date")
    )

    if len(daily_hhi) < 30:
        return _EMPTY_EVENTS.clone()

    mean_hhi = daily_hhi["hhi"].mean()
    std_hhi = daily_hhi["hhi"].std()
    if std_hhi is None or std_hhi == 0:
        return _EMPTY_EVENTS.clone()

    breakout = (
        daily_hhi
        .with_columns(
            ((pl.col("hhi") - mean_hhi) / std_hhi).alias("z_hhi")
        )
        .filter(pl.col("z_hhi").abs() > z_threshold)
        .with_columns(
            pl.when(pl.col("z_hhi") > 0)
            .then(pl.lit(1, dtype=pl.Int8))
            .otherwise(pl.lit(-1, dtype=pl.Int8))
            .alias("direction")
        )
        .select("date", "direction")
    )
    return breakout


def filter_herding_agreement(
    data: SymbolData, brokers: list[str], params: dict,
) -> pl.DataFrame:
    """Strategy 9: Days when top-K and bottom-K trade in same direction.

    brokers list contains top_k followed by bottom_k (from select_top_and_bottom_k).
    """
    top_k = params.get("top_k", 20)
    top_brokers = set(brokers[:top_k])
    bottom_brokers = set(brokers[top_k:])

    daily_net = (
        data.trade_df
        .with_columns(
            pl.col("broker").cast(pl.Utf8),
            (pl.col("buy_shares") - pl.col("sell_shares")).alias("net_buy"),
        )
    )

    top_daily = (
        daily_net.filter(pl.col("broker").is_in(top_brokers))
        .group_by("date")
        .agg(pl.col("net_buy").sum().alias("top_net"))
    )
    bottom_daily = (
        daily_net.filter(pl.col("broker").is_in(bottom_brokers))
        .group_by("date")
        .agg(pl.col("net_buy").sum().alias("bottom_net"))
    )

    merged = top_daily.join(bottom_daily, on="date", how="inner")

    agreement = (
        merged
        .filter(
            ((pl.col("top_net") > 0) & (pl.col("bottom_net") > 0))
            | ((pl.col("top_net") < 0) & (pl.col("bottom_net") < 0))
        )
        .with_columns(
            pl.when(pl.col("top_net") > 0)
            .then(pl.lit(1, dtype=pl.Int8))
            .otherwise(pl.lit(-1, dtype=pl.Int8))
            .alias("direction")
        )
        .select("date", "direction")
        .sort("date")
    )
    return agreement
