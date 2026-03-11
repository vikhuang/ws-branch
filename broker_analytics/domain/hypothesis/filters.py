"""Step 2: Event filter functions.

Each function identifies specific event dates and their direction.
Output schema: DataFrame[date: Date, direction: Int8]

Signature: (SymbolData, list[str], params: dict) -> pl.DataFrame
"""

from datetime import date

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
    """Strategy 3: Adding to winning position with significant floating profit.

    Conviction = broker has net_shares > 0, floating profit > min_profit_ratio,
    AND is still buying (net_buy > 0). Requires >= min_brokers on same day.
    params: min_brokers (int, default 3), min_profit_ratio (float, default 0.2)
    """
    min_brokers = params.get("min_brokers", 3)
    min_profit_ratio = params.get("min_profit_ratio", 0.2)

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
            (pl.col("net_shares") > 0)
            & (pl.col("avg_cost") > 0)
            & (
                pl.col("unrealized_pnl")
                / (pl.col("net_shares") * pl.col("avg_cost"))
                > min_profit_ratio
            )
            & (pl.col("net_buy") > 0)
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
    """Strategy 4: Multiple top brokers exiting or significantly reducing positions.

    Exit = net_shares crosses from positive to <= 0.
    Significant reduction = net_shares drops by > reduction_pct in one day.
    Uses rolling window to count collective exits.
    params: min_brokers (int, default 5), window_days (int, default 20),
            reduction_pct (float, default 0.5)
    """
    min_brokers = params.get("min_brokers", 5)
    window_days = params.get("window_days", 20)
    reduction_pct = params.get("reduction_pct", 0.5)

    trade_df = (
        data.trade_df
        .with_columns(pl.col("broker").cast(pl.Utf8))
        .filter(pl.col("broker").is_in(brokers))
        .sort("date")
    )

    if len(trade_df) == 0:
        return _EMPTY_EVENTS.clone()

    # Compute cumulative net_shares per broker
    positions = (
        trade_df
        .with_columns(
            (pl.col("buy_shares") - pl.col("sell_shares"))
            .cum_sum()
            .over("broker")
            .alias("net_shares")
        )
        .with_columns(
            pl.col("net_shares").shift(1).over("broker").alias("prev_shares")
        )
        .select("broker", "date", "net_shares", "prev_shares")
    )

    # Detect exit events per broker-date:
    # 1) Position crosses from positive to zero/negative
    # 2) Position drops by > reduction_pct (and was meaningful)
    exit_events = positions.filter(
        pl.col("prev_shares").is_not_null()
        & (pl.col("prev_shares") > 0)
        & (
            (pl.col("net_shares") <= 0)  # full exit
            | (
                (pl.col("net_shares")
                 < pl.col("prev_shares") * (1.0 - reduction_pct))
            )  # significant reduction
        )
    ).select("broker", "date")

    if len(exit_events) == 0:
        return _EMPTY_EVENTS.clone()

    # Rolling window: for each date, count unique brokers with exit
    # events in the trailing window_days
    all_dates = positions.select("date").unique().sort("date")
    exit_list = exit_events.to_dicts()

    from datetime import timedelta
    date_broker_exits: dict = {}
    for row in exit_list:
        date_broker_exits.setdefault(row["date"], set()).add(row["broker"])

    results = []
    sorted_dates = all_dates["date"].to_list()
    for d in sorted_dates:
        window_start = d - timedelta(days=window_days)
        n_exiting = len({
            b
            for exit_d, brokers_set in date_broker_exits.items()
            if window_start <= exit_d <= d
            for b in brokers_set
        })
        if n_exiting >= min_brokers:
            results.append({"date": d, "direction": -1})

    if not results:
        return _EMPTY_EVENTS.clone()

    return pl.DataFrame(results).with_columns(
        pl.col("direction").cast(pl.Int8)
    ).sort("date")


def filter_contrarian_on_panic(
    data: SymbolData, brokers: list[str], params: dict,
) -> pl.DataFrame:
    """Strategy 7: Top brokers buying on panic days.

    Panic = single-day drop > drop_pct OR 3-day cumulative drop > cum_drop_pct.
    params: drop_pct (float, default -0.02), cum_drop_pct (float, default -0.05),
            min_brokers (int, default 3)
    """
    drop_pct = params.get("drop_pct", -0.02)
    cum_drop_pct = params.get("cum_drop_pct", -0.05)
    min_brokers = params.get("min_brokers", 3)

    sym_prices = (
        data.prices
        .filter(pl.col("symbol_id") == data.symbol)
        .sort("date")
        .select("date", "close_price")
    )
    if len(sym_prices) < 4:
        return _EMPTY_EVENTS.clone()

    sym_prices = sym_prices.with_columns(
        (pl.col("close_price") / pl.col("close_price").shift(1) - 1.0)
        .alias("daily_return"),
        (pl.col("close_price") / pl.col("close_price").shift(3) - 1.0)
        .alias("cum_3d_return"),
    )

    # Single-day panic OR 3-day cumulative panic
    panic_dates = set(
        sym_prices
        .filter(
            (pl.col("daily_return") < drop_pct)
            | (pl.col("cum_3d_return") < cum_drop_pct)
        )
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


def filter_cluster_accumulation(
    data: SymbolData, brokers: list[str], params: dict,
) -> pl.DataFrame:
    """Strategy 5: Same broker has large trades in multiple cluster stocks simultaneously.

    Detects days where a selected broker has 2-sigma trades in >= min_cluster_stocks
    stocks within the cluster. Requires _cluster_trades injected by runner.
    params: min_cluster_stocks (int, default 2), sigma (float, default 2.0)
    """
    cluster_trades = params.get("_cluster_trades", {})
    min_cluster_stocks = params.get("min_cluster_stocks", 2)
    sigma = params.get("sigma", 2.0)

    if not cluster_trades:
        return _EMPTY_EVENTS.clone()

    # For each cluster stock, find large trade events by selected brokers
    from collections import defaultdict
    # (broker, date) -> {stock: direction}
    broker_date_stocks: dict[tuple, dict] = defaultdict(dict)

    for sym, trades in cluster_trades.items():
        sym_trades = trades.filter(
            pl.col("broker").cast(pl.Utf8).is_in(brokers)
        )
        if len(sym_trades) == 0:
            continue

        large = flag_large_trades(sym_trades, sigma)
        flagged = large.filter(pl.col("large_dir") != 0)

        for row in flagged.iter_rows(named=True):
            key = (row["broker"], row["date"])
            broker_date_stocks[key][sym] = row["large_dir"]

    # Events: dates where any broker hits >= min_cluster_stocks
    date_directions: dict = {}
    for (broker, date), stocks in broker_date_stocks.items():
        if len(stocks) >= min_cluster_stocks:
            # Net direction across cluster stocks
            net_dir = sum(stocks.values())
            if date not in date_directions:
                date_directions[date] = 0
            date_directions[date] += net_dir

    if not date_directions:
        return _EMPTY_EVENTS.clone()

    results = []
    for date, net in sorted(date_directions.items()):
        if net != 0:
            results.append({
                "date": date,
                "direction": 1 if net > 0 else -1,
            })

    if not results:
        return _EMPTY_EVENTS.clone()

    return pl.DataFrame(results).with_columns(
        pl.col("direction").cast(pl.Int8)
    ).sort("date")


def filter_concentration_increase(
    data: SymbolData, brokers: list[str], params: dict,
) -> pl.DataFrame:
    """Strategy 8: Concentrated brokers actively increasing position in this stock.

    Among brokers already concentrated in this stock (from selector),
    detect days when they add to their position (net_buy > 0 AND already hold shares).
    params: min_brokers (int, default 2)
    """
    min_brokers = params.get("min_brokers", 2)

    positions = derive_positions(
        data.trade_df, data.pnl_daily_df, data.prices, data.symbol
    )
    positions = positions.filter(pl.col("broker").is_in(brokers))

    if len(positions) == 0:
        return _EMPTY_EVENTS.clone()

    daily_net = (
        data.trade_df
        .with_columns(
            pl.col("broker").cast(pl.Utf8),
            (pl.col("buy_shares") - pl.col("sell_shares")).alias("net_buy"),
        )
        .filter(pl.col("broker").is_in(brokers))
        .select("broker", "date", "net_buy")
    )

    # Join positions with daily trades: concentrated broker has position AND is buying
    adding = (
        positions
        .join(daily_net, on=["broker", "date"], how="inner")
        .filter(
            (pl.col("net_shares") > 0)
            & (pl.col("net_buy") > 0)
        )
        .group_by("date")
        .agg(pl.len().alias("n_adding"))
        .filter(pl.col("n_adding") >= min_brokers)
        .with_columns(pl.lit(1, dtype=pl.Int8).alias("direction"))
        .select("date", "direction")
        .sort("date")
    )
    return adding


def filter_herding_divergence(
    data: SymbolData, brokers: list[str], params: dict,
) -> pl.DataFrame:
    """Strategy 9: Crowd buys but smart money doesn't, or vice versa.

    Herding index = crowd_buy_pct - smart_buy_pct.
    High herding (crowd buys, smart doesn't) → direction = -1 (short).
    Low herding (crowd sells, smart buys) → direction = +1 (long).
    params: herding_threshold (float, default 0.3)
    """
    threshold = params.get("herding_threshold", 0.3)
    smart_set = set(brokers)  # top-K from selector

    daily_net = (
        data.trade_df
        .with_columns(
            pl.col("broker").cast(pl.Utf8),
            (pl.col("buy_shares") - pl.col("sell_shares")).alias("net_buy"),
        )
    )

    # Smart money: fraction of top-K that are net buyers per day
    smart_daily = (
        daily_net.filter(pl.col("broker").is_in(smart_set))
        .group_by("date")
        .agg(
            (pl.col("net_buy") > 0).mean().alias("smart_buy_pct"),
        )
    )

    # Crowd: fraction of non-top-K that are net buyers per day
    crowd_daily = (
        daily_net.filter(~pl.col("broker").is_in(smart_set))
        .group_by("date")
        .agg(
            (pl.col("net_buy") > 0).mean().alias("crowd_buy_pct"),
        )
    )

    merged = smart_daily.join(crowd_daily, on="date", how="inner")
    merged = merged.with_columns(
        (pl.col("crowd_buy_pct") - pl.col("smart_buy_pct"))
        .alias("herding_index")
    )

    events = (
        merged
        .filter(pl.col("herding_index").abs() > threshold)
        .with_columns(
            pl.when(pl.col("herding_index") > 0)
            .then(pl.lit(-1, dtype=pl.Int8))   # crowd buys, smart doesn't → bearish
            .otherwise(pl.lit(1, dtype=pl.Int8))  # crowd sells, smart buys → bullish
            .alias("direction")
        )
        .select("date", "direction")
        .sort("date")
    )
    return events


def filter_large_trades_test_window(
    data: SymbolData, brokers: list[str], params: dict,
) -> pl.DataFrame:
    """Strategy 0: Large trades in test window only, with amount filter.

    Same logic as filter_large_trades but restricted to dates >= test_start_date
    and filtered by minimum trade amount.
    params: test_start_date (str, default "2024-01-01"),
            sigma (float, default 2.0),
            min_amount (int, default 10_000_000 TWD)
    """
    test_start_str = params.get("test_start_date", "2024-01-01")
    test_start = date.fromisoformat(test_start_str)
    sigma = params.get("sigma", 2.0)
    min_amount = params.get("min_amount", 10_000_000)

    # Test window + selected brokers only
    test_trades = data.trade_df.filter(
        pl.col("broker").cast(pl.Utf8).is_in(brokers),
        pl.col("date") >= test_start,
    )
    if len(test_trades) == 0:
        return _EMPTY_EVENTS.clone()

    # 2σ large trade detection on test window data
    large = flag_large_trades(test_trades, sigma)
    large = large.filter(pl.col("large_dir") != 0)
    if len(large) == 0:
        return _EMPTY_EVENTS.clone()

    # Amount filter: join back to get buy_amount/sell_amount
    amount_cols = test_trades.select(
        pl.col("broker").cast(pl.Utf8), "date", "buy_amount", "sell_amount",
    )
    large = large.join(amount_cols, on=["broker", "date"], how="left")
    large = large.filter(
        pl.when(pl.col("large_dir") > 0)
        .then(pl.col("buy_amount") >= min_amount)
        .otherwise(pl.col("sell_amount") >= min_amount)
    )
    if len(large) == 0:
        return _EMPTY_EVENTS.clone()

    # Aggregate by date (same as filter_large_trades)
    events = (
        large
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
