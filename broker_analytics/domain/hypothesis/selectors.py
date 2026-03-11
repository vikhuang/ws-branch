"""Step 1: Broker selection functions.

Each function selects a set of broker IDs based on strategy-specific criteria.
All are pure functions -- no I/O, no side effects.

Signature: (SymbolData, GlobalContext, params: dict) -> list[str]
"""

import math
from datetime import date

import numpy as np
import polars as pl

from broker_analytics.domain.hypothesis.types import SymbolData, GlobalContext, BrokerList
from broker_analytics.domain.large_trade import flag_large_trades
from broker_analytics.domain.timing_alpha import compute_timing_alpha


def select_top_k_by_pnl(
    data: SymbolData, ctx: GlobalContext, params: dict,
) -> BrokerList:
    """Select top-K brokers by per-stock PNL ranking.

    Used by strategies 3, 4, 7.
    params: top_k (int, default 20)
    """
    top_k = params.get("top_k", 20)
    df = data.pnl_df.sort("total_pnl", descending=True).head(top_k)
    return df["broker"].cast(pl.Utf8).to_list()


def select_niche_top_brokers(
    data: SymbolData, ctx: GlobalContext, params: dict,
) -> BrokerList:
    """Strategy 1: Small players with high per-stock PNL.

    Excludes "big players" (top by trading amount in this stock),
    then selects top-K among remaining brokers by rolling N-year PNL.

    Rationale: if a normally unimportant broker ranks high in PNL for
    a specific stock, that contrast is a stronger signal than PNL alone.

    params: exclude_top_pct (float, default 0.1) — fraction of brokers to exclude,
            top_k (int, default 10),
            years (int, default 3),
            train_end_date (str, default "2023-12-31")
    """
    exclude_top_pct = params.get("exclude_top_pct", 0.1)
    top_k = params.get("top_k", 10)
    years = params.get("years", 3)
    train_end_str = params.get("train_end_date", "2023-12-31")
    train_end = date.fromisoformat(train_end_str)

    # 1. Training window trades only
    train_trades = data.trade_df.filter(pl.col("date") <= train_end)
    if len(train_trades) == 0:
        return []

    # 2. Per-broker total trading amount in this stock (training window)
    broker_amounts = (
        train_trades
        .with_columns(pl.col("broker").cast(pl.Utf8))
        .with_columns(
            (pl.col("buy_amount").fill_null(0) + pl.col("sell_amount").fill_null(0))
            .alias("total_amount")
        )
        .group_by("broker")
        .agg(pl.col("total_amount").sum())
        .sort("total_amount", descending=True)
    )

    # 3. Exclude top N% by amount (the "big players" / 大戶)
    n_exclude = max(1, int(len(broker_amounts) * exclude_top_pct))
    big_players = set(broker_amounts.head(n_exclude)["broker"].to_list())

    # 4. Among remaining "small players", rank by rolling N-year PNL
    local_ranking = _rolling_ranking_to_date(data.pnl_daily_df, years, train_end)
    if len(local_ranking) == 0:
        return []

    niche_ranking = local_ranking.filter(~pl.col("broker").is_in(big_players))
    if len(niche_ranking) == 0:
        return []

    return niche_ranking.head(top_k)["broker"].to_list()


def select_dual_window_intersection(
    data: SymbolData, ctx: GlobalContext, params: dict,
) -> BrokerList:
    """Strategy 2: Brokers in top-K for BOTH short and long rolling windows.

    Uses pnl_daily rolling aggregation (no look-ahead).
    params: top_k (int, default 20),
            short_years (int, default 1), long_years (int, default 3)
    """
    top_k = params.get("top_k", 20)
    short_years = params.get("short_years", 1)
    long_years = params.get("long_years", 3)

    short_set = _rolling_top_k(data.pnl_daily_df, short_years, top_k)
    long_set = _rolling_top_k(data.pnl_daily_df, long_years, top_k)
    return list(short_set & long_set)


def select_top_and_bottom_k(
    data: SymbolData, ctx: GlobalContext, params: dict,
) -> BrokerList:
    """Strategy 9: Select both top-K and bottom-K brokers.

    Returns combined list: first top_k are "top", rest are "bottom".
    The filter step distinguishes groups using this ordering.
    params: top_k (int, default 20)
    """
    top_k = params.get("top_k", 20)
    df = data.pnl_df.sort("total_pnl", descending=True)
    brokers = df["broker"].cast(pl.Utf8).to_list()
    top = brokers[:top_k]
    bottom = brokers[-top_k:] if len(brokers) >= top_k else brokers
    return top + bottom


def select_ta_regime_change(
    data: SymbolData, ctx: GlobalContext, params: dict,
) -> BrokerList:
    """Strategy 6: Brokers whose recent timing alpha breaks out vs own history.

    Temporal z-score: each broker's latest-window TA compared to its own
    historical TA distribution across overlapping windows.
    params: window_days (int, default 120), z_threshold (float, default 2.0),
            min_windows (int, default 4) — minimum historical windows needed
    """
    z_threshold = params.get("z_threshold", 2.0)
    window_days = params.get("window_days", 120)
    min_windows = params.get("min_windows", 4)
    step_days = window_days // 2

    trade_df = data.trade_df.with_columns(pl.col("broker").cast(pl.Utf8))
    brokers = trade_df["broker"].unique().to_list()
    prices_dict = _build_price_dict(data.prices, data.symbol)

    if not prices_dict:
        return []

    sorted_dates = sorted(prices_dict.keys())
    if len(sorted_dates) < window_days + step_days:
        return []

    returns = {}
    for i in range(1, len(sorted_dates)):
        p0 = prices_dict[sorted_dates[i - 1]]
        p1 = prices_dict[sorted_dates[i]]
        if p0 > 0:
            returns[sorted_dates[i]] = (p1 - p0) / p0

    trade_lookup: dict[tuple, int] = {}
    for row in trade_df.iter_rows(named=True):
        trade_lookup[(row["broker"], row["date"])] = (
            (row.get("buy_shares") or 0) - (row.get("sell_shares") or 0)
        )

    # Build overlapping windows
    n = len(sorted_dates)
    windows = []
    start = 0
    while start + window_days <= n:
        windows.append(sorted_dates[start:start + window_days])
        start += step_days
    if len(windows) < min_windows + 1:
        return []

    # For each broker, compute TA per window, then temporal z-score
    selected = []
    for broker in brokers:
        ta_values = []
        for w_dates in windows:
            nb = [trade_lookup.get((broker, d), 0) for d in w_dates]
            ret = [returns.get(d, 0.0) for d in w_dates]
            ta_values.append(compute_timing_alpha(nb, ret))

        ta_latest = ta_values[-1]
        ta_history = ta_values[:-1]
        if len(ta_history) < min_windows:
            continue

        mean_h = sum(ta_history) / len(ta_history)
        var_h = sum((v - mean_h) ** 2 for v in ta_history) / len(ta_history)
        std_h = var_h ** 0.5
        if std_h == 0:
            continue

        z = (ta_latest - mean_h) / std_h
        if abs(z) > z_threshold:
            selected.append(broker)

    return selected


def select_all_active_brokers(
    data: SymbolData, ctx: GlobalContext, params: dict,
) -> BrokerList:
    """Strategy 8: Select all active brokers (for HHI computation).

    Returns brokers with sufficient trading days.
    params: min_active_days (int, default 60)
    """
    min_active = params.get("min_active_days", 60)
    broker_counts = (
        data.trade_df
        .with_columns(pl.col("broker").cast(pl.Utf8))
        .group_by("broker")
        .agg(pl.len().alias("n_days"))
        .filter(pl.col("n_days") >= min_active)
    )
    return broker_counts["broker"].to_list()


def select_concentrated_brokers(
    data: SymbolData, ctx: GlobalContext, params: dict,
) -> BrokerList:
    """Strategy 8: Select brokers with high portfolio concentration in this stock.

    Uses pre-computed _broker_concentrations (injected by runner).
    params: min_concentration (float, default 0.3) — min weight of this stock
            in broker's portfolio
    """
    concentrations = params.get("_broker_concentrations")
    min_concentration = params.get("min_concentration", 0.3)

    if concentrations is None or len(concentrations) == 0:
        return []

    # Filter to brokers concentrated in this stock AND active in this stock's trades
    active_brokers = set(
        data.trade_df.with_columns(pl.col("broker").cast(pl.Utf8))
        ["broker"].unique().to_list()
    )

    concentrated = (
        concentrations
        .filter(pl.col("concentration_ratio") > min_concentration)
        ["broker"].to_list()
    )

    return [b for b in concentrated if b in active_brokers]


def select_by_large_trade_scar(
    data: SymbolData, ctx: GlobalContext, params: dict,
) -> BrokerList:
    """Strategy 0: Select brokers by large trade SCAR on training window.

    Computes per-broker direction-adjusted SCAR across multiple horizons
    in a training window, then returns top-K brokers by mean SCAR.

    PNL-independent -- selection is purely based on large trade forward returns.

    params: train_end_date (str, default "2023-12-31"),
            sigma (float, default 2.0), top_k (int, default 20),
            min_events (int, default 5),
            min_amount (int, default 10_000_000 TWD),
            horizons (tuple[int,...], default (5, 10, 20, 60))
    """
    train_end_str = params.get("train_end_date", "2023-12-31")
    train_end = date.fromisoformat(train_end_str)
    sigma = params.get("sigma", 2.0)
    top_k = params.get("top_k", 20)
    min_events = params.get("min_events", 5)
    min_amount = params.get("min_amount", 10_000_000)
    horizons = params.get("horizons", (5, 10, 20, 60))

    # ETF filter (Taiwan ETFs start with "00")
    if data.symbol.startswith("00"):
        return []

    # 1. Training window trades
    train_trades = data.trade_df.filter(pl.col("date") <= train_end)
    if len(train_trades) == 0:
        return []

    # 2. Large trade detection on training window
    large = flag_large_trades(train_trades, sigma)
    large = large.filter(pl.col("large_dir") != 0)
    if len(large) == 0:
        return []

    # 3. Amount filter: join back to get buy_amount/sell_amount
    amount_cols = train_trades.select(
        pl.col("broker").cast(pl.Utf8), "date", "buy_amount", "sell_amount",
    )
    large = large.join(amount_cols, on=["broker", "date"], how="left")
    large = large.filter(
        pl.when(pl.col("large_dir") > 0)
        .then(pl.col("buy_amount") >= min_amount)
        .otherwise(pl.col("sell_amount") >= min_amount)
    )
    if len(large) == 0:
        return []

    # 4. Price lookup for this symbol
    prices_dict = _build_price_dict(data.prices, data.symbol)
    if not prices_dict:
        return []
    sorted_dates = sorted(prices_dict.keys())
    closes = [prices_dict[d] for d in sorted_dates]
    date_to_idx = {d: i for i, d in enumerate(sorted_dates)}
    n_prices = len(closes)

    # 5. Training window drift & volatility
    train_closes = [closes[i] for i, d in enumerate(sorted_dates) if d <= train_end]
    if len(train_closes) < 30:
        return []
    tc = np.array(train_closes, dtype=np.float64)
    daily_rets = np.diff(tc) / tc[:-1]
    drift_per_day = float(np.mean(daily_rets))
    daily_std = float(np.std(daily_rets, ddof=1))
    if daily_std == 0:
        return []

    # 6. Per-broker SCAR scoring
    broker_scars: dict[str, list[float]] = {}
    for row in large.iter_rows(named=True):
        broker = row["broker"]
        idx = date_to_idx.get(row["date"])
        if idx is None:
            continue
        direction = row["large_dir"]
        entry_price = closes[idx]
        if entry_price == 0:
            continue

        for h in horizons:
            if idx + h >= n_prices:
                continue
            raw_ret = (closes[idx + h] - entry_price) / entry_price
            drift = drift_per_day * h
            vol_h = daily_std * math.sqrt(h)
            scar = (raw_ret - drift) / vol_h * direction
            broker_scars.setdefault(broker, []).append(scar)

    # Aggregate: mean SCAR per broker, filter by min_events
    broker_mean: dict[str, float] = {}
    for broker, scars in broker_scars.items():
        if len(scars) < min_events:
            continue
        broker_mean[broker] = float(np.mean(scars))

    if not broker_mean:
        return []

    # 7. Top-K by mean SCAR
    sorted_brokers = sorted(broker_mean, key=broker_mean.get, reverse=True)
    return sorted_brokers[:top_k]


# =============================================================================
# Helpers
# =============================================================================

def _rolling_top_k(
    pnl_daily: pl.DataFrame, years: int, k: int,
) -> set[str]:
    """Get top-K brokers by rolling PNL over trailing years."""
    max_date = pnl_daily["date"].max()
    if max_date is None:
        return set()

    try:
        start = max_date.replace(year=max_date.year - years)
    except ValueError:
        start = date(max_date.year - years, max_date.month, max_date.day - 1)

    window = pnl_daily.filter(pl.col("date") >= start)
    agg = (
        window.sort("date")
        .group_by("broker")
        .agg([
            pl.col("realized_pnl").sum(),
            pl.col("unrealized_pnl").last(),
        ])
        .with_columns(
            (pl.col("realized_pnl") + pl.col("unrealized_pnl")).alias("total_pnl")
        )
        .sort("total_pnl", descending=True)
        .head(k)
    )
    return set(agg["broker"].cast(pl.Utf8).to_list())


def _rolling_ranking_to_date(
    pnl_daily: pl.DataFrame, years: int, end_date: date,
) -> pl.DataFrame:
    """Compute broker ranking from pnl_daily within [end_date - years, end_date].

    Returns DataFrame[broker, total_pnl] sorted descending.
    """
    if len(pnl_daily) == 0:
        return pl.DataFrame(schema={"broker": pl.Utf8, "total_pnl": pl.Float64})

    try:
        start = end_date.replace(year=end_date.year - years)
    except ValueError:
        start = date(end_date.year - years, end_date.month, end_date.day - 1)

    window = pnl_daily.filter(
        (pl.col("date") >= start) & (pl.col("date") <= end_date)
    )
    if len(window) == 0:
        return pl.DataFrame(schema={"broker": pl.Utf8, "total_pnl": pl.Float64})

    return (
        window.sort("date")
        .group_by("broker")
        .agg([
            pl.col("realized_pnl").sum(),
            pl.col("unrealized_pnl").last(),
        ])
        .with_columns(
            pl.col("broker").cast(pl.Utf8),
            (pl.col("realized_pnl") + pl.col("unrealized_pnl")).alias("total_pnl"),
        )
        .sort("total_pnl", descending=True)
    )


def _build_price_dict(
    prices: pl.DataFrame, symbol: str,
) -> dict:
    """Extract {date: price} for a symbol from long-format prices DataFrame."""
    sym_prices = (
        prices
        .filter(pl.col("symbol_id") == symbol)
        .select("date", "close_price")
    )
    return {row["date"]: row["close_price"] for row in sym_prices.iter_rows(named=True)}
