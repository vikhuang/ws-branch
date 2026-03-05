"""Step 1: Broker selection functions.

Each function selects a set of broker IDs based on strategy-specific criteria.
All are pure functions -- no I/O, no side effects.

Signature: (SymbolData, GlobalContext, params: dict) -> list[str]
"""

import polars as pl

from broker_analytics.domain.hypothesis.types import SymbolData, GlobalContext, BrokerList
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


def select_contrarian_brokers(
    data: SymbolData, ctx: GlobalContext, params: dict,
) -> BrokerList:
    """Strategy 1: Global PNL bottom 20% BUT per-stock PNL top 20%.

    These brokers are globally "bad" but good at THIS specific stock.
    params: global_pct (float, default 0.2), local_pct (float, default 0.2)
    """
    global_pct = params.get("global_pct", 0.2)
    local_pct = params.get("local_pct", 0.2)

    # Global bottom
    global_df = ctx.global_ranking.sort("total_pnl")
    n_global = max(1, int(len(global_df) * global_pct))
    global_bottom = set(global_df.head(n_global)["broker"].cast(pl.Utf8).to_list())

    # Per-stock top
    local_df = data.pnl_df.sort("total_pnl", descending=True)
    n_local = max(1, int(len(local_df) * local_pct))
    local_top = set(local_df.head(n_local)["broker"].cast(pl.Utf8).to_list())

    return list(global_bottom & local_top)


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
    """Strategy 6: Brokers whose rolling timing alpha z-score > threshold.

    Computes timing alpha over trailing window, then z-score across brokers.
    params: window_days (int, default 120), z_threshold (float, default 2.0)
    """
    z_threshold = params.get("z_threshold", 2.0)
    window_days = params.get("window_days", 120)

    trade_df = data.trade_df.with_columns(pl.col("broker").cast(pl.Utf8))
    brokers = trade_df["broker"].unique().to_list()
    prices_dict = _build_price_dict(data.prices, data.symbol)

    if not prices_dict:
        return []

    sorted_dates = sorted(prices_dict.keys())
    returns = {}
    for i in range(1, len(sorted_dates)):
        p0 = prices_dict[sorted_dates[i - 1]]
        p1 = prices_dict[sorted_dates[i]]
        if p0 > 0:
            returns[sorted_dates[i]] = (p1 - p0) / p0

    recent_dates = sorted_dates[-window_days:]

    trade_lookup: dict[tuple, int] = {}
    for row in trade_df.iter_rows(named=True):
        trade_lookup[(row["broker"], row["date"])] = (
            (row.get("buy_shares") or 0) - (row.get("sell_shares") or 0)
        )

    alphas = {}
    for broker in brokers:
        nb = [trade_lookup.get((broker, d), 0) for d in recent_dates]
        ret = [returns.get(d, 0.0) for d in recent_dates]
        alphas[broker] = compute_timing_alpha(nb, ret)

    if not alphas:
        return []

    values = list(alphas.values())
    mean_a = sum(values) / len(values)
    var_a = sum((v - mean_a) ** 2 for v in values) / len(values)
    std_a = var_a ** 0.5
    if std_a == 0:
        return []

    return [b for b, a in alphas.items() if abs((a - mean_a) / std_a) > z_threshold]


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
        from datetime import date
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
