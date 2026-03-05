"""Step 4: Baseline return functions.

Each function produces the comparison returns for hypothesis testing.
Reuses domain/forward_returns.sample_unconditional_returns -- no duplication.

Signature: (SymbolData, pl.DataFrame, params: dict) -> dict[int, np.ndarray]
"""

import numpy as np
import polars as pl

from broker_analytics.domain.forward_returns import (
    compute_forward_returns,
    sample_unconditional_returns,
)
from broker_analytics.domain.hypothesis.types import SymbolData, HorizonReturns


def baseline_unconditional(
    data: SymbolData, events: pl.DataFrame, params: dict,
) -> HorizonReturns:
    """Standard unconditional baseline: random-date forward returns.

    Used by strategies 1, 2, 3, 4, 5, 6, 7, 8.
    params: n_samples (int, default 10000), horizons, seed (int, default 42)
    """
    horizons = params.get("horizons", (1, 5, 10, 20))
    n_samples = params.get("n_samples", 10000)
    seed = params.get("seed", 42)
    symbol = params.get("baseline_symbol", data.symbol)

    return sample_unconditional_returns(
        data.prices, symbol, n_samples=n_samples, horizons=horizons, seed=seed,
    )


def baseline_cross_stock_unconditional(
    data: SymbolData, events: pl.DataFrame, params: dict,
) -> HorizonReturns:
    """Strategy 5: Unconditional returns of TARGET stock.

    params: target_symbol (str)
    """
    target = params["target_symbol"]
    horizons = params.get("horizons", (1, 5, 10, 20))
    n_samples = params.get("n_samples", 10000)
    seed = params.get("seed", 42)

    return sample_unconditional_returns(
        data.prices, target, n_samples=n_samples, horizons=horizons, seed=seed,
    )


def baseline_disagreement_returns(
    data: SymbolData, events: pl.DataFrame, params: dict,
) -> HorizonReturns:
    """Strategy 9: Forward returns on DISAGREEMENT days (dual-group baseline).

    Instead of unconditional returns, the baseline is forward returns on days
    when top-K and bottom-K trade in OPPOSITE directions.
    params: top_k (int), horizons, _brokers_list (injected by orchestrator)
    """
    top_k = params.get("top_k", 20)
    horizons = params.get("horizons", (1, 5, 10, 20))
    brokers = params.get("_brokers_list", [])

    if not brokers:
        return {h: np.array([], dtype=np.float64) for h in horizons}

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
        .group_by("date").agg(pl.col("net_buy").sum().alias("top_net"))
    )
    bottom_daily = (
        daily_net.filter(pl.col("broker").is_in(bottom_brokers))
        .group_by("date").agg(pl.col("net_buy").sum().alias("bottom_net"))
    )

    merged = top_daily.join(bottom_daily, on="date", how="inner")

    # Disagreement: opposite signs
    disagree = (
        merged
        .filter(
            ((pl.col("top_net") > 0) & (pl.col("bottom_net") < 0))
            | ((pl.col("top_net") < 0) & (pl.col("bottom_net") > 0))
        )
        .with_columns(
            pl.when(pl.col("top_net") > 0)
            .then(pl.lit(1, dtype=pl.Int8))
            .otherwise(pl.lit(-1, dtype=pl.Int8))
            .alias("direction"),
            pl.lit(1.0).alias("signal_value"),
        )
        .select("date", "direction", "signal_value")
    )

    if len(disagree) == 0:
        return {h: np.array([], dtype=np.float64) for h in horizons}

    ret_df = compute_forward_returns(disagree, data.prices, data.symbol, horizons)

    result: HorizonReturns = {}
    for h in horizons:
        col = f"ret_{h}d"
        if col in ret_df.columns:
            result[h] = ret_df[col].drop_nulls().to_numpy().astype(np.float64)
        else:
            result[h] = np.array([], dtype=np.float64)

    return result
