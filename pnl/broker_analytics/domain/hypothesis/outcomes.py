"""Step 3: Outcome measurement functions.

Each function computes forward returns for event dates.
Reuses domain/forward_returns.py -- no duplication.

Signature: (SymbolData, pl.DataFrame, params: dict) -> dict[int, np.ndarray]
"""

import numpy as np
import polars as pl

from broker_analytics.domain.forward_returns import compute_forward_returns
from broker_analytics.domain.hypothesis.types import SymbolData, HorizonReturns


def outcome_forward_returns(
    data: SymbolData, events: pl.DataFrame, params: dict,
) -> HorizonReturns:
    """Standard forward returns on the same symbol.

    Direction-adjusted: short signals get negated returns.
    Used by strategies 1, 2, 3, 4, 6, 7, 8, 9.
    params: horizons (tuple[int,...], default (1,5,10,20))
    """
    horizons = params.get("horizons", (1, 5, 10, 20))

    if len(events) == 0:
        return {h: np.array([], dtype=np.float64) for h in horizons}

    # Add signal_value column if missing (required by compute_forward_returns)
    if "signal_value" not in events.columns:
        events = events.with_columns(pl.lit(1.0).alias("signal_value"))

    ret_df = compute_forward_returns(events, data.prices, data.symbol, horizons)

    result: HorizonReturns = {}
    for h in horizons:
        col = f"ret_{h}d"
        if col in ret_df.columns:
            mask = pl.col(col).is_not_null()
            valid = ret_df.filter(mask)
            arr = valid[col].to_numpy().astype(np.float64)
            dirs = valid["direction"].to_numpy().astype(np.float64)
            result[h] = arr * dirs  # direction-adjust
        else:
            result[h] = np.array([], dtype=np.float64)

    return result


def outcome_cross_stock_returns(
    data: SymbolData, events: pl.DataFrame, params: dict,
) -> HorizonReturns:
    """Strategy 5: Forward returns of a DIFFERENT (target) stock.

    Events are detected on source stock; returns measured on target stock.
    params: target_symbol (str), horizons (tuple)
    """
    target_symbol = params["target_symbol"]
    horizons = params.get("horizons", (1, 5, 10, 20))

    if len(events) == 0:
        return {h: np.array([], dtype=np.float64) for h in horizons}

    if "signal_value" not in events.columns:
        events = events.with_columns(pl.lit(1.0).alias("signal_value"))

    ret_df = compute_forward_returns(events, data.prices, target_symbol, horizons)

    result: HorizonReturns = {}
    for h in horizons:
        col = f"ret_{h}d"
        if col in ret_df.columns:
            result[h] = ret_df[col].drop_nulls().to_numpy().astype(np.float64)
        else:
            result[h] = np.array([], dtype=np.float64)

    return result
