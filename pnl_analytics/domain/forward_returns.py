"""Forward Returns: Compute post-event returns and unconditional baseline.

Calculates forward returns (in bps) at multiple horizons for event dates,
and samples random baseline returns for comparison.

Pure functions — input/output are polars DataFrames and numpy arrays.
"""

import numpy as np
import polars as pl


def compute_forward_returns(
    events: pl.DataFrame,
    prices: pl.DataFrame,
    symbol: str,
    horizons: tuple[int, ...] = (1, 5, 10, 20),
) -> pl.DataFrame:
    """Compute forward returns at multiple horizons for event dates.

    Returns are in basis points (bps): (close[T+h] - close[T]) / close[T] × 10000.
    Uses bar-count shift (trading days), not calendar days.

    Args:
        events: Event DataFrame with columns: date, direction, signal_value.
        prices: Price DataFrame with columns: symbol_id, date, close_price.
        symbol: Stock symbol to filter prices.
        horizons: Forward horizons in trading days.

    Returns:
        DataFrame with columns: date, direction, signal_value,
        ret_{h}d for each horizon (Float64, bps). Null if T+h exceeds data.
    """
    # Filter prices for this symbol, sorted by date
    sym_prices = (
        prices
        .filter(pl.col("symbol_id") == symbol)
        .sort("date")
        .select("date", "close_price")
    )

    if len(sym_prices) == 0 or len(events) == 0:
        cols = {"date": pl.Date, "direction": pl.Int8, "signal_value": pl.Float64}
        for h in horizons:
            cols[f"ret_{h}d"] = pl.Float64
        return pl.DataFrame(schema=cols)

    # Build date → index lookup
    dates = sym_prices["date"].to_list()
    closes = sym_prices["close_price"].to_numpy()
    date_to_idx = {d: i for i, d in enumerate(dates)}
    n = len(dates)

    # Compute returns for each event
    rows = []
    for row in events.iter_rows(named=True):
        event_date = row["date"]
        idx = date_to_idx.get(event_date)
        if idx is None:
            continue

        entry_price = closes[idx]
        if entry_price == 0:
            continue

        ret_row = {
            "date": event_date,
            "direction": row["direction"],
            "signal_value": row["signal_value"],
        }

        for h in horizons:
            future_idx = idx + h
            if future_idx < n:
                ret = (closes[future_idx] - entry_price) / entry_price * 10000
                ret_row[f"ret_{h}d"] = float(ret)
            else:
                ret_row[f"ret_{h}d"] = None

        rows.append(ret_row)

    if not rows:
        cols = {"date": pl.Date, "direction": pl.Int8, "signal_value": pl.Float64}
        for h in horizons:
            cols[f"ret_{h}d"] = pl.Float64
        return pl.DataFrame(schema=cols)

    return pl.DataFrame(rows)


def sample_unconditional_returns(
    prices: pl.DataFrame,
    symbol: str,
    n_samples: int = 10000,
    horizons: tuple[int, ...] = (1, 5, 10, 20),
    seed: int = 42,
) -> dict[int, np.ndarray]:
    """Sample random baseline forward returns.

    Randomly picks n_samples trading dates and computes forward returns.
    Used as the unconditional baseline for hypothesis testing.

    Args:
        prices: Price DataFrame with columns: symbol_id, date, close_price.
        symbol: Stock symbol.
        n_samples: Number of random samples.
        horizons: Forward horizons in trading days.
        seed: Random seed for reproducibility.

    Returns:
        Dict mapping horizon → 1-D numpy array of returns (bps).
        NaN-free (dates without sufficient forward data are skipped).
    """
    sym_prices = (
        prices
        .filter(pl.col("symbol_id") == symbol)
        .sort("date")
        .select("close_price")
    )

    if len(sym_prices) == 0:
        return {h: np.array([], dtype=np.float64) for h in horizons}

    closes = sym_prices["close_price"].to_numpy()
    n = len(closes)
    max_horizon = max(horizons)

    # Valid indices: can compute all horizons
    valid_end = n - max_horizon
    if valid_end <= 0:
        return {h: np.array([], dtype=np.float64) for h in horizons}

    rng = np.random.default_rng(seed)
    sample_indices = rng.choice(valid_end, size=min(n_samples, valid_end), replace=False)

    result = {}
    for h in horizons:
        rets = (closes[sample_indices + h] - closes[sample_indices]) / closes[sample_indices] * 10000
        result[h] = rets.astype(np.float64)

    return result
