"""Forward Returns: Compute post-event returns and unconditional baseline.

Calculates forward returns (in bps) at multiple horizons for event dates,
samples random baseline returns for comparison, computes daily CAR curves,
and standardizes returns (SCAR) for cross-stock pooling.

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


# =============================================================================
# Daily CAR (Decay Curve)
# =============================================================================

def compute_daily_car(
    events: pl.DataFrame,
    prices: pl.DataFrame,
    symbol: str,
    max_horizon: int = 20,
) -> np.ndarray:
    """Compute mean Cumulative Abnormal Return day by day.

    For each event, compute daily return from T to T+d (d = 1..max_horizon).
    Then average across events. Used for decay curve visualization.

    Direction-aware: distribution events (-1) have their returns negated,
    so positive CAR means the signal is working in both directions.

    Args:
        events: Event DataFrame with columns: date, direction, signal_value.
        prices: Price DataFrame with columns: symbol_id, date, close_price.
        symbol: Stock symbol.
        max_horizon: Maximum forward day (inclusive).

    Returns:
        1-D numpy array of length max_horizon. car[d-1] = mean CAR at day d.
        NaN if no valid events for that day.
    """
    sym_prices = (
        prices
        .filter(pl.col("symbol_id") == symbol)
        .sort("date")
        .select("date", "close_price")
    )

    if len(sym_prices) == 0 or len(events) == 0:
        return np.full(max_horizon, np.nan)

    dates = sym_prices["date"].to_list()
    closes = sym_prices["close_price"].to_numpy()
    date_to_idx = {d: i for i, d in enumerate(dates)}
    n = len(dates)

    # Collect per-event daily returns (direction-adjusted)
    # Shape: (n_events, max_horizon)
    all_cars = []
    for row in events.iter_rows(named=True):
        idx = date_to_idx.get(row["date"])
        if idx is None:
            continue
        entry_price = closes[idx]
        if entry_price == 0:
            continue

        direction = row["direction"]
        daily = np.full(max_horizon, np.nan)
        for d in range(1, max_horizon + 1):
            if idx + d < n:
                ret_bps = (closes[idx + d] - entry_price) / entry_price * 10000
                daily[d - 1] = ret_bps * direction  # sign-adjust
        all_cars.append(daily)

    if not all_cars:
        return np.full(max_horizon, np.nan)

    car_matrix = np.array(all_cars)  # (n_events, max_horizon)
    return np.nanmean(car_matrix, axis=0)


# =============================================================================
# SCAR Standardization
# =============================================================================

def standardize_returns(
    event_returns: np.ndarray,
    prices: pl.DataFrame,
    symbol: str,
    horizon: int,
    estimation_window: int = 120,
) -> np.ndarray:
    """Standardize event returns by stock volatility → SCAR.

    SCAR_i = CAR_i / σ_stock, where σ_stock is the annualized daily
    return std estimated from a window BEFORE the event period.

    Args:
        event_returns: 1-D array of raw event returns (bps).
        prices: Price DataFrame with columns: symbol_id, date, close_price.
        symbol: Stock symbol.
        horizon: Horizon in days (used to scale volatility).
        estimation_window: Number of trailing days for σ estimation.

    Returns:
        1-D array of standardized returns (same length as input).
        If volatility cannot be estimated, returns the raw values.
    """
    sym_prices = (
        prices
        .filter(pl.col("symbol_id") == symbol)
        .sort("date")
        .select("close_price")
    )

    if len(sym_prices) < estimation_window + 1:
        return event_returns

    closes = sym_prices["close_price"].to_numpy()

    # Use the last estimation_window days before the end of data
    # for a single stock-level σ estimate
    daily_rets = np.diff(closes[-estimation_window - 1:]) / closes[-estimation_window - 1:-1]
    daily_std = float(np.std(daily_rets, ddof=1))

    if daily_std == 0:
        return event_returns

    # Scale to horizon: σ_h = σ_daily × sqrt(h), convert to bps
    horizon_std_bps = daily_std * np.sqrt(horizon) * 10000

    return event_returns / horizon_std_bps
