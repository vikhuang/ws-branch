"""Timing Alpha: Measure market timing ability.

Timing Alpha measures whether a broker's trading decisions
predict future returns.

Formula:
    timing_alpha = Σ((net_buy[t-1] - avg_net_buy) × return[t])

This is the demeaned net buy position times next-day return,
summed over all days.

Relationship to Lead Correlation:
    Lead = corr(net_buy[t-1], return[t])  # Direction consistency
    Timing Alpha ≈ Lead × std(net_buy) × n  # Cumulative contribution
    Correlation: ~+0.29 (provides independent information)

Interpretation:
- Positive timing alpha: Buying before price rises, selling before drops
- Negative timing alpha: Buying before price drops, selling before rises
- Zero timing alpha: No predictive ability

Note: Most "significant" timing alpha may be due to luck.
Permutation testing shows ~5% significant at p<0.05, as expected by chance.
"""

from dataclasses import dataclass
from typing import Sequence


# =============================================================================
# Data Classes
# =============================================================================

@dataclass(frozen=True, slots=True)
class TimingAlphaResult:
    """Result of timing alpha calculation.

    Attributes:
        timing_alpha: Raw timing alpha value (in share-return units)
        normalized_alpha: Alpha normalized by volatility
        avg_net_buy: Average daily net buy position
        n_days: Number of days in calculation
    """
    timing_alpha: float
    normalized_alpha: float
    avg_net_buy: float
    n_days: int

    @property
    def is_positive(self) -> bool:
        """Check if timing ability is positive."""
        return self.timing_alpha > 0


# =============================================================================
# Core Calculation
# =============================================================================

def calculate_timing_alpha(
    net_buys: Sequence[float | int],
    daily_returns: Sequence[float],
) -> float:
    """Calculate timing alpha from aligned time series.

    The timing alpha measures the cumulative contribution of
    predictive trading decisions.

    Formula:
        α = Σ((net_buy[t-1] - mean(net_buy)) × return[t])

    Args:
        net_buys: Daily net buy amounts (buy - sell)
                  Must be aligned with daily_returns by date
        daily_returns: Daily stock returns
                      Must be same length as net_buys

    Returns:
        Timing alpha value. Positive means predictive ability.

    Example:
        >>> net_buys = [100, -50, 200, -100, 50]  # Buy/sell pattern
        >>> returns = [0.01, -0.02, 0.03, -0.01, 0.02]  # Next-day returns
        >>> alpha = calculate_timing_alpha(net_buys, returns)
    """
    n = len(net_buys)
    if n < 2:
        return 0.0

    if len(daily_returns) != n:
        raise ValueError(
            f"net_buys and daily_returns must have same length: "
            f"{n} != {len(daily_returns)}"
        )

    # Calculate average net buy
    avg_net_buy = sum(net_buys) / n

    # Calculate timing alpha
    # net_buy[t-1] predicts return[t]
    timing_alpha = 0.0
    for i in range(1, n):
        demeaned_net_buy = net_buys[i - 1] - avg_net_buy
        timing_alpha += demeaned_net_buy * daily_returns[i]

    return timing_alpha


def calculate_timing_alpha_detailed(
    net_buys: Sequence[float | int],
    daily_returns: Sequence[float],
) -> TimingAlphaResult | None:
    """Calculate timing alpha with additional statistics.

    Args:
        net_buys: Daily net buy amounts
        daily_returns: Daily stock returns

    Returns:
        TimingAlphaResult with all metrics, or None if insufficient data
    """
    n = len(net_buys)
    if n < 2:
        return None

    if len(daily_returns) != n:
        raise ValueError(
            f"net_buys and daily_returns must have same length: "
            f"{n} != {len(daily_returns)}"
        )

    # Calculate average net buy
    avg_net_buy = sum(net_buys) / n

    # Calculate timing alpha and variance for normalization
    timing_alpha = 0.0
    sum_sq_demeaned = 0.0

    for i in range(1, n):
        demeaned = net_buys[i - 1] - avg_net_buy
        timing_alpha += demeaned * daily_returns[i]
        sum_sq_demeaned += demeaned ** 2

    # Normalize by standard deviation of net buys
    if sum_sq_demeaned > 0:
        std_net_buy = (sum_sq_demeaned / (n - 1)) ** 0.5
        normalized_alpha = timing_alpha / (std_net_buy * n) if std_net_buy > 0 else 0.0
    else:
        normalized_alpha = 0.0

    return TimingAlphaResult(
        timing_alpha=timing_alpha,
        normalized_alpha=normalized_alpha,
        avg_net_buy=avg_net_buy,
        n_days=n,
    )


# =============================================================================
# Series Preparation
# =============================================================================

def prepare_timing_series(
    trade_data: dict[str, tuple[int, int]],
    returns: dict[str, float],
    dates: list[str],
) -> tuple[list[int], list[float]]:
    """Prepare aligned series for timing alpha calculation.

    Args:
        trade_data: Dict mapping date to (buy_shares, sell_shares)
        returns: Dict mapping date to daily return
        dates: Ordered list of all trading dates

    Returns:
        Tuple of (net_buys, daily_returns) aligned lists

    Example:
        >>> trades = {"2024-01-02": (1000, 500), "2024-01-03": (200, 800)}
        >>> returns = {"2024-01-02": 0.01, "2024-01-03": -0.02}
        >>> dates = ["2024-01-01", "2024-01-02", "2024-01-03"]
        >>> net_buys, daily_rets = prepare_timing_series(trades, returns, dates)
    """
    # Only include dates that have return data
    valid_dates = [d for d in dates if d in returns]

    net_buys = []
    daily_returns = []

    for date in valid_dates:
        if date in trade_data:
            buy, sell = trade_data[date]
            net_buys.append(buy - sell)
        else:
            net_buys.append(0)
        daily_returns.append(returns[date])

    return net_buys, daily_returns


def calculate_daily_contribution(
    net_buys: Sequence[float | int],
    daily_returns: Sequence[float],
) -> list[float]:
    """Calculate the daily contribution to timing alpha.

    Useful for analyzing when timing ability was strongest.

    Args:
        net_buys: Daily net buy amounts
        daily_returns: Daily stock returns

    Returns:
        List of daily contributions to timing alpha
    """
    n = len(net_buys)
    if n < 2:
        return []

    avg_net_buy = sum(net_buys) / n

    contributions = []
    for i in range(1, n):
        demeaned = net_buys[i - 1] - avg_net_buy
        contributions.append(demeaned * daily_returns[i])

    return contributions
