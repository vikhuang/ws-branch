"""Returns and correlation calculations.

Provides pure functions for financial calculations:
- Daily returns from price data
- Pearson correlation with statistical significance

All functions are stateless and depend only on their inputs.
"""

from dataclasses import dataclass
import math
from typing import Sequence

import polars as pl


# =============================================================================
# Data Classes
# =============================================================================

@dataclass(frozen=True, slots=True)
class CorrelationResult:
    """Result of a correlation calculation.

    Attributes:
        r: Pearson correlation coefficient (-1 to 1)
        p_value: Two-tailed p-value for H0: r=0
        n: Sample size used in calculation
    """
    r: float
    p_value: float
    n: int

    @property
    def is_significant(self) -> bool:
        """Check if correlation is significant at 5% level."""
        return self.p_value < 0.05

    @property
    def strength(self) -> str:
        """Interpret correlation strength.

        Returns:
            "strong", "moderate", "weak", or "negligible"
        """
        abs_r = abs(self.r)
        if abs_r >= 0.7:
            return "strong"
        elif abs_r >= 0.4:
            return "moderate"
        elif abs_r >= 0.2:
            return "weak"
        return "negligible"


# =============================================================================
# Daily Returns
# =============================================================================

def calculate_daily_returns(prices: pl.DataFrame) -> dict[str, float]:
    """Calculate daily returns from price data.

    Args:
        prices: DataFrame with 'date' and 'close_price' columns,
                need not be sorted

    Returns:
        Dict mapping date string to return (as decimal, e.g., 0.02 = 2%)
        First date has no return (needs previous price)

    Raises:
        ValueError: If required columns are missing

    Example:
        >>> df = pl.DataFrame({"date": ["2024-01-01", "2024-01-02"],
        ...                    "close_price": [100.0, 102.0]})
        >>> returns = calculate_daily_returns(df)
        >>> returns["2024-01-02"]
        0.02
    """
    required_cols = {"date", "close_price"}
    if not required_cols.issubset(set(prices.columns)):
        missing = required_cols - set(prices.columns)
        raise ValueError(f"Missing required columns: {missing}")

    sorted_prices = prices.sort("date")
    dates = sorted_prices["date"].to_list()
    closes = sorted_prices["close_price"].to_list()

    returns = {}
    for i in range(1, len(dates)):
        prev_close = closes[i - 1]
        if prev_close > 0:
            returns[dates[i]] = (closes[i] - prev_close) / prev_close

    return returns


def calculate_return(start_price: float, end_price: float) -> float:
    """Calculate simple return between two prices.

    Args:
        start_price: Starting price (must be > 0)
        end_price: Ending price

    Returns:
        Return as decimal (e.g., 0.02 = 2%)

    Raises:
        ValueError: If start_price <= 0
    """
    if start_price <= 0:
        raise ValueError(f"start_price must be positive, got {start_price}")
    return (end_price - start_price) / start_price


# =============================================================================
# Correlation
# =============================================================================

def pearson_correlation(
    x: Sequence[float],
    y: Sequence[float],
    min_samples: int = 10
) -> CorrelationResult | None:
    """Calculate Pearson correlation coefficient with p-value.

    Uses the t-test approximation for testing H0: r = 0.

    Args:
        x: First sequence of values
        y: Second sequence of values (same length as x)
        min_samples: Minimum sample size required (default 10)

    Returns:
        CorrelationResult with r, p_value, and n, or None if:
        - Sample size < min_samples
        - Either sequence has zero variance

    Raises:
        ValueError: If x and y have different lengths

    Example:
        >>> result = pearson_correlation([1, 2, 3, 4, 5] * 3, [2, 4, 6, 8, 10] * 3)
        >>> result.r
        1.0
        >>> result.is_significant
        True
    """
    if len(x) != len(y):
        raise ValueError(f"x and y must have same length: {len(x)} != {len(y)}")

    n = len(x)
    if n < min_samples:
        return None

    # Calculate means
    mean_x = sum(x) / n
    mean_y = sum(y) / n

    # Calculate correlation
    numerator = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
    var_x = sum((xi - mean_x) ** 2 for xi in x)
    var_y = sum((yi - mean_y) ** 2 for yi in y)

    denom = math.sqrt(var_x * var_y)
    if denom == 0:
        return None

    r = numerator / denom

    # Calculate p-value using t-distribution approximation
    if abs(r) >= 1.0:
        # Perfect correlation
        p_value = 0.0
    else:
        # t-statistic for testing H0: r = 0
        t_stat = r * math.sqrt(n - 2) / math.sqrt(1 - r ** 2)
        # Approximate p-value using normal distribution (valid for large n)
        # Two-tailed test
        p_value = 2 * (1 - _normal_cdf(abs(t_stat)))

    return CorrelationResult(r=r, p_value=p_value, n=n)


def correlation_coefficient(
    x: Sequence[float],
    y: Sequence[float],
    min_samples: int = 10
) -> float | None:
    """Calculate Pearson correlation coefficient only.

    Simpler version of pearson_correlation() that returns only r.
    Use this when p-value is not needed.

    Args:
        x: First sequence of values
        y: Second sequence of values
        min_samples: Minimum sample size (default 10)

    Returns:
        Correlation coefficient r, or None if calculation not possible
    """
    result = pearson_correlation(x, y, min_samples)
    return result.r if result else None


# =============================================================================
# Helper Functions
# =============================================================================

def _normal_cdf(x: float) -> float:
    """Standard normal cumulative distribution function.

    Uses the error function for computation.
    """
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def align_series(
    series1: dict[str, float],
    series2: dict[str, float],
    lag: int = 0
) -> tuple[list[float], list[float]]:
    """Align two time series by date keys with optional lag.

    Args:
        series1: First time series {date: value}
        series2: Second time series {date: value}
        lag: Lag offset. If positive, series1 is shifted forward in time.
             lag=1 means series1[t-1] is paired with series2[t]

    Returns:
        Two aligned lists (x, y) containing only matching dates

    Example:
        >>> s1 = {"2024-01-01": 1.0, "2024-01-02": 2.0, "2024-01-03": 3.0}
        >>> s2 = {"2024-01-02": 10.0, "2024-01-03": 20.0}
        >>> x, y = align_series(s1, s2, lag=1)
        >>> # x has s1 values from 01-01 and 01-02
        >>> # y has s2 values from 01-02 and 01-03
    """
    dates1 = sorted(series1.keys())
    dates2 = sorted(series2.keys())

    x_values = []
    y_values = []

    if lag == 0:
        # Simple intersection
        common_dates = set(dates1) & set(dates2)
        for date in sorted(common_dates):
            x_values.append(series1[date])
            y_values.append(series2[date])
    elif lag > 0:
        # series1 leads: pair series1[earlier] with series2[later]
        for i, date2 in enumerate(dates2):
            if i >= lag:
                date1 = dates1[dates1.index(date2) - lag] if date2 in dates1 else None
                # Find the date that is 'lag' positions before in dates1
                try:
                    idx2_in_dates1 = dates1.index(date2)
                    if idx2_in_dates1 >= lag:
                        date1 = dates1[idx2_in_dates1 - lag]
                        if date1 in series1 and date2 in series2:
                            x_values.append(series1[date1])
                            y_values.append(series2[date2])
                except ValueError:
                    continue
    else:
        # Negative lag: series2 leads
        return align_series(series2, series1, -lag)[::-1]

    return x_values, y_values


def lead_lag_series(
    signal: dict[str, float],
    response: dict[str, float],
    dates: list[str]
) -> tuple[list[float], list[float]]:
    """Prepare series for lead correlation analysis.

    Pairs signal[t-1] with response[t] for all valid dates.

    Args:
        signal: Signal series (e.g., net buys)
        response: Response series (e.g., returns)
        dates: Ordered list of all dates

    Returns:
        (signal_values, response_values) aligned for lead analysis

    Example:
        >>> signal = {"01-01": 100, "01-02": -50, "01-03": 75}
        >>> response = {"01-02": 0.01, "01-03": -0.02}
        >>> x, y = lead_lag_series(signal, response, ["01-01", "01-02", "01-03"])
        >>> # x = [100, -50]  (signal from t-1)
        >>> # y = [0.01, -0.02]  (response from t)
    """
    x_values = []
    y_values = []

    for i in range(1, len(dates)):
        prev_date = dates[i - 1]
        curr_date = dates[i]

        if prev_date in signal and curr_date in response:
            x_values.append(signal[prev_date])
            y_values.append(response[curr_date])

    return x_values, y_values
