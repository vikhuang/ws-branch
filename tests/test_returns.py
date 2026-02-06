"""Unit tests for domain/returns.py.

Tests verify:
1. Daily returns calculation matches original implementation
2. Pearson correlation matches numpy.corrcoef
3. Alignment functions work correctly with lags
"""

import math
import pytest

import polars as pl
import numpy as np

from pnl_analytics.domain.returns import (
    CorrelationResult,
    calculate_daily_returns,
    calculate_return,
    pearson_correlation,
    correlation_coefficient,
    lead_lag_series,
)


# =============================================================================
# CorrelationResult Tests
# =============================================================================

class TestCorrelationResult:
    """Tests for CorrelationResult dataclass."""

    def test_is_significant_true(self):
        """Should be significant when p < 0.05."""
        result = CorrelationResult(r=0.5, p_value=0.01, n=100)
        assert result.is_significant is True

    def test_is_significant_false(self):
        """Should not be significant when p >= 0.05."""
        result = CorrelationResult(r=0.1, p_value=0.10, n=100)
        assert result.is_significant is False

    def test_strength_strong(self):
        """Strong correlation when |r| >= 0.7."""
        result = CorrelationResult(r=0.8, p_value=0.001, n=100)
        assert result.strength == "strong"

    def test_strength_moderate(self):
        """Moderate correlation when 0.4 <= |r| < 0.7."""
        result = CorrelationResult(r=-0.5, p_value=0.01, n=100)
        assert result.strength == "moderate"

    def test_strength_weak(self):
        """Weak correlation when 0.2 <= |r| < 0.4."""
        result = CorrelationResult(r=0.25, p_value=0.05, n=100)
        assert result.strength == "weak"

    def test_strength_negligible(self):
        """Negligible correlation when |r| < 0.2."""
        result = CorrelationResult(r=0.05, p_value=0.50, n=100)
        assert result.strength == "negligible"

    def test_frozen(self):
        """CorrelationResult should be immutable."""
        result = CorrelationResult(r=0.5, p_value=0.01, n=100)
        with pytest.raises(AttributeError):
            result.r = 0.6


# =============================================================================
# calculate_daily_returns Tests
# =============================================================================

class TestCalculateDailyReturns:
    """Tests for calculate_daily_returns function."""

    def test_basic_returns(self):
        """Basic return calculation."""
        df = pl.DataFrame({
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "close_price": [100.0, 102.0, 101.0]
        })
        returns = calculate_daily_returns(df)

        assert "2024-01-02" in returns
        assert "2024-01-03" in returns
        assert "2024-01-01" not in returns  # First date has no return

        assert returns["2024-01-02"] == pytest.approx(0.02)
        assert returns["2024-01-03"] == pytest.approx(-0.0098039, rel=1e-4)

    def test_unsorted_input(self):
        """Should handle unsorted input."""
        df = pl.DataFrame({
            "date": ["2024-01-03", "2024-01-01", "2024-01-02"],
            "close_price": [101.0, 100.0, 102.0]
        })
        returns = calculate_daily_returns(df)

        assert returns["2024-01-02"] == pytest.approx(0.02)

    def test_missing_columns_raises(self):
        """Should raise ValueError for missing columns."""
        df = pl.DataFrame({"date": ["2024-01-01"], "price": [100.0]})
        with pytest.raises(ValueError, match="Missing required columns"):
            calculate_daily_returns(df)

    def test_zero_price_skipped(self):
        """Should skip returns where previous price is zero."""
        df = pl.DataFrame({
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "close_price": [0.0, 100.0, 102.0]
        })
        returns = calculate_daily_returns(df)

        assert "2024-01-02" not in returns  # Previous price was 0
        assert "2024-01-03" in returns

    def test_matches_original_implementation(self):
        """Should match the original calculate_returns function."""
        # Use real price data from the repository
        from pnl_analytics.infrastructure import PriceRepository, DEFAULT_PATHS

        repo = PriceRepository(DEFAULT_PATHS)
        df = repo.get_all()

        new_returns = calculate_daily_returns(df)

        # Verify with manual calculation
        sorted_df = df.sort("date")
        dates = sorted_df["date"].to_list()
        prices = sorted_df["close_price"].to_list()

        for i in range(1, min(10, len(dates))):
            date = dates[i]
            expected = (prices[i] - prices[i-1]) / prices[i-1]
            assert new_returns[date] == pytest.approx(expected)


# =============================================================================
# calculate_return Tests
# =============================================================================

class TestCalculateReturn:
    """Tests for calculate_return function."""

    def test_positive_return(self):
        """Positive return when end > start."""
        assert calculate_return(100.0, 110.0) == pytest.approx(0.10)

    def test_negative_return(self):
        """Negative return when end < start."""
        assert calculate_return(100.0, 90.0) == pytest.approx(-0.10)

    def test_zero_return(self):
        """Zero return when end == start."""
        assert calculate_return(100.0, 100.0) == 0.0

    def test_zero_start_raises(self):
        """Should raise ValueError for zero start price."""
        with pytest.raises(ValueError, match="must be positive"):
            calculate_return(0.0, 100.0)

    def test_negative_start_raises(self):
        """Should raise ValueError for negative start price."""
        with pytest.raises(ValueError, match="must be positive"):
            calculate_return(-100.0, 100.0)


# =============================================================================
# pearson_correlation Tests
# =============================================================================

class TestPearsonCorrelation:
    """Tests for pearson_correlation function."""

    def test_perfect_positive(self):
        """Perfect positive correlation."""
        x = [1, 2, 3, 4, 5] * 3  # Need >= 10 samples
        y = [2, 4, 6, 8, 10] * 3
        result = pearson_correlation(x, y)

        assert result is not None
        assert result.r == pytest.approx(1.0)
        assert result.p_value < 0.001

    def test_perfect_negative(self):
        """Perfect negative correlation."""
        x = [1, 2, 3, 4, 5] * 3
        y = [10, 8, 6, 4, 2] * 3
        result = pearson_correlation(x, y)

        assert result is not None
        assert result.r == pytest.approx(-1.0)

    def test_no_correlation(self):
        """No correlation (random-ish data)."""
        x = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        y = [5, 3, 8, 2, 9, 1, 7, 4, 6, 10]
        result = pearson_correlation(x, y)

        assert result is not None
        assert abs(result.r) < 0.5  # Weak correlation

    def test_too_few_samples_returns_none(self):
        """Should return None for too few samples."""
        x = [1, 2, 3]
        y = [4, 5, 6]
        result = pearson_correlation(x, y, min_samples=10)
        assert result is None

    def test_custom_min_samples(self):
        """Should respect custom min_samples."""
        x = [1, 2, 3, 4, 5]
        y = [2, 4, 6, 8, 10]
        result = pearson_correlation(x, y, min_samples=5)
        assert result is not None

    def test_zero_variance_returns_none(self):
        """Should return None for zero variance."""
        x = [5, 5, 5, 5, 5] * 3
        y = [1, 2, 3, 4, 5] * 3
        result = pearson_correlation(x, y)
        assert result is None

    def test_different_lengths_raises(self):
        """Should raise ValueError for different lengths."""
        with pytest.raises(ValueError, match="same length"):
            pearson_correlation([1, 2, 3], [1, 2])

    def test_matches_numpy(self):
        """Should match numpy.corrcoef."""
        np.random.seed(42)
        x = np.random.randn(100).tolist()
        y = np.random.randn(100).tolist()

        result = pearson_correlation(x, y)
        numpy_r = np.corrcoef(x, y)[0, 1]

        assert result is not None
        assert result.r == pytest.approx(numpy_r, abs=1e-10)


# =============================================================================
# correlation_coefficient Tests
# =============================================================================

class TestCorrelationCoefficient:
    """Tests for correlation_coefficient function."""

    def test_returns_only_r(self):
        """Should return only the correlation coefficient."""
        x = [1, 2, 3, 4, 5] * 3
        y = [2, 4, 6, 8, 10] * 3
        r = correlation_coefficient(x, y)
        assert isinstance(r, float)
        assert r == pytest.approx(1.0)

    def test_returns_none_for_insufficient_data(self):
        """Should return None for insufficient data."""
        x = [1, 2, 3]
        y = [4, 5, 6]
        r = correlation_coefficient(x, y)
        assert r is None


# =============================================================================
# lead_lag_series Tests
# =============================================================================

class TestLeadLagSeries:
    """Tests for lead_lag_series function."""

    def test_basic_lead(self):
        """Basic lead series alignment."""
        signal = {"01-01": 100, "01-02": -50, "01-03": 75}
        response = {"01-02": 0.01, "01-03": -0.02}
        dates = ["01-01", "01-02", "01-03"]

        x, y = lead_lag_series(signal, response, dates)

        assert x == [100, -50]  # signal from t-1
        assert y == [0.01, -0.02]  # response from t

    def test_missing_dates(self):
        """Should skip dates with missing data."""
        signal = {"01-01": 100, "01-03": 75}  # Missing 01-02
        response = {"01-02": 0.01, "01-03": -0.02}
        dates = ["01-01", "01-02", "01-03"]

        x, y = lead_lag_series(signal, response, dates)

        # For i=1 (01-02): signal[01-01]=100 exists, response[01-02]=0.01 exists → pair
        # For i=2 (01-03): signal[01-02] missing → skip
        assert x == [100]
        assert y == [0.01]

    def test_with_real_data(self):
        """Test with actual repository data."""
        from pnl_analytics.infrastructure import (
            PriceRepository,
            TradeRepository,
            IndexMapRepository,
            DEFAULT_PATHS,
        )

        price_repo = PriceRepository(DEFAULT_PATHS)
        trade_repo = TradeRepository(DEFAULT_PATHS)
        index_repo = IndexMapRepository(DEFAULT_PATHS)

        returns = calculate_daily_returns(price_repo.get_all())
        dates = sorted(returns.keys())

        # Get net buys for Merrill (1440)
        broker_trades = trade_repo.get_by_broker("1440")
        net_buys = {}
        for row in broker_trades.iter_rows(named=True):
            date = row["date"]
            net = row["buy_shares"] - row["sell_shares"]
            net_buys[date] = net

        x, y = lead_lag_series(net_buys, returns, dates)

        # Should have matched pairs
        assert len(x) > 0
        assert len(x) == len(y)


# =============================================================================
# Integration Tests
# =============================================================================

class TestIntegration:
    """Integration tests using real data."""

    def test_lead_correlation_calculation(self):
        """Calculate lead correlation like the original code."""
        from pnl_analytics.infrastructure import (
            PriceRepository,
            TradeRepository,
            DEFAULT_PATHS,
        )

        price_repo = PriceRepository(DEFAULT_PATHS)
        trade_repo = TradeRepository(DEFAULT_PATHS)

        # Calculate returns
        returns = calculate_daily_returns(price_repo.get_all())
        dates = sorted(returns.keys())

        # Get Merrill net buys
        broker_trades = trade_repo.get_by_broker("1440")
        net_buys = {}
        for row in broker_trades.iter_rows(named=True):
            net_buys[row["date"]] = row["buy_shares"] - row["sell_shares"]

        # Prepare lead series
        x, y = lead_lag_series(net_buys, returns, dates)

        # Calculate correlation
        result = pearson_correlation(x, y)

        # Should get a valid result
        assert result is not None
        assert -1 <= result.r <= 1
        assert 0 <= result.p_value <= 1
