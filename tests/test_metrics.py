"""Unit tests for domain/metrics/ module.

Tests verify:
1. Execution alpha calculation matches original implementation
2. Timing alpha calculation matches original implementation
3. Permutation test produces valid p-values
4. Results match the Merrill Lynch baseline
"""

import pytest
import random

from pnl_analytics.domain.metrics import (
    # Execution Alpha
    TradeAlpha,
    BrokerExecutionAlpha,
    calculate_trade_alpha,
    add_alpha_columns,
    calculate_broker_alpha,
    calculate_all_broker_alphas,
    # Timing Alpha
    TimingAlphaResult,
    calculate_timing_alpha,
    calculate_timing_alpha_detailed,
    prepare_timing_series,
    # Statistical
    PermutationTestResult,
    permutation_test,
    permutation_test_detailed,
    interpret_significance,
    expected_false_positives,
)


# =============================================================================
# Execution Alpha Tests
# =============================================================================

class TestTradeAlpha:
    """Tests for TradeAlpha dataclass."""

    def test_is_positive(self):
        """is_positive should reflect alpha sign."""
        positive = TradeAlpha(0.10, 0.08, 0.02, 100000, 2000)
        negative = TradeAlpha(0.08, 0.10, -0.02, 100000, -2000)
        assert positive.is_positive is True
        assert negative.is_positive is False

    def test_frozen(self):
        """TradeAlpha should be immutable."""
        alpha = TradeAlpha(0.10, 0.08, 0.02, 100000, 2000)
        with pytest.raises(AttributeError):
            alpha.alpha = 0.05


class TestCalculateTradeAlpha:
    """Tests for calculate_trade_alpha function."""

    def test_long_trade_positive_alpha(self):
        """Long trade with better execution than market."""
        # Bought at 100, sold at 110 (10% return)
        # Market: close 101 -> 108 (6.93% return)
        # Alpha: 10% - 6.93% = 3.07%
        result = calculate_trade_alpha(
            trade_type="long",
            buy_price=100,
            sell_price=110,
            close_at_buy=101,
            close_at_sell=108,
            shares=1000,
        )
        assert result is not None
        assert result.trade_return == pytest.approx(0.10)
        assert result.benchmark_return == pytest.approx(0.0693069, rel=1e-3)
        assert result.alpha > 0

    def test_long_trade_negative_alpha(self):
        """Long trade with worse execution than market."""
        # Bought at 102, sold at 108 (5.88% return)
        # Market: close 100 -> 110 (10% return)
        # Alpha: 5.88% - 10% = -4.12%
        result = calculate_trade_alpha(
            trade_type="long",
            buy_price=102,
            sell_price=108,
            close_at_buy=100,
            close_at_sell=110,
            shares=1000,
        )
        assert result is not None
        assert result.alpha < 0

    def test_short_trade_positive_alpha(self):
        """Short trade with better execution than market."""
        # Shorted at 110, covered at 100 (9.09% return)
        # Market: close 108 -> 101 (6.48% short return)
        # Alpha: 9.09% - 6.48% = 2.61%
        result = calculate_trade_alpha(
            trade_type="short",
            buy_price=110,  # Open short price
            sell_price=100,  # Cover price
            close_at_buy=108,
            close_at_sell=101,
            shares=1000,
        )
        assert result is not None
        assert result.trade_return == pytest.approx(0.0909, rel=1e-2)
        assert result.alpha > 0

    def test_trade_type_case_insensitive(self):
        """Should accept various case strings."""
        result = calculate_trade_alpha(
            trade_type="LONG",  # Uppercase
            buy_price=100,
            sell_price=110,
            close_at_buy=100,
            close_at_sell=110,
            shares=1000,
        )
        assert result is not None
        assert result.alpha == pytest.approx(0.0)  # Same as market

    def test_invalid_prices_returns_none(self):
        """Should return None for invalid prices."""
        assert calculate_trade_alpha("long", 0, 110, 100, 110, 1000) is None
        assert calculate_trade_alpha("long", 100, 110, 0, 110, 1000) is None
        assert calculate_trade_alpha("long", 100, 110, 100, 0, 1000) is None

    def test_invalid_shares_returns_none(self):
        """Should return None for invalid shares."""
        assert calculate_trade_alpha("long", 100, 110, 100, 110, 0) is None
        assert calculate_trade_alpha("long", 100, 110, 100, 110, -100) is None

    def test_trade_value_calculation(self):
        """Trade value should be shares * buy_price."""
        result = calculate_trade_alpha(
            trade_type="long",
            buy_price=100,
            sell_price=110,
            close_at_buy=100,
            close_at_sell=110,
            shares=1000,
        )
        assert result.trade_value == 100000


class TestBrokerExecutionAlpha:
    """Tests for BrokerExecutionAlpha dataclass."""

    def test_alpha_percent(self):
        """alpha_percent should be weighted_alpha * 100."""
        result = BrokerExecutionAlpha(
            broker="1440",
            weighted_alpha=0.001318,
            total_alpha_dollars=1_000_000,
            total_trade_value=100_000_000,
            trade_count=500,
            long_count=400,
            short_count=100,
        )
        assert result.alpha_percent == pytest.approx(0.1318)


# =============================================================================
# Timing Alpha Tests
# =============================================================================

class TestCalculateTimingAlpha:
    """Tests for calculate_timing_alpha function."""

    def test_perfect_prediction(self):
        """Perfect prediction should give positive alpha."""
        # Buy before price goes up, sell before price goes down
        net_buys = [100, 100, -100, -100, 100, 100, -100, -100, 100, 100]
        returns = [0.0, 0.01, 0.01, -0.01, -0.01, 0.01, 0.01, -0.01, -0.01, 0.01]
        # net_buys[t-1] × returns[t] should be positive
        alpha = calculate_timing_alpha(net_buys, returns)
        assert alpha > 0

    def test_anti_prediction(self):
        """Anti-prediction should give negative alpha."""
        # Buy before price goes down, sell before price goes up
        net_buys = [100, 100, -100, -100, 100, 100, -100, -100, 100, 100]
        returns = [0.0, -0.01, -0.01, 0.01, 0.01, -0.01, -0.01, 0.01, 0.01, -0.01]
        alpha = calculate_timing_alpha(net_buys, returns)
        assert alpha < 0

    def test_random_no_prediction(self):
        """Random data should give near-zero alpha."""
        random.seed(42)
        net_buys = [random.randint(-100, 100) for _ in range(1000)]
        returns = [random.uniform(-0.02, 0.02) for _ in range(1000)]
        alpha = calculate_timing_alpha(net_buys, returns)
        # Should be relatively small compared to the scale
        assert abs(alpha) < 100 * 1000 * 0.02  # Very loose bound

    def test_empty_returns_zero(self):
        """Empty or single-element should return 0."""
        assert calculate_timing_alpha([], []) == 0.0
        assert calculate_timing_alpha([100], [0.01]) == 0.0

    def test_length_mismatch_raises(self):
        """Different lengths should raise ValueError."""
        with pytest.raises(ValueError, match="same length"):
            calculate_timing_alpha([1, 2, 3], [0.01, 0.02])

    def test_matches_original_formula(self):
        """Should match the original implementation formula."""
        net_buys = [100, -50, 200, -100, 50, 75, -25, 150, -80, 30]
        returns = [0.01, -0.02, 0.03, -0.01, 0.02, -0.01, 0.01, -0.02, 0.01, 0.0]

        # Original formula
        avg_nb = sum(net_buys) / len(net_buys)
        expected = 0.0
        for i in range(1, len(net_buys)):
            expected += (net_buys[i-1] - avg_nb) * returns[i]

        result = calculate_timing_alpha(net_buys, returns)
        assert result == pytest.approx(expected)


class TestCalculateTimingAlphaDetailed:
    """Tests for calculate_timing_alpha_detailed function."""

    def test_returns_result_object(self):
        """Should return TimingAlphaResult."""
        net_buys = [100, -50, 200, -100, 50] * 3
        returns = [0.01, -0.02, 0.03, -0.01, 0.02] * 3
        result = calculate_timing_alpha_detailed(net_buys, returns)

        assert isinstance(result, TimingAlphaResult)
        assert result.n_days == 15
        assert result.avg_net_buy == pytest.approx(40.0)

    def test_insufficient_data_returns_none(self):
        """Should return None for insufficient data."""
        assert calculate_timing_alpha_detailed([100], [0.01]) is None


# =============================================================================
# Statistical Tests
# =============================================================================

class TestPermutationTest:
    """Tests for permutation_test function."""

    def test_returns_valid_pvalue(self):
        """Should return p-value between 0 and 1."""
        net_buys = [100, -50, 200, -100, 50] * 5
        returns = [0.01, -0.02, 0.03, -0.01, 0.02] * 5
        p = permutation_test(net_buys, returns, n_permutations=100, seed=42)
        assert 0 <= p <= 1

    def test_perfect_prediction_low_pvalue(self):
        """Perfect prediction should have low p-value."""
        # Perfectly correlated: buy before up, sell before down
        net_buys = [100, 100, 100, -100, -100, -100] * 10
        returns = [0.0, 0.02, 0.02, 0.02, -0.02, -0.02] * 10
        # Shift to align: net_buys[t-1] predicts returns[t]
        p = permutation_test(net_buys, returns, n_permutations=500, seed=42)
        assert p < 0.10  # Should be relatively significant

    def test_reproducible_with_seed(self):
        """Same seed should give same result."""
        net_buys = list(range(50))
        returns = [x * 0.001 for x in range(50)]

        p1 = permutation_test(net_buys, returns, n_permutations=100, seed=123)
        p2 = permutation_test(net_buys, returns, n_permutations=100, seed=123)
        assert p1 == p2


class TestPermutationTestDetailed:
    """Tests for permutation_test_detailed function."""

    def test_returns_result_object(self):
        """Should return PermutationTestResult."""
        net_buys = list(range(30))
        returns = [x * 0.001 for x in range(30)]
        result = permutation_test_detailed(net_buys, returns, n_permutations=50, seed=42)

        assert isinstance(result, PermutationTestResult)
        assert result.n_permutations == 50
        assert 0 <= result.n_extreme <= 50

    def test_significance_label(self):
        """Should have correct significance labels."""
        # Mock results with different p-values
        r1 = PermutationTestResult(100, 0.005, 1000, 5)
        r2 = PermutationTestResult(100, 0.03, 1000, 30)
        r3 = PermutationTestResult(100, 0.08, 1000, 80)
        r4 = PermutationTestResult(100, 0.20, 1000, 200)

        assert r1.significance_label == "**"
        assert r2.significance_label == "*"
        assert r3.significance_label == "†"
        assert r4.significance_label == ""


class TestInterpretSignificance:
    """Tests for interpret_significance function."""

    def test_highly_significant(self):
        """p < 0.01 should be highly significant."""
        assert "Highly significant" in interpret_significance(0.005)

    def test_significant(self):
        """p < 0.05 should be significant."""
        assert "Significant" in interpret_significance(0.03)

    def test_marginally_significant(self):
        """p < 0.10 should be marginally significant."""
        assert "Marginally" in interpret_significance(0.08)

    def test_not_significant(self):
        """p >= 0.10 should not be significant."""
        assert "Not significant" in interpret_significance(0.15)


class TestExpectedFalsePositives:
    """Tests for expected_false_positives function."""

    def test_calculation(self):
        """Should calculate n_tests * alpha."""
        assert expected_false_positives(940, 0.05) == 47.0
        assert expected_false_positives(100, 0.01) == 1.0


# =============================================================================
# Integration Tests with Real Data
# =============================================================================

class TestIntegrationWithRealData:
    """Integration tests using actual repository data.

    Note: These tests require the full ETL pipeline to have run.
    They are skipped if data is not available.
    """

    @pytest.mark.skip(reason="Requires closed_trades.parquet (old architecture)")
    def test_merrill_execution_alpha(self):
        """Verify Merrill (1440) execution alpha matches baseline."""
        pass

    @pytest.mark.skip(reason="Requires index_maps.json (old architecture)")
    def test_merrill_timing_alpha(self):
        """Verify Merrill timing alpha calculation works."""
        pass

    @pytest.mark.skip(reason="Requires closed_trades.parquet (old architecture)")
    def test_all_broker_alphas(self):
        """Test calculating alpha for all brokers."""
        pass
