"""Unit tests for broker_analytics.domain pure functions."""

import numpy as np

from broker_analytics.domain.timing_alpha import compute_timing_alpha
from broker_analytics.domain.statistics import welch_t_test


class TestTimingAlpha:

    def test_positive_timing(self):
        """Buy before rise, sell before drop → positive alpha."""
        net_buys = [100, -100, 100, -100]
        returns = [0.0, 0.02, -0.02, 0.02]  # shifted: nb[t-1] predicts ret[t]
        alpha = compute_timing_alpha(net_buys, returns)
        assert alpha > 0

    def test_negative_timing(self):
        """Buy before drop, sell before rise → negative alpha."""
        net_buys = [100, -100, 100, -100]
        returns = [0.0, -0.02, 0.02, -0.02]
        alpha = compute_timing_alpha(net_buys, returns)
        assert alpha < 0

    def test_zero_with_constant_buys(self):
        """Constant net_buy → zero std → zero alpha."""
        net_buys = [100, 100, 100, 100]
        returns = [0.01, 0.02, -0.01, 0.03]
        alpha = compute_timing_alpha(net_buys, returns)
        assert alpha == 0.0

    def test_insufficient_data(self):
        """Less than 2 data points → zero."""
        assert compute_timing_alpha([100], [0.01]) == 0.0
        assert compute_timing_alpha([], []) == 0.0

    def test_normalized_by_volume(self):
        """Scaling net_buys by 10x should NOT change alpha (normalized)."""
        net_buys = [100, -50, 200, -100, 50]
        returns = [0.01, -0.02, 0.03, -0.01, 0.02]
        alpha_small = compute_timing_alpha(net_buys, returns)
        alpha_big = compute_timing_alpha([x * 10 for x in net_buys], returns)
        assert abs(alpha_small - alpha_big) < 1e-10


class TestWelchTTest:

    def test_identical_distributions(self):
        """Same data → t-stat near zero, p near 1."""
        data = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        t_stat, p_value = welch_t_test(data, data)
        assert abs(t_stat) < 1e-10
        assert p_value > 0.99

    def test_different_distributions(self):
        """Clearly different data → small p-value."""
        a = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        b = np.array([10.0, 11.0, 12.0, 13.0, 14.0])
        t_stat, p_value = welch_t_test(a, b)
        assert p_value < 0.01
