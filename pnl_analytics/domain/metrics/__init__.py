"""Trading metrics for broker performance analysis.

This package provides metrics for evaluating broker trading performance:

- Timing Alpha: Ability to predict future price movements
- Statistical: Hypothesis testing for metric significance
"""

# Timing Alpha
from pnl_analytics.domain.metrics.timing_alpha import (
    TimingAlphaResult,
    calculate_timing_alpha,
    calculate_timing_alpha_detailed,
    prepare_timing_series,
    calculate_daily_contribution,
)

# Statistical
from pnl_analytics.domain.metrics.statistical import (
    PermutationTestResult,
    permutation_test,
    permutation_test_detailed,
    generic_permutation_test,
    interpret_significance,
    expected_false_positives,
)

__all__ = [
    # Timing Alpha
    "TimingAlphaResult",
    "calculate_timing_alpha",
    "calculate_timing_alpha_detailed",
    "prepare_timing_series",
    "calculate_daily_contribution",
    # Statistical
    "PermutationTestResult",
    "permutation_test",
    "permutation_test_detailed",
    "generic_permutation_test",
    "interpret_significance",
    "expected_false_positives",
]
