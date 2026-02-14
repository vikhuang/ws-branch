"""Domain Layer: Core business logic and entities.

This layer contains:
- metrics/: Alpha and statistical calculations
"""

from pnl_analytics.domain.metrics import (
    # Timing Alpha
    TimingAlphaResult,
    calculate_timing_alpha,
    # Statistical
    PermutationTestResult,
    permutation_test,
)

__all__ = [
    # Metrics - Timing Alpha
    "TimingAlphaResult",
    "calculate_timing_alpha",
    # Metrics - Statistical
    "PermutationTestResult",
    "permutation_test",
]
