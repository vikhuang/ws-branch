"""Domain Layer: Core business logic and entities.

This layer contains:
- metrics/: Alpha and statistical calculations
- statistics: General-purpose hypothesis testing
- event_detection: Smart money event detection
- forward_returns: Post-event return computation
"""

from broker_analytics.domain.metrics import (
    # Timing Alpha
    TimingAlphaResult,
    calculate_timing_alpha,
    # Statistical
    PermutationTestResult,
    permutation_test,
)
from broker_analytics.domain.statistics import (
    DistributionSummary,
    DistributionShape,
    HypothesisTestResult,
    compare_distributions,
    describe_shape,
    permutation_test as stats_permutation_test,
)
from broker_analytics.domain.event_detection import (
    EventConfig,
    detect_smart_money_events,
)
from broker_analytics.domain.forward_returns import (
    compute_forward_returns,
    sample_unconditional_returns,
    compute_daily_car,
    standardize_returns,
)

__all__ = [
    # Metrics - Timing Alpha
    "TimingAlphaResult",
    "calculate_timing_alpha",
    # Metrics - Statistical
    "PermutationTestResult",
    "permutation_test",
    # Statistics
    "DistributionSummary",
    "DistributionShape",
    "HypothesisTestResult",
    "compare_distributions",
    "describe_shape",
    "stats_permutation_test",
    # Event Detection
    "EventConfig",
    "detect_smart_money_events",
    # Forward Returns
    "compute_forward_returns",
    "sample_unconditional_returns",
    "compute_daily_car",
    "standardize_returns",
]
