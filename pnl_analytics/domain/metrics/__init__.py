"""Trading metrics for broker performance analysis.

This package provides metrics for evaluating broker trading performance:

- Execution Alpha: Quality of trade execution vs market close
- Timing Alpha: Ability to predict future price movements
- Statistical: Hypothesis testing for metric significance

Usage:
    from pnl_analytics.domain.metrics import (
        calculate_trade_alpha,
        calculate_timing_alpha,
        permutation_test,
    )
"""

# Execution Alpha
from pnl_analytics.domain.metrics.execution_alpha import (
    TradeAlpha,
    BrokerExecutionAlpha,
    calculate_trade_alpha,
    add_alpha_columns,
    calculate_broker_alpha,
    calculate_all_broker_alphas,
)

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
    # Execution Alpha
    "TradeAlpha",
    "BrokerExecutionAlpha",
    "calculate_trade_alpha",
    "add_alpha_columns",
    "calculate_broker_alpha",
    "calculate_all_broker_alphas",
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
