"""Domain Layer: Core business logic and entities.

This layer contains:
- models.py: Data structures (Lot, ClosedTrade, BrokerSummary)
- returns.py: Return and correlation calculations
- metrics/: Alpha and statistical calculations
- fifo.py: FIFO accounting logic (to be added)
"""

from pnl_analytics.domain.models import (
    Lot,
    ClosedTrade,
    BrokerSummary,
    TradeType,
)
from pnl_analytics.domain.returns import (
    CorrelationResult,
    calculate_daily_returns,
    calculate_return,
    pearson_correlation,
    correlation_coefficient,
    align_series,
    lead_lag_series,
)
from pnl_analytics.domain.metrics import (
    # Execution Alpha
    TradeAlpha,
    BrokerExecutionAlpha,
    calculate_trade_alpha,
    calculate_broker_alpha,
    # Timing Alpha
    TimingAlphaResult,
    calculate_timing_alpha,
    # Statistical
    PermutationTestResult,
    permutation_test,
)

__all__ = [
    # Models
    "Lot",
    "ClosedTrade",
    "BrokerSummary",
    "TradeType",
    # Returns
    "CorrelationResult",
    "calculate_daily_returns",
    "calculate_return",
    "pearson_correlation",
    "correlation_coefficient",
    "align_series",
    "lead_lag_series",
    # Metrics - Execution Alpha
    "TradeAlpha",
    "BrokerExecutionAlpha",
    "calculate_trade_alpha",
    "calculate_broker_alpha",
    # Metrics - Timing Alpha
    "TimingAlphaResult",
    "calculate_timing_alpha",
    # Metrics - Statistical
    "PermutationTestResult",
    "permutation_test",
]
