"""Composable Hypothesis Testing Framework.

Five-step pipeline: Selector → Filter → Outcome → Baseline → StatTest.
Each step is a plain function connected by type contracts.

Usage:
    from broker_analytics.domain.hypothesis import HypothesisConfig, STRATEGIES
    from broker_analytics.domain.hypothesis.registry import get_strategy
"""

from broker_analytics.domain.hypothesis.types import (
    BrokerList,
    HorizonReturns,
    SymbolData,
    GlobalContext,
    HypothesisConfig,
    HypothesisResult,
    HorizonDetail,
    SelectorFn,
    FilterFn,
    OutcomeFn,
    BaselineFn,
    StatTestFn,
)

__all__ = [
    "BrokerList",
    "HorizonReturns",
    "SymbolData",
    "GlobalContext",
    "HypothesisConfig",
    "HypothesisResult",
    "HorizonDetail",
    "SelectorFn",
    "FilterFn",
    "OutcomeFn",
    "BaselineFn",
    "StatTestFn",
]
