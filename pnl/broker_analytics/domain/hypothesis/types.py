"""Type contracts for the composable hypothesis pipeline.

All types are frozen dataclasses with __slots__ per project convention.
Step functions use these as their interface contracts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import numpy as np
import polars as pl

from broker_analytics.domain.statistics import HypothesisTestResult


# =============================================================================
# Step Output Types
# =============================================================================

# Step 1 output: broker IDs
BrokerList = list[str]

# Step 2 output: DataFrame[date: Date, direction: Int8]
# direction: +1 = long signal, -1 = short signal
# Enforced by schema, not a custom type.

# Step 3/4 output: horizon → array of returns (bps)
HorizonReturns = dict[int, np.ndarray]


# =============================================================================
# Data Bundles (read-only context passed to step functions)
# =============================================================================

@dataclass(frozen=True, slots=True)
class SymbolData:
    """All data for a single symbol, loaded once and passed down the pipeline.

    Avoids repeated file I/O by pre-loading everything the steps need.
    Pure data -- no methods, no I/O.
    """

    symbol: str
    trade_df: pl.DataFrame          # daily_summary schema
    pnl_daily_df: pl.DataFrame      # pnl_daily schema
    pnl_df: pl.DataFrame            # pnl/{sym} per-stock ranking
    prices: pl.DataFrame            # close_prices (long: symbol_id, date, close_price)


@dataclass(frozen=True, slots=True)
class GlobalContext:
    """Cross-symbol data needed by certain strategies.

    Strategies 1 (contrarian), 5 (cross-stock), 8 (concentration)
    need data beyond a single symbol.
    """

    global_ranking: pl.DataFrame    # derived/broker_ranking
    all_symbols: list[str]          # available symbols
    prices: pl.DataFrame            # full close_prices (long format)


# =============================================================================
# Step Function Signatures
# =============================================================================

SelectorFn = Callable[[SymbolData, GlobalContext, dict], BrokerList]
FilterFn = Callable[[SymbolData, list[str], dict], pl.DataFrame]
OutcomeFn = Callable[[SymbolData, pl.DataFrame, dict], HorizonReturns]
BaselineFn = Callable[[SymbolData, pl.DataFrame, dict], HorizonReturns]
StatTestFn = Callable[[HorizonReturns, HorizonReturns, dict], dict[int, HypothesisTestResult]]


# =============================================================================
# Pipeline Configuration
# =============================================================================

@dataclass(frozen=True, slots=True)
class HypothesisConfig:
    """Configuration for a single hypothesis strategy.

    Points to concrete functions for each pipeline step.
    The params dict passes strategy-specific knobs (top_k, sigma, etc.)
    without changing the function signature.
    """

    name: str
    display_name: str               # Chinese display name
    description: str                # One-line English description
    selector: SelectorFn
    filter: FilterFn
    outcome: OutcomeFn
    baseline: BaselineFn
    stat_test: StatTestFn
    params: dict = field(default_factory=dict)
    horizons: tuple[int, ...] = (1, 5, 10, 20)
    requires: frozenset[str] = frozenset({"trade_df", "pnl_daily_df", "pnl_df", "prices"})


# =============================================================================
# Rolling Cross-Validation
# =============================================================================

@dataclass(frozen=True, slots=True)
class CVFold:
    """One fold in rolling cross-validation."""

    train_end_date: str      # e.g. "2023-06-30"
    test_start_date: str     # e.g. "2023-07-01"
    test_end_date: str       # e.g. "2024-06-30"
    label: str = ""          # display label


DEFAULT_FOLDS: tuple[CVFold, ...] = (
    CVFold("2023-06-30", "2023-07-01", "2024-06-30", "2023H2-2024H1"),
    CVFold("2023-12-31", "2024-01-01", "2024-12-31", "2024"),
    CVFold("2024-06-30", "2024-07-01", "2025-06-30", "2024H2-2025H1"),
    CVFold("2024-12-31", "2025-01-01", "2025-12-31", "2025"),
    CVFold("2025-06-30", "2025-07-01", "2026-03-31", "2025H2-2026Q1"),
)


# =============================================================================
# Result Types
# =============================================================================

@dataclass(frozen=True, slots=True)
class HorizonDetail:
    """Result for one horizon within a hypothesis test."""

    horizon: int
    n_events: int
    n_baseline: int
    cond_mean: float
    uncond_mean: float
    test_result: HypothesisTestResult


@dataclass(frozen=True, slots=True)
class HypothesisResult:
    """Complete result of running one hypothesis on one symbol."""

    strategy_name: str
    symbol: str
    n_brokers_selected: int
    n_events: int
    horizon_details: tuple[HorizonDetail, ...]
    conclusion: str                 # "significant" / "marginal" / "no_effect"
    params: dict
