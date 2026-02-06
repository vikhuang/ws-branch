"""PNL Analytics: High-speed broker PNL analysis system.

A modular system for analyzing broker trading performance,
including PNL calculation, alpha metrics, and statistical testing.

Architecture:
- domain/: Core business logic (models, calculations)
- infrastructure/: I/O and external dependencies
- application/: Use cases and services
- interfaces/: CLI and API endpoints
"""

__version__ = "0.12.0"

from pnl_analytics.domain import (
    Lot,
    ClosedTrade,
    BrokerSummary,
    TradeType,
)
from pnl_analytics.infrastructure import (
    DataPaths,
    AnalysisConfig,
    DEFAULT_PATHS,
    RepositoryError,
)

__all__ = [
    # Version
    "__version__",
    # Domain models
    "Lot",
    "ClosedTrade",
    "BrokerSummary",
    "TradeType",
    # Infrastructure
    "DataPaths",
    "AnalysisConfig",
    "DEFAULT_PATHS",
    "RepositoryError",
]
