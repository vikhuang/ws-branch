"""Broker Analytics: Broker behavior analysis from trade-level data.

Analyzes broker trading patterns across multiple dimensions:
PNL performance, timing alpha, large-trade signals, and event studies.

Architecture:
- domain/: Pure business logic (metrics, statistics, detection)
- infrastructure/: I/O and external dependencies (config, repositories)
- application/: Use cases and services
- interfaces/: CLI entry points
"""

__version__ = "0.29.0"

from broker_analytics.infrastructure import (
    DataPaths,
    AnalysisConfig,
    DEFAULT_PATHS,
    RepositoryError,
)

__all__ = [
    "__version__",
    "DataPaths",
    "AnalysisConfig",
    "DEFAULT_PATHS",
    "RepositoryError",
]
