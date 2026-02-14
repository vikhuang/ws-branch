"""PNL Analytics: High-speed broker PNL analysis system.

A modular system for analyzing broker trading performance,
including PNL calculation, alpha metrics, and statistical testing.

Architecture:
- domain/: Core business logic (metrics, calculations)
- infrastructure/: I/O and external dependencies
- application/: Use cases and services
- interfaces/: CLI and API endpoints
"""

__version__ = "0.15.0"

from pnl_analytics.infrastructure import (
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
