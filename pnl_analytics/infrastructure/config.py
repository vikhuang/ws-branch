"""Configuration: Centralized paths and settings.

This module provides:
- DataPaths: File paths for all data sources
- AnalysisConfig: Parameters for analysis algorithms

Design Principles:
- Immutable configuration (frozen dataclass)
- Sensible defaults
- Easy to override for testing
"""

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DataPaths:
    """File paths for data sources.

    All paths are relative to the project root by default.
    Can be overridden for testing or different environments.

    Attributes:
        root: Project root directory
        trade_summary: Daily trade summary (ETL output)
        price_master: Price data (sync output)
        closed_trades: Closed trades (PNL engine output)
        realized_pnl: Realized PNL tensor
        unrealized_pnl: Unrealized PNL tensor
        index_maps: Dimension mappings
        broker_names: Broker name mappings (JSON)
        broker_master: Official broker data (XLS)
    """

    root: Path = Path(".")

    @property
    def trade_summary(self) -> Path:
        return self.root / "daily_trade_summary.parquet"

    @property
    def price_master(self) -> Path:
        return self.root / "price_master.parquet"

    @property
    def closed_trades(self) -> Path:
        return self.root / "closed_trades.parquet"

    @property
    def realized_pnl(self) -> Path:
        return self.root / "realized_pnl.npy"

    @property
    def unrealized_pnl(self) -> Path:
        return self.root / "unrealized_pnl.npy"

    @property
    def index_maps(self) -> Path:
        return self.root / "index_maps.json"

    @property
    def broker_names(self) -> Path:
        return self.root / "broker_names.json"

    @property
    def broker_master(self) -> Path:
        return self.root / "證券商基本資料.xls"

    def validate(self) -> list[str]:
        """Check which required files are missing.

        Returns:
            List of missing file paths (empty if all exist)
        """
        required = [
            self.trade_summary,
            self.price_master,
            self.index_maps,
        ]
        return [str(p) for p in required if not p.exists()]


@dataclass(frozen=True)
class AnalysisConfig:
    """Configuration for analysis algorithms.

    Attributes:
        min_trading_days: Minimum days for valid analysis
        min_volume: Minimum volume for valid analysis
        significance_level: P-value threshold for significance
        permutation_count: Number of permutations for testing
        lead_lag_threshold: Threshold for style classification
    """

    min_trading_days: int = 20
    min_volume: int = 100
    significance_level: float = 0.05
    permutation_count: int = 1000
    lead_lag_threshold: float = 0.05


# Default instances
DEFAULT_PATHS = DataPaths()
DEFAULT_CONFIG = AnalysisConfig()
