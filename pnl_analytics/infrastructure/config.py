"""Configuration: Centralized paths and settings.

This module provides:
- DataPaths: File paths for all data sources
- AnalysisConfig: Parameters for analysis algorithms

Directory Structure (v2):
    data/
    ├── daily_summary/           # ETL output (by symbol)
    │   ├── 2330.parquet
    │   └── ...
    ├── price/
    │   └── close_prices.parquet
    ├── pnl/                     # PNL engine output (by symbol)
    │   ├── 2330.parquet
    │   └── ...
    └── derived/                 # Pre-aggregated tables
        └── broker_ranking.parquet
"""

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DataPaths:
    """File paths for data sources.

    Attributes:
        root: Project root directory
        data_dir: Main data directory
    """

    root: Path = Path(".")

    # --- Directories ---

    @property
    def data_dir(self) -> Path:
        """Main data directory."""
        return self.root / "data"

    @property
    def daily_summary_dir(self) -> Path:
        """Daily trade summaries (by symbol)."""
        return self.data_dir / "daily_summary"

    @property
    def price_dir(self) -> Path:
        """Price data directory."""
        return self.data_dir / "price"

    @property
    def pnl_dir(self) -> Path:
        """PNL results (by symbol)."""
        return self.data_dir / "pnl"

    @property
    def derived_dir(self) -> Path:
        """Pre-aggregated tables for queries."""
        return self.data_dir / "derived"

    # --- Files ---

    @property
    def close_prices(self) -> Path:
        """Close prices for all symbols."""
        return self.price_dir / "close_prices.parquet"

    @property
    def broker_ranking(self) -> Path:
        """Pre-aggregated broker ranking table."""
        return self.derived_dir / "broker_ranking.parquet"

    @property
    def broker_names(self) -> Path:
        """Broker name mappings (JSON)."""
        return self.root / "broker_names.json"

    @property
    def broker_master(self) -> Path:
        """Official broker data (XLS)."""
        return self.root / "證券商基本資料.xls"

    # --- Helper Methods ---

    def symbol_trade_path(self, symbol: str) -> Path:
        """Path to a symbol's daily trade summary."""
        return self.daily_summary_dir / f"{symbol}.parquet"

    def symbol_pnl_path(self, symbol: str) -> Path:
        """Path to a symbol's PNL results."""
        return self.pnl_dir / f"{symbol}.parquet"

    def list_symbols(self) -> list[str]:
        """List all symbols with trade data."""
        if not self.daily_summary_dir.exists():
            return []
        return sorted(p.stem for p in self.daily_summary_dir.glob("*.parquet"))

    def validate(self) -> list[str]:
        """Check which required paths are missing.

        Returns:
            List of missing paths (empty if all exist)
        """
        missing = []

        # Check directories
        if not self.daily_summary_dir.exists():
            missing.append(str(self.daily_summary_dir))
        if not self.price_dir.exists():
            missing.append(str(self.price_dir))

        # Check required files
        if not self.close_prices.exists():
            missing.append(str(self.close_prices))

        return missing

    def ensure_dirs(self) -> None:
        """Create all required directories."""
        self.daily_summary_dir.mkdir(parents=True, exist_ok=True)
        self.price_dir.mkdir(parents=True, exist_ok=True)
        self.pnl_dir.mkdir(parents=True, exist_ok=True)
        self.derived_dir.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class AnalysisConfig:
    """Configuration for analysis algorithms.

    Attributes:
        backtest_start: Start date for performance calculation (FIFO from 2021)
        min_trading_days: Minimum days for valid analysis
        min_volume: Minimum volume for valid analysis
        significance_level: P-value threshold for significance
        permutation_count: Number of permutations for testing
        lead_lag_threshold: Threshold for style classification
        parallel_workers: Number of parallel workers for PNL calculation
    """

    backtest_start: str = "2023-01-01"
    min_trading_days: int = 20
    min_volume: int = 100
    significance_level: float = 0.05
    permutation_count: int = 1000
    lead_lag_threshold: float = 0.05
    parallel_workers: int = 12


# Default instances
DEFAULT_PATHS = DataPaths()
DEFAULT_CONFIG = AnalysisConfig()
