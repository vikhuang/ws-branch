"""Trade Repository: Access to trade data.

Provides read access to:
- daily_trade_summary.parquet (daily aggregated trades)
- closed_trades.parquet (completed trades for alpha analysis)
"""

from pathlib import Path
from typing import Sequence

import polars as pl

from pnl_analytics.infrastructure.repositories.base import Repository, RepositoryError
from pnl_analytics.infrastructure.config import DataPaths, DEFAULT_PATHS


class TradeRepository(Repository[pl.DataFrame]):
    """Repository for daily trade summary data.

    Provides access to aggregated daily trades with caching.

    Example:
        >>> repo = TradeRepository()
        >>> df = repo.get_all()
        >>> broker_df = repo.get_by_broker("1440")
    """

    def __init__(self, paths: DataPaths = DEFAULT_PATHS):
        self._paths = paths
        self._cache: pl.DataFrame | None = None

    def get_all(self) -> pl.DataFrame:
        """Load all trade data.

        Returns:
            DataFrame with columns: date, symbol_id, broker,
            buy_shares, sell_shares, buy_amount, sell_amount

        Raises:
            RepositoryError: If file cannot be read
        """
        if self._cache is not None:
            return self._cache

        path = self._paths.trade_summary
        if not path.exists():
            raise RepositoryError(f"Trade summary file not found", str(path))

        try:
            self._cache = pl.read_parquet(path)
            return self._cache
        except Exception as e:
            raise RepositoryError(f"Failed to read trade summary: {e}", str(path))

    def get_by_broker(self, broker: str) -> pl.DataFrame:
        """Get trades for a specific broker.

        Args:
            broker: Broker code (e.g., "1440")

        Returns:
            Filtered DataFrame for the broker
        """
        if not broker:
            raise ValueError("broker cannot be empty")
        return self.get_all().filter(pl.col("broker") == broker)

    def get_by_brokers(self, brokers: Sequence[str]) -> pl.DataFrame:
        """Get trades for multiple brokers.

        Args:
            brokers: List of broker codes

        Returns:
            Filtered DataFrame for the brokers
        """
        if not brokers:
            raise ValueError("brokers list cannot be empty")
        return self.get_all().filter(pl.col("broker").is_in(brokers))

    def get_by_date_range(self, start: str, end: str) -> pl.DataFrame:
        """Get trades within a date range.

        Args:
            start: Start date (inclusive) in YYYY-MM-DD format
            end: End date (inclusive) in YYYY-MM-DD format

        Returns:
            Filtered DataFrame for the date range
        """
        return self.get_all().filter(pl.col("date").is_between(start, end))

    def get_brokers(self) -> list[str]:
        """Get list of all unique broker codes."""
        return self.get_all()["broker"].unique().sort().to_list()

    def get_dates(self) -> list[str]:
        """Get list of all unique dates."""
        return self.get_all()["date"].unique().sort().to_list()

    def clear_cache(self) -> None:
        """Clear cached data."""
        self._cache = None


class ClosedTradeRepository(Repository[pl.DataFrame]):
    """Repository for closed trade data.

    Provides access to completed trades for alpha analysis.
    """

    def __init__(self, paths: DataPaths = DEFAULT_PATHS):
        self._paths = paths
        self._cache: pl.DataFrame | None = None

    def get_all(self) -> pl.DataFrame:
        """Load all closed trades.

        Returns:
            DataFrame with columns: symbol, broker, shares,
            buy_date, buy_price, sell_date, sell_price,
            realized_pnl, trade_type

        Raises:
            RepositoryError: If file cannot be read
        """
        if self._cache is not None:
            return self._cache

        path = self._paths.closed_trades
        if not path.exists():
            raise RepositoryError(f"Closed trades file not found", str(path))

        try:
            self._cache = pl.read_parquet(path)
            return self._cache
        except Exception as e:
            raise RepositoryError(f"Failed to read closed trades: {e}", str(path))

    def get_by_broker(self, broker: str) -> pl.DataFrame:
        """Get closed trades for a specific broker."""
        if not broker:
            raise ValueError("broker cannot be empty")
        return self.get_all().filter(pl.col("broker") == broker)

    def get_by_trade_type(self, trade_type: str) -> pl.DataFrame:
        """Get closed trades by type (long/short)."""
        if trade_type not in ("long", "short"):
            raise ValueError(f"trade_type must be 'long' or 'short', got: {trade_type}")
        return self.get_all().filter(pl.col("trade_type") == trade_type)

    def clear_cache(self) -> None:
        """Clear cached data."""
        self._cache = None
