"""Price Repository: Access to price data.

Provides read access to:
- price_master.parquet (daily closing prices)
"""

from pathlib import Path

import polars as pl

from pnl_analytics.infrastructure.repositories.base import Repository, RepositoryError
from pnl_analytics.infrastructure.config import DataPaths, DEFAULT_PATHS


class PriceRepository(Repository[pl.DataFrame]):
    """Repository for price data.

    Provides access to daily closing prices with caching.

    Example:
        >>> repo = PriceRepository()
        >>> df = repo.get_all()
        >>> prices = repo.get_price_dict()  # {date: price}
    """

    def __init__(self, paths: DataPaths = DEFAULT_PATHS):
        self._paths = paths
        self._cache: pl.DataFrame | None = None
        self._price_dict_cache: dict[str, float] | None = None

    def get_all(self) -> pl.DataFrame:
        """Load all price data.

        Returns:
            DataFrame with columns: coid, date, close_price
            sorted by date ascending

        Raises:
            RepositoryError: If file cannot be read
        """
        if self._cache is not None:
            return self._cache

        path = self._paths.price_master
        if not path.exists():
            raise RepositoryError(f"Price master file not found", str(path))

        try:
            self._cache = pl.read_parquet(path).sort("date")
            return self._cache
        except Exception as e:
            raise RepositoryError(f"Failed to read price master: {e}", str(path))

    def get_price_dict(self) -> dict[str, float]:
        """Get prices as a dictionary.

        Returns:
            Dict mapping date (YYYY-MM-DD) to close_price
        """
        if self._price_dict_cache is not None:
            return self._price_dict_cache

        df = self.get_all()
        self._price_dict_cache = {
            row["date"]: row["close_price"]
            for row in df.iter_rows(named=True)
        }
        return self._price_dict_cache

    def get_price(self, date: str) -> float | None:
        """Get price for a specific date.

        Args:
            date: Date in YYYY-MM-DD format

        Returns:
            Close price or None if not found
        """
        return self.get_price_dict().get(date)

    def get_first_price(self) -> float:
        """Get the first (earliest) price."""
        df = self.get_all()
        return df.head(1)["close_price"].item()

    def get_last_price(self) -> float:
        """Get the last (latest) price."""
        df = self.get_all()
        return df.tail(1)["close_price"].item()

    def get_first_date(self) -> str:
        """Get the first (earliest) date."""
        df = self.get_all()
        return df.head(1)["date"].item()

    def get_last_date(self) -> str:
        """Get the last (latest) date."""
        df = self.get_all()
        return df.tail(1)["date"].item()

    def get_market_return(self) -> float:
        """Calculate total market return over the period.

        Returns:
            Return as decimal (e.g., 3.6 for 360%)
        """
        first = self.get_first_price()
        last = self.get_last_price()
        if first == 0:
            return 0.0
        return (last - first) / first

    def get_dates(self) -> list[str]:
        """Get list of all dates."""
        return self.get_all()["date"].to_list()

    def clear_cache(self) -> None:
        """Clear cached data."""
        self._cache = None
        self._price_dict_cache = None
