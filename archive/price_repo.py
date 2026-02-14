"""Price Repository: Access to close price data.

Provides read access to data/price/close_prices.parquet.
Contains daily close prices for all symbols.
"""

from datetime import date

import polars as pl

from pnl_analytics.infrastructure.repositories.base import Repository, RepositoryError
from pnl_analytics.infrastructure.config import DataPaths, DEFAULT_PATHS


class PriceRepository(Repository[pl.DataFrame]):
    """Repository for close price data.

    Loads data from data/price/close_prices.parquet.

    Example:
        >>> repo = PriceRepository()
        >>> df = repo.get_all()
        >>> lookup = repo.get_lookup()  # {(symbol, date): price}
    """

    def __init__(self, paths: DataPaths = DEFAULT_PATHS):
        self._paths = paths
        self._cache: pl.DataFrame | None = None
        self._lookup_cache: dict[tuple[str, date], float] | None = None

    def get_all(self) -> pl.DataFrame:
        """Load all price data.

        Returns:
            DataFrame with columns: symbol_id, date, close_price
            sorted by symbol_id, date

        Raises:
            RepositoryError: If file cannot be read
        """
        if self._cache is not None:
            return self._cache

        path = self._paths.close_prices
        if not path.exists():
            raise RepositoryError("Price file not found", str(path))

        try:
            self._cache = pl.read_parquet(path).sort(["symbol_id", "date"])
            return self._cache
        except Exception as e:
            raise RepositoryError(f"Failed to read prices: {e}", str(path))

    def get_lookup(self) -> dict[tuple[str, date], float]:
        """Get prices as a lookup dictionary.

        Returns:
            Dict mapping (symbol_id, date) to close_price
        """
        if self._lookup_cache is not None:
            return self._lookup_cache

        df = self.get_all()
        self._lookup_cache = {}

        for row in df.iter_rows(named=True):
            key = (row["symbol_id"], row["date"])
            self._lookup_cache[key] = row["close_price"]

        return self._lookup_cache

    def get_symbol(self, symbol: str) -> pl.DataFrame:
        """Get prices for a specific symbol.

        Args:
            symbol: Stock symbol (e.g., "2330")

        Returns:
            DataFrame with date, close_price columns
        """
        return self.get_all().filter(pl.col("symbol_id") == symbol)

    def get_price(self, symbol: str, d: date) -> float | None:
        """Get price for a specific symbol and date.

        Args:
            symbol: Stock symbol
            d: Date object

        Returns:
            Close price or None if not found
        """
        return self.get_lookup().get((symbol, d))

    def list_symbols(self) -> list[str]:
        """Get list of all symbols with price data."""
        return self.get_all()["symbol_id"].unique().sort().to_list()

    def get_date_range(self) -> tuple[date, date]:
        """Get min and max dates in price data.

        Returns:
            Tuple of (min_date, max_date)
        """
        df = self.get_all()
        return df["date"].min(), df["date"].max()

    def clear_cache(self) -> None:
        """Clear cached data."""
        self._cache = None
        self._lookup_cache = None
