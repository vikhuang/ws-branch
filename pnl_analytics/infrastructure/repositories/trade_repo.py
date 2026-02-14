"""Trade Repository: Access to per-symbol trade data.

Provides read access to daily_summary/{symbol}.parquet files.
Each file contains broker-level daily aggregates for one symbol.
"""

import polars as pl

from pnl_analytics.infrastructure.repositories.base import Repository, RepositoryError
from pnl_analytics.infrastructure.config import DataPaths, DEFAULT_PATHS


class TradeRepository(Repository[pl.DataFrame]):
    """Repository for daily trade summaries.

    Loads data from daily_summary/{symbol}.parquet files.
    Supports loading single symbols or aggregating across symbols.

    Example:
        >>> repo = TradeRepository()
        >>> df = repo.get_symbol("2330")
        >>> symbols = repo.list_symbols()
    """

    def __init__(self, paths: DataPaths = DEFAULT_PATHS):
        self._paths = paths
        self._symbol_cache: dict[str, pl.DataFrame] = {}

    def get_all(self) -> pl.DataFrame:
        """Load all trade data (all symbols concatenated).

        Warning: May be slow for 2800+ symbols. Use get_symbol() for single symbol.

        Returns:
            DataFrame with columns: symbol_id, broker, date,
            buy_shares, sell_shares, buy_amount, sell_amount
        """
        symbols = self.list_symbols()
        if not symbols:
            raise RepositoryError(
                "No trade data found",
                str(self._paths.daily_summary_dir)
            )

        dfs = []
        for symbol in symbols:
            df = self.get_symbol(symbol)
            dfs.append(df.with_columns(pl.lit(symbol).alias("symbol_id")))

        return pl.concat(dfs)

    def get_symbol(self, symbol: str) -> pl.DataFrame:
        """Load trade data for a single symbol.

        Args:
            symbol: Stock symbol (e.g., "2330")

        Returns:
            DataFrame with columns: broker, date,
            buy_shares, sell_shares, buy_amount, sell_amount

        Raises:
            RepositoryError: If file not found
        """
        if symbol in self._symbol_cache:
            return self._symbol_cache[symbol]

        path = self._paths.symbol_trade_path(symbol)
        if not path.exists():
            raise RepositoryError(f"Trade data not found for {symbol}", str(path))

        try:
            df = pl.read_parquet(path)
            self._symbol_cache[symbol] = df
            return df
        except Exception as e:
            raise RepositoryError(f"Failed to read trade data: {e}", str(path))

    def get_by_broker(self, symbol: str, broker: str) -> pl.DataFrame:
        """Get trades for a specific broker in a symbol.

        Args:
            symbol: Stock symbol
            broker: Broker code (e.g., "1440")

        Returns:
            Filtered DataFrame for the broker
        """
        return self.get_symbol(symbol).filter(pl.col("broker") == broker)

    def list_symbols(self) -> list[str]:
        """Get list of all available symbols."""
        return self._paths.list_symbols()

    def get_brokers(self, symbol: str) -> list[str]:
        """Get list of all brokers for a symbol."""
        return self.get_symbol(symbol)["broker"].unique().sort().to_list()

    def get_dates(self, symbol: str) -> list:
        """Get list of all dates for a symbol."""
        return self.get_symbol(symbol)["date"].unique().sort().to_list()

    def clear_cache(self) -> None:
        """Clear cached data."""
        self._symbol_cache.clear()
