"""PNL Repository: Access to broker ranking data.

Provides read access to derived/broker_ranking.parquet.
Contains pre-aggregated broker PNL across all symbols.
"""

import polars as pl

from pnl_analytics.infrastructure.repositories.base import Repository, RepositoryError
from pnl_analytics.infrastructure.config import DataPaths, DEFAULT_PATHS


class RankingRepository(Repository[pl.DataFrame]):
    """Repository for broker ranking data.

    Loads pre-aggregated PNL from derived/broker_ranking.parquet.

    Example:
        >>> repo = RankingRepository()
        >>> df = repo.get_all()
        >>> top10 = repo.get_top(10)
        >>> broker = repo.get_broker("1440")
    """

    def __init__(self, paths: DataPaths = DEFAULT_PATHS):
        self._paths = paths
        self._cache: pl.DataFrame | None = None

    def get_all(self) -> pl.DataFrame:
        """Load broker ranking data.

        Returns:
            DataFrame with columns: rank, broker, total_pnl,
            realized_pnl, unrealized_pnl, total_buy_amount,
            total_sell_amount, total_amount, win_count, loss_count,
            trade_count, win_rate

        Raises:
            RepositoryError: If file cannot be read
        """
        if self._cache is not None:
            return self._cache

        path = self._paths.broker_ranking
        if not path.exists():
            raise RepositoryError("Broker ranking not found", str(path))

        try:
            self._cache = pl.read_parquet(path)
            return self._cache
        except Exception as e:
            raise RepositoryError(f"Failed to read ranking: {e}", str(path))

    def get_top(self, n: int = 10) -> pl.DataFrame:
        """Get top N brokers by PNL.

        Args:
            n: Number of top brokers to return

        Returns:
            DataFrame with top N brokers
        """
        return self.get_all().head(n)

    def get_bottom(self, n: int = 10) -> pl.DataFrame:
        """Get bottom N brokers by PNL.

        Args:
            n: Number of bottom brokers to return

        Returns:
            DataFrame with bottom N brokers
        """
        return self.get_all().tail(n)

    def get_broker(self, broker: str) -> pl.DataFrame:
        """Get ranking for a specific broker.

        Args:
            broker: Broker code (e.g., "1440")

        Returns:
            Single-row DataFrame for the broker

        Raises:
            RepositoryError: If broker not found
        """
        df = self.get_all().filter(pl.col("broker") == broker)
        if len(df) == 0:
            raise RepositoryError(f"Broker {broker} not found in ranking")
        return df

    def get_broker_rank(self, broker: str) -> int | None:
        """Get rank for a specific broker.

        Args:
            broker: Broker code

        Returns:
            Rank (1-indexed) or None if not found
        """
        try:
            df = self.get_broker(broker)
            return df["rank"].item()
        except RepositoryError:
            return None

    def get_broker_pnl(self, broker: str) -> float | None:
        """Get total PNL for a specific broker.

        Args:
            broker: Broker code

        Returns:
            Total PNL or None if not found
        """
        try:
            df = self.get_broker(broker)
            return df["total_pnl"].item()
        except RepositoryError:
            return None

    def list_brokers(self) -> list[str]:
        """Get list of all brokers in ranking order."""
        return self.get_all()["broker"].to_list()

    def get_total_pnl(self) -> float:
        """Get total PNL across all brokers."""
        return self.get_all()["total_pnl"].sum()

    def get_broker_count(self) -> int:
        """Get number of brokers in ranking."""
        return len(self.get_all())

    def clear_cache(self) -> None:
        """Clear cached data."""
        self._cache = None
