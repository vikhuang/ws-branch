"""Price data repository.

Provides unified access to close prices and OHLC data.
Reads from local parquet files; fetches from BigQuery on cache miss.

Used by: signal_report, market_scan, export_signals, pnl_engine
"""

from collections import defaultdict
from datetime import date
from pathlib import Path

import polars as pl

from broker_analytics.infrastructure.config import DataPaths, DEFAULT_PATHS


class PriceRepository:
    """Read-only access to price data.

    Args:
        paths: DataPaths for locating parquet files.
    """

    def __init__(self, paths: DataPaths = DEFAULT_PATHS) -> None:
        self._paths = paths
        self._close_cache: dict[str, dict[date, float]] | None = None

    # --- Close Prices ---

    def get_close_prices(self, symbol: str) -> dict[date, float]:
        """Get close prices for a single symbol.

        Returns:
            {date: close_price} dict.
        """
        if self._close_cache is None:
            self._load_all_close()
        return self._close_cache.get(symbol, {})

    def get_all_close_prices(self) -> dict[str, dict[date, float]]:
        """Get close prices for all symbols.

        Returns:
            {symbol: {date: close_price}} dict.
        """
        if self._close_cache is None:
            self._load_all_close()
        return self._close_cache

    def _load_all_close(self) -> None:
        """Load close_prices.parquet into memory (one-time)."""
        path = self._paths.close_prices
        if not path.exists():
            self._close_cache = {}
            return

        df = pl.read_parquet(path)
        prices: dict[str, dict[date, float]] = defaultdict(dict)
        for row in df.iter_rows(named=True):
            d = row["date"]
            if isinstance(d, str):
                d = date.fromisoformat(d)
            prices[row["symbol_id"]][d] = float(row["close_price"])
        self._close_cache = dict(prices)

    # --- OHLC ---

    def get_ohlc(self, symbol: str) -> pl.DataFrame:
        """Get OHLC for a single symbol (from cache or BigQuery).

        Returns:
            DataFrame[date, open, close]
        """
        from broker_analytics.infrastructure.bigquery import fetch_ohlc
        return fetch_ohlc(symbol, cache_dir=self._paths.price_dir)

    def get_ohlc_batch(self, symbols: list[str]) -> dict[str, pl.DataFrame]:
        """Get OHLC for multiple symbols (from cache or BigQuery).

        Returns:
            {symbol: DataFrame[date, open, close]}
        """
        from broker_analytics.infrastructure.bigquery import fetch_ohlc_batch
        return fetch_ohlc_batch(symbols, cache_dir=self._paths.price_dir)

    # --- Cache Management ---

    def clear_cache(self) -> None:
        """Clear in-memory close price cache."""
        self._close_cache = None
