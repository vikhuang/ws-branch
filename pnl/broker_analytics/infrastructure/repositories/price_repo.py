"""Price data repository.

Provides unified access to price data via ws-core.
All prices come from ~/r20/data/tej/prices.parquet (managed by ws-admin).

Used by: signal_report, market_scan, export_signals, pnl_engine, hypothesis_runner
"""

from collections import defaultdict
from datetime import date

import polars as pl
from ws_core import prices as ws_prices

from broker_analytics.infrastructure.config import DataPaths, DEFAULT_PATHS


class PriceRepository:
    """Read-only access to price data via ws-core.

    Args:
        paths: DataPaths for locating parquet files.
    """

    def __init__(self, paths: DataPaths = DEFAULT_PATHS) -> None:
        self._paths = paths
        self._close_cache: dict[str, dict[date, float]] | None = None
        self._df_cache: pl.DataFrame | None = None

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

    def get_prices_df(self) -> pl.DataFrame:
        """Get close prices as DataFrame[symbol_id, date, close_price].

        Cached after first call. Used by hypothesis_runner, event_study, etc.
        """
        if self._df_cache is None:
            self._df_cache = (
                ws_prices(columns=["coid", "mdate", "close_d"], start="2021-01-01")
                .filter(pl.col("close_d").is_not_null())
                .rename({"coid": "symbol_id", "mdate": "date", "close_d": "close_price"})
                .cast({"close_price": pl.Float32})
            )
        return self._df_cache

    def _load_all_close(self) -> None:
        """Load prices from ws-core into memory (one-time)."""
        df = self.get_prices_df()
        prices: dict[str, dict[date, float]] = defaultdict(dict)
        for row in df.iter_rows(named=True):
            prices[row["symbol_id"]][row["date"]] = float(row["close_price"])
        self._close_cache = dict(prices)

    # --- OHLC ---

    def get_ohlc(self, symbol: str) -> pl.DataFrame:
        """Get OHLC for a single symbol.

        Returns:
            DataFrame[date, open, close]
        """
        df = ws_prices(
            coids=[symbol],
            columns=["mdate", "open_d", "close_d"],
            start="2021-01-01",
        )
        return df.rename({"mdate": "date", "open_d": "open", "close_d": "close"})

    def get_ohlc_batch(self, symbols: list[str]) -> dict[str, pl.DataFrame]:
        """Get OHLC for multiple symbols.

        Returns:
            {symbol: DataFrame[date, open, close]}
        """
        df = ws_prices(
            coids=symbols,
            columns=["coid", "mdate", "open_d", "close_d"],
            start="2021-01-01",
        )
        result = {}
        for sym_df in df.partition_by("coid", maintain_order=False):
            symbol = sym_df["coid"][0]
            result[symbol] = sym_df.drop("coid").rename(
                {"mdate": "date", "open_d": "open", "close_d": "close"}
            )
        return result

    # --- Cache Management ---

    def clear_cache(self) -> None:
        """Clear in-memory caches."""
        self._close_cache = None
        self._df_cache = None
