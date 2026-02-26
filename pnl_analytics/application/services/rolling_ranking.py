"""Rolling PNL Ranking Service.

Computes broker rankings over a rolling time window using pnl_daily data.
For a given date T and window of N years, ranks brokers by total PNL
accumulated in [T-N years, T] across all symbols.

FIFO state accumulates from the beginning; only PNL events within the
window are counted (realized sum + unrealized at window end).
"""

from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date
from pathlib import Path

import polars as pl

from pnl_analytics.infrastructure.config import DataPaths, DEFAULT_PATHS
from pnl_analytics.infrastructure.repositories import BrokerRepository


def _process_one_symbol(
    path: Path,
    window_start: date,
    window_end: date,
) -> dict[str, tuple[float, float]]:
    """Process a single pnl_daily file for one symbol.

    Returns:
        {broker: (realized_sum, unrealized_at_end)} for the window.
    """
    df = pl.read_parquet(path)
    df = df.filter(
        (pl.col("date") >= window_start) & (pl.col("date") <= window_end)
    )
    if len(df) == 0:
        return {}

    agg = (
        df.sort("date")
        .group_by("broker")
        .agg([
            pl.col("realized_pnl").sum(),
            pl.col("unrealized_pnl").last(),
        ])
    )

    return {
        row["broker"]: (row["realized_pnl"], row["unrealized_pnl"])
        for row in agg.iter_rows(named=True)
    }


class RollingRankingService:
    """Computes broker ranking over a rolling PNL window.

    Example:
        >>> service = RollingRankingService()
        >>> df = service.compute(date(2025, 12, 30), window_years=3)
        >>> print(df.head(10))
    """

    def __init__(self, paths: DataPaths = DEFAULT_PATHS, workers: int = 12):
        self._paths = paths
        self._workers = workers
        self._broker_repo = BrokerRepository(paths)

    def compute(
        self,
        query_date: date,
        window_years: int = 3,
        with_names: bool = True,
    ) -> pl.DataFrame:
        """Compute rolling broker ranking.

        Args:
            query_date: End date of the window (T).
            window_years: Window size in years.
            with_names: Add broker name column.

        Returns:
            DataFrame with columns: rank, broker, [name], total_pnl,
            realized_pnl, unrealized_pnl.
        """
        # Handle leap year edge case (e.g., 2024-02-29 - 3y → 2021-02-28)
        try:
            window_start = query_date.replace(year=query_date.year - window_years)
        except ValueError:
            # Feb 29 in non-leap year → use Feb 28
            window_start = date(
                query_date.year - window_years,
                query_date.month,
                query_date.day - 1,
            )

        # Collect all pnl_daily files
        pnl_daily_dir = self._paths.pnl_daily_dir
        files = sorted(pnl_daily_dir.glob("*.parquet"))
        if not files:
            return pl.DataFrame()

        # Parallel processing
        broker_totals: dict[str, dict] = defaultdict(
            lambda: {"realized": 0.0, "unrealized": 0.0}
        )

        with ProcessPoolExecutor(max_workers=self._workers) as executor:
            futures = {
                executor.submit(
                    _process_one_symbol, f, window_start, query_date,
                ): f
                for f in files
            }

            for future in as_completed(futures):
                result = future.result()
                for broker, (realized, unrealized) in result.items():
                    b = broker_totals[broker]
                    b["realized"] += realized
                    b["unrealized"] += unrealized

        # Build ranking
        rows = []
        for broker, totals in broker_totals.items():
            rows.append({
                "broker": broker,
                "total_pnl": totals["realized"] + totals["unrealized"],
                "realized_pnl": totals["realized"],
                "unrealized_pnl": totals["unrealized"],
            })

        if not rows:
            return pl.DataFrame()

        df = pl.DataFrame(rows)
        df = df.sort("total_pnl", descending=True)
        df = df.with_row_index("rank", offset=1)

        if with_names:
            try:
                broker_names = self._broker_repo.get_all()
                df = df.with_columns(
                    pl.col("broker")
                    .map_elements(
                        lambda b: broker_names.get(b, ""),
                        return_dtype=pl.Utf8,
                    )
                    .alias("name")
                )
            except Exception:
                pass

        return df
