"""Symbol Analysis Service: Smart money signal for individual stocks.

For a given stock, measures whether historically profitable brokers
are buying or selling, across multiple time windows.

Signal: sum of PNL ranks for the top 15 net buyers/sellers.
Lower sum = skilled traders are active on that side.
"""

from dataclasses import dataclass
from datetime import date

import polars as pl

from pnl_analytics.infrastructure.config import DataPaths, DEFAULT_PATHS
from pnl_analytics.infrastructure.repositories import (
    TradeRepository,
    RankingRepository,
    BrokerRepository,
    RepositoryError,
)

DEFAULT_WINDOWS = (1, 5, 10, 20, 60)
TOP_N = 15


@dataclass(frozen=True, slots=True)
class SmartMoneySignal:
    """Signal for a single time window.

    Attributes:
        window: Number of trading days in the window
        buy_rank_sum: Sum of PNL ranks for top N net buyers
        sell_rank_sum: Sum of PNL ranks for top N net sellers
        n_active_brokers: Brokers active in this window
        realized_pnl: Σ realized_pnl for active brokers (global PNL)
        unrealized_pnl: Σ unrealized_pnl for active brokers (global PNL)
    """
    window: int
    buy_rank_sum: int
    sell_rank_sum: int
    n_active_brokers: int
    realized_pnl: float
    unrealized_pnl: float


@dataclass(frozen=True, slots=True)
class SymbolAnalysisResult:
    """Analysis result for a single stock across multiple windows."""
    symbol: str
    last_date: date
    signals: tuple[SmartMoneySignal, ...]

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "last_date": self.last_date.isoformat(),
            "signals": [
                {
                    "window": s.window,
                    "buy_rank_sum": s.buy_rank_sum,
                    "sell_rank_sum": s.sell_rank_sum,
                    "n_active_brokers": s.n_active_brokers,
                    "realized_pnl": s.realized_pnl,
                    "unrealized_pnl": s.unrealized_pnl,
                }
                for s in self.signals
            ],
        }


class SymbolAnalyzer:
    """Analyzes smart money flow for a single stock.

    Example:
        >>> analyzer = SymbolAnalyzer()
        >>> result = analyzer.analyze("2330")
        >>> for s in result.signals:
        ...     print(f"{s.window}d: buy={s.buy_rank_sum} sell={s.sell_rank_sum}")
    """

    def __init__(self, paths: DataPaths = DEFAULT_PATHS):
        self._paths = paths
        self._trade_repo = TradeRepository(paths)
        self._ranking_repo = RankingRepository(paths)
        self._broker_repo = BrokerRepository(paths)

    def analyze(
        self,
        symbol: str,
        windows: tuple[int, ...] = DEFAULT_WINDOWS,
    ) -> SymbolAnalysisResult | None:
        """Compute smart money signals for all time windows.

        Args:
            symbol: Stock symbol (e.g., "2330")
            windows: Trading day windows to compute

        Returns:
            SymbolAnalysisResult or None if symbol not found
        """
        try:
            trade_df = self._trade_repo.get_symbol(symbol)
        except RepositoryError:
            return None

        ranking_df = self._ranking_repo.get_all().select(
            "broker", "rank", "realized_pnl", "unrealized_pnl"
        )

        all_dates = sorted(trade_df["date"].unique().to_list())
        if not all_dates:
            return None

        last_date = all_dates[-1]
        signals = []

        for window in windows:
            window_dates = all_dates[-window:]
            signal = self._compute_signal(trade_df, ranking_df, window_dates, window)
            signals.append(signal)

        return SymbolAnalysisResult(
            symbol=symbol,
            last_date=last_date,
            signals=tuple(signals),
        )

    def get_top_brokers(
        self,
        symbol: str,
        window: int = 1,
        n: int = TOP_N,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Get top N net buyers and sellers for a specific window.

        Args:
            symbol: Stock symbol
            window: Number of trading days
            n: Number of top brokers to return

        Returns:
            (buy_top_df, sell_top_df) with columns:
            broker, name, net_buy, rank
        """
        trade_df = self._trade_repo.get_symbol(symbol)
        ranking_df = self._ranking_repo.get_all().select(
            "broker", "rank"
        )

        all_dates = sorted(trade_df["date"].unique().to_list())
        window_dates = all_dates[-window:]

        agg = self._aggregate_window(trade_df, ranking_df, window_dates)

        # Add broker names
        try:
            broker_names = self._broker_repo.get_all()
        except RepositoryError:
            broker_names = {}

        agg = agg.with_columns(
            pl.col("broker")
            .map_elements(lambda b: broker_names.get(b, ""), return_dtype=pl.Utf8)
            .alias("name")
        ).select("broker", "name", "net_buy", "rank")

        buy_top = agg.sort("net_buy", descending=True).head(n)
        sell_top = agg.sort("net_buy").head(n)

        return buy_top, sell_top

    def _aggregate_window(
        self,
        trade_df: pl.DataFrame,
        ranking_df: pl.DataFrame,
        window_dates: list,
    ) -> pl.DataFrame:
        """Aggregate trades for a window and join with ranking."""
        window_df = trade_df.filter(pl.col("date").is_in(window_dates))

        agg = window_df.group_by("broker").agg(
            (pl.col("buy_shares").sum() - pl.col("sell_shares").sum()).alias("net_buy"),
        ).with_columns(
            pl.col("broker").cast(pl.Utf8),
        )

        return agg.join(ranking_df, on="broker", how="left")

    def _compute_signal(
        self,
        trade_df: pl.DataFrame,
        ranking_df: pl.DataFrame,
        window_dates: list,
        window: int,
    ) -> SmartMoneySignal:
        """Compute signal for a single window."""
        agg = self._aggregate_window(trade_df, ranking_df, window_dates)

        # Filter to brokers that have a rank (exist in ranking)
        ranked = agg.filter(pl.col("rank").is_not_null())

        n_active = len(ranked)

        if n_active == 0:
            return SmartMoneySignal(
                window=window,
                buy_rank_sum=0,
                sell_rank_sum=0,
                n_active_brokers=0,
                realized_pnl=0.0,
                unrealized_pnl=0.0,
            )

        n = min(TOP_N, n_active)

        buy_top = ranked.sort("net_buy", descending=True).head(n)
        buy_rank_sum = int(buy_top["rank"].sum())

        sell_top = ranked.sort("net_buy").head(n)
        sell_rank_sum = int(sell_top["rank"].sum())

        return SmartMoneySignal(
            window=window,
            buy_rank_sum=buy_rank_sum,
            sell_rank_sum=sell_rank_sum,
            n_active_brokers=n_active,
            realized_pnl=ranked["realized_pnl"].sum(),
            unrealized_pnl=ranked["unrealized_pnl"].sum(),
        )
