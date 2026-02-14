"""Broker Analysis Service: Query broker details.

Provides detailed analysis for a single broker using
pre-aggregated ranking data and per-symbol trade data.
"""

from dataclasses import dataclass

import polars as pl

from pnl_analytics.infrastructure.config import DataPaths, DEFAULT_PATHS
from pnl_analytics.infrastructure.repositories import (
    RankingRepository,
    TradeRepository,
    BrokerRepository,
    RepositoryError,
)


@dataclass(frozen=True, slots=True)
class BrokerAnalysisResult:
    """Analysis result for a single broker."""

    # Identity
    broker: str
    name: str
    rank: int

    # PNL
    total_pnl: float
    realized_pnl: float
    unrealized_pnl: float

    # Trading activity
    total_buy_amount: float
    total_sell_amount: float
    total_amount: float
    timing_alpha: float

    # Derived
    direction: str  # "做多", "做空", "中性"

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "broker": self.broker,
            "name": self.name,
            "rank": self.rank,
            "total_pnl": self.total_pnl,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "total_buy_amount": self.total_buy_amount,
            "total_sell_amount": self.total_sell_amount,
            "total_amount": self.total_amount,
            "timing_alpha": self.timing_alpha,
            "direction": self.direction,
        }


class BrokerAnalyzer:
    """Analyzes a single broker's trading performance.

    Example:
        >>> analyzer = BrokerAnalyzer()
        >>> result = analyzer.analyze("1440")
        >>> print(result.total_pnl)
    """

    def __init__(self, paths: DataPaths = DEFAULT_PATHS):
        self._paths = paths
        self._ranking_repo = RankingRepository(paths)
        self._trade_repo = TradeRepository(paths)
        self._broker_repo = BrokerRepository(paths)

    def analyze(self, broker: str) -> BrokerAnalysisResult | None:
        """Analyze a single broker.

        Args:
            broker: Broker code (e.g., "1440")

        Returns:
            BrokerAnalysisResult or None if broker not found
        """
        # Get ranking data
        try:
            ranking_df = self._ranking_repo.get_broker(broker)
        except RepositoryError:
            return None

        row = ranking_df.row(0, named=True)

        # Get broker name
        try:
            name = self._broker_repo.get_name(broker)
        except RepositoryError:
            name = ""

        # Determine direction from buy/sell amounts
        buy_amt = row["total_buy_amount"]
        sell_amt = row["total_sell_amount"]
        if buy_amt > sell_amt * 1.1:
            direction = "做多"
        elif sell_amt > buy_amt * 1.1:
            direction = "做空"
        else:
            direction = "中性"

        return BrokerAnalysisResult(
            broker=broker,
            name=name,
            rank=row["rank"],
            total_pnl=row["total_pnl"],
            realized_pnl=row["realized_pnl"],
            unrealized_pnl=row["unrealized_pnl"],
            total_buy_amount=row["total_buy_amount"],
            total_sell_amount=row["total_sell_amount"],
            total_amount=row["total_amount"],
            timing_alpha=row["timing_alpha"],
            direction=direction,
        )

    def get_symbol_breakdown(self, broker: str) -> pl.DataFrame:
        """Get breakdown of broker activity by symbol.

        Args:
            broker: Broker code

        Returns:
            DataFrame with per-symbol aggregates
        """
        symbols = self._trade_repo.list_symbols()
        rows = []

        for symbol in symbols:
            try:
                df = self._trade_repo.get_by_broker(symbol, broker)
                if len(df) == 0:
                    continue

                rows.append({
                    "symbol": symbol,
                    "trading_days": len(df),
                    "buy_shares": df["buy_shares"].sum(),
                    "sell_shares": df["sell_shares"].sum(),
                    "buy_amount": df["buy_amount"].sum(),
                    "sell_amount": df["sell_amount"].sum(),
                    "net_shares": df["buy_shares"].sum() - df["sell_shares"].sum(),
                })
            except RepositoryError:
                continue

        if not rows:
            return pl.DataFrame()

        return pl.DataFrame(rows).sort("buy_amount", descending=True)

    def get_daily_activity(self, broker: str, symbol: str) -> pl.DataFrame:
        """Get daily trading activity for a broker in a specific symbol.

        Args:
            broker: Broker code
            symbol: Stock symbol

        Returns:
            DataFrame with daily trades
        """
        return self._trade_repo.get_by_broker(symbol, broker)
