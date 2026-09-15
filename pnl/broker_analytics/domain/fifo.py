"""FIFO position tracking for broker accounts.

Each (symbol, broker) pair is an independent account.
- Sells close long positions first (FIFO), then open short
- Buys cover short positions first (FIFO), then open long
- Tracks realized PNL (closed trades) and unrealized PNL (open positions)
"""

from dataclasses import dataclass, field
from datetime import date
from typing import NamedTuple


@dataclass
class Lot:
    """A single purchase/short lot."""
    shares: int
    cost_per_share: float
    buy_date: date


@dataclass
class FIFOAccount:
    """Tracks position for a (symbol, broker) pair using FIFO."""
    long_lots: list = field(default_factory=list)
    short_lots: list = field(default_factory=list)

    @property
    def position(self) -> int:
        """Net position: positive=long, negative=short."""
        long_shares = sum(lot.shares for lot in self.long_lots)
        short_shares = sum(lot.shares for lot in self.short_lots)
        return long_shares - short_shares

    def process_day(
        self,
        buy_shares: int,
        sell_shares: int,
        buy_amount: float,
        sell_amount: float,
        close_price: float,
        current_date: date,
    ) -> tuple[float, float]:
        """Process a day's transactions using FIFO.

        Returns:
            (realized_pnl_today, unrealized_pnl)
        """
        realized_today = 0.0
        avg_buy = buy_amount / buy_shares if buy_shares > 0 else 0.0
        avg_sell = sell_amount / sell_shares if sell_shares > 0 else 0.0

        # Process sells: close longs first, then open shorts
        if sell_shares > 0:
            while sell_shares > 0 and self.long_lots:
                lot = self.long_lots[0]
                take = min(sell_shares, lot.shares)
                realized_today += take * (avg_sell - lot.cost_per_share)
                lot.shares -= take
                sell_shares -= take
                if lot.shares == 0:
                    self.long_lots.pop(0)

            if sell_shares > 0:
                self.short_lots.append(Lot(sell_shares, avg_sell, current_date))

        # Process buys: cover shorts first, then open longs
        if buy_shares > 0:
            while buy_shares > 0 and self.short_lots:
                lot = self.short_lots[0]
                take = min(buy_shares, lot.shares)
                realized_today += take * (lot.cost_per_share - avg_buy)
                lot.shares -= take
                buy_shares -= take
                if lot.shares == 0:
                    self.short_lots.pop(0)

            if buy_shares > 0:
                self.long_lots.append(Lot(buy_shares, avg_buy, current_date))

        # Calculate unrealized PNL
        unrealized = 0.0
        for lot in self.long_lots:
            unrealized += lot.shares * (close_price - lot.cost_per_share)
        for lot in self.short_lots:
            unrealized += lot.shares * (lot.cost_per_share - close_price)

        return realized_today, unrealized

    def get_lots(self) -> list[tuple[str, int, float, date]]:
        """Extract current lots for serialization.

        Returns:
            List of (side, shares, cost_per_share, open_date) tuples.
        """
        lots = []
        for lot in self.long_lots:
            lots.append(("long", lot.shares, lot.cost_per_share, lot.buy_date))
        for lot in self.short_lots:
            lots.append(("short", lot.shares, lot.cost_per_share, lot.buy_date))
        return lots


class BrokerResult(NamedTuple):
    """Aggregated result for a single broker in a symbol."""
    broker: str
    total_pnl: float
    realized_pnl: float
    unrealized_pnl: float
    total_buy: float
    total_sell: float
    timing_alpha: float
