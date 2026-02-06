"""Domain Models: Core data structures for PNL analytics.

These models represent the fundamental business entities:
- Lot: A single purchase/short lot with cost basis
- ClosedTrade: A completed (closed) trade with realized PNL
- TradeType: Enum for trade direction

Design Principles:
- Immutable where possible (frozen dataclass)
- Defensive validation in __post_init__
- Computed properties for derived values
- Type-safe with Literal types
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

# Type alias for trade direction
TradeType = Literal["long", "short"]

# Date format pattern
DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _validate_date(date_str: str, field_name: str) -> None:
    """Validate date string format (YYYY-MM-DD)."""
    if not date_str:
        raise ValueError(f"{field_name} cannot be empty")
    if not DATE_PATTERN.match(date_str):
        raise ValueError(f"{field_name} must be YYYY-MM-DD format, got: {date_str}")


def _validate_positive(value: int | float, field_name: str) -> None:
    """Validate that value is positive."""
    if value <= 0:
        raise ValueError(f"{field_name} must be positive, got: {value}")


def _validate_non_negative(value: int | float, field_name: str) -> None:
    """Validate that value is non-negative."""
    if value < 0:
        raise ValueError(f"{field_name} must be non-negative, got: {value}")


@dataclass(frozen=True, slots=True)
class Lot:
    """A single purchase or short-sale lot.

    Represents a batch of shares acquired at a specific cost.
    Used for FIFO cost basis tracking.

    Attributes:
        shares: Number of shares in this lot (must be positive)
        cost_per_share: Cost per share in TWD (must be non-negative)
        buy_date: Date of acquisition in YYYY-MM-DD format

    Example:
        >>> lot = Lot(shares=1000, cost_per_share=150.5, buy_date="2024-01-15")
        >>> lot.total_cost
        150500.0
    """

    shares: int
    cost_per_share: float
    buy_date: str

    def __post_init__(self) -> None:
        """Validate all fields after initialization."""
        _validate_positive(self.shares, "shares")
        _validate_non_negative(self.cost_per_share, "cost_per_share")
        _validate_date(self.buy_date, "buy_date")

    @property
    def total_cost(self) -> float:
        """Total cost of this lot."""
        return self.shares * self.cost_per_share

    def split(self, take_shares: int) -> tuple[Lot, Lot | None]:
        """Split this lot into two parts.

        Args:
            take_shares: Number of shares to take from this lot

        Returns:
            Tuple of (taken_lot, remaining_lot or None if fully consumed)

        Raises:
            ValueError: If take_shares exceeds available shares
        """
        if take_shares <= 0:
            raise ValueError(f"take_shares must be positive, got: {take_shares}")
        if take_shares > self.shares:
            raise ValueError(
                f"Cannot take {take_shares} shares from lot with {self.shares} shares"
            )

        taken = Lot(
            shares=take_shares,
            cost_per_share=self.cost_per_share,
            buy_date=self.buy_date,
        )

        remaining_shares = self.shares - take_shares
        if remaining_shares == 0:
            return taken, None

        remaining = Lot(
            shares=remaining_shares,
            cost_per_share=self.cost_per_share,
            buy_date=self.buy_date,
        )
        return taken, remaining


@dataclass(frozen=True, slots=True)
class ClosedTrade:
    """A completed trade with realized PNL.

    Represents a position that has been opened and closed,
    with all relevant pricing and timing information.

    Attributes:
        symbol: Stock symbol (e.g., "2330")
        broker: Broker code (e.g., "1440")
        shares: Number of shares traded (must be positive)
        buy_date: Date position was opened (YYYY-MM-DD)
        buy_price: Price per share when opened (TWD)
        sell_date: Date position was closed (YYYY-MM-DD)
        sell_price: Price per share when closed (TWD)
        trade_type: "long" or "short"

    Example:
        >>> trade = ClosedTrade(
        ...     symbol="2330", broker="1440", shares=1000,
        ...     buy_date="2024-01-15", buy_price=150.0,
        ...     sell_date="2024-02-20", sell_price=160.0,
        ...     trade_type="long"
        ... )
        >>> trade.realized_pnl
        10000.0
        >>> trade.holding_days
        36
    """

    symbol: str
    broker: str
    shares: int
    buy_date: str
    buy_price: float
    sell_date: str
    sell_price: float
    trade_type: TradeType

    def __post_init__(self) -> None:
        """Validate all fields after initialization."""
        if not self.symbol:
            raise ValueError("symbol cannot be empty")
        if not self.broker:
            raise ValueError("broker cannot be empty")
        _validate_positive(self.shares, "shares")
        _validate_date(self.buy_date, "buy_date")
        _validate_non_negative(self.buy_price, "buy_price")
        _validate_date(self.sell_date, "sell_date")
        _validate_non_negative(self.sell_price, "sell_price")
        if self.trade_type not in ("long", "short"):
            raise ValueError(f"trade_type must be 'long' or 'short', got: {self.trade_type}")

    @property
    def realized_pnl(self) -> float:
        """Calculate realized PNL for this trade.

        For long trades: (sell_price - buy_price) * shares
        For short trades: (buy_price - sell_price) * shares
        """
        if self.trade_type == "long":
            return self.shares * (self.sell_price - self.buy_price)
        else:  # short
            return self.shares * (self.buy_price - self.sell_price)

    @property
    def trade_return(self) -> float:
        """Calculate return percentage for this trade.

        Returns:
            Return as a decimal (e.g., 0.05 for 5%)
        """
        if self.buy_price == 0:
            return 0.0
        if self.trade_type == "long":
            return (self.sell_price - self.buy_price) / self.buy_price
        else:  # short
            return (self.buy_price - self.sell_price) / self.buy_price

    @property
    def holding_days(self) -> int:
        """Calculate number of days position was held."""
        buy_dt = datetime.strptime(self.buy_date, "%Y-%m-%d")
        sell_dt = datetime.strptime(self.sell_date, "%Y-%m-%d")
        return (sell_dt - buy_dt).days

    @property
    def is_profitable(self) -> bool:
        """Check if this trade was profitable."""
        return self.realized_pnl > 0

    @property
    def is_day_trade(self) -> bool:
        """Check if this was a day trade (same-day close)."""
        return self.buy_date == self.sell_date

    @property
    def trade_value(self) -> float:
        """Calculate trade value (shares * buy_price)."""
        return self.shares * self.buy_price


@dataclass(frozen=True, slots=True)
class BrokerSummary:
    """Summary statistics for a broker.

    Aggregated metrics for reporting and ranking.
    """

    broker: str
    name: str
    direction: Literal["做多", "做空", "中性"]
    total_pnl: float
    realized_pnl: float
    unrealized_pnl: float
    trading_days: int
    total_volume: int  # in shares (股)
    total_amount: float  # in TWD
    cumulative_net: int  # net position in shares (股)
    trade_count: int

    # Optional metrics (may be None if not calculated)
    exec_alpha: float | None = None
    timing_alpha: float | None = None
    lead_corr: float | None = None
    lag_corr: float | None = None
    p_value: float | None = None

    @property
    def timing_significance(self) -> str | None:
        """Determine timing significance based on p-value."""
        if self.p_value is None or self.timing_alpha is None:
            return None
        if self.p_value < 0.05:
            return "顯著正向" if self.timing_alpha > 0 else "顯著負向"
        return "不顯著"

    @property
    def style(self) -> str | None:
        """Determine trading style based on lag correlation."""
        if self.lag_corr is None:
            return None
        if self.lag_corr > 0.05:
            return "順勢"
        if self.lag_corr < -0.05:
            return "逆勢"
        return "中性"
