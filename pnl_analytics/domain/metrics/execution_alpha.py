"""Execution Alpha: Measure trade execution quality.

Execution Alpha compares actual trade prices to market close prices
during the same holding period.

Formula:
- Long trade:
    trade_return = (sell_price - buy_price) / buy_price
    benchmark_return = (close_at_sell - close_at_buy) / close_at_buy
    alpha = trade_return - benchmark_return

- Short trade:
    trade_return = (buy_price - sell_price) / buy_price
    benchmark_return = (close_at_buy - close_at_sell) / close_at_buy
    alpha = trade_return - benchmark_return

Interpretation:
- Positive alpha: Execution prices better than market close
- Negative alpha: Execution prices worse than market close
- Zero alpha: Traded at close prices

Typical ranges:
- General brokers: -2% to +2%
- Large foreign (UBS, Goldman): near 0% (positive/negative cancel out)
- Excellent execution: +0.5% to +1.5%
"""

from dataclasses import dataclass
from typing import Sequence

import polars as pl

# =============================================================================
# Data Classes
# =============================================================================

@dataclass(frozen=True, slots=True)
class TradeAlpha:
    """Alpha calculation result for a single trade.

    Attributes:
        trade_return: Return based on execution prices
        benchmark_return: Return based on close prices
        alpha: Difference (trade_return - benchmark_return)
        trade_value: Notional value (shares × buy_price)
        alpha_dollars: Alpha in dollar terms (alpha × trade_value)
    """
    trade_return: float
    benchmark_return: float
    alpha: float
    trade_value: float
    alpha_dollars: float

    @property
    def is_positive(self) -> bool:
        """Check if execution beat the market."""
        return self.alpha > 0


@dataclass(frozen=True, slots=True)
class BrokerExecutionAlpha:
    """Aggregated execution alpha for a broker.

    Attributes:
        broker: Broker code
        weighted_alpha: Value-weighted average alpha
        total_alpha_dollars: Sum of alpha in dollars
        total_trade_value: Sum of trade values
        trade_count: Number of closed trades
        long_count: Number of long trades
        short_count: Number of short trades
    """
    broker: str
    weighted_alpha: float
    total_alpha_dollars: float
    total_trade_value: float
    trade_count: int
    long_count: int
    short_count: int

    @property
    def is_positive(self) -> bool:
        """Check if overall execution beat the market."""
        return self.weighted_alpha > 0

    @property
    def alpha_percent(self) -> float:
        """Alpha as percentage."""
        return self.weighted_alpha * 100


# =============================================================================
# Single Trade Calculation
# =============================================================================

def calculate_trade_alpha(
    trade_type: str,
    buy_price: float,
    sell_price: float,
    close_at_buy: float,
    close_at_sell: float,
    shares: int,
) -> TradeAlpha | None:
    """Calculate execution alpha for a single trade.

    Args:
        trade_type: "long" or "short"
        buy_price: Execution price when opening position
        sell_price: Execution price when closing position
        close_at_buy: Market close price on buy date
        close_at_sell: Market close price on sell date
        shares: Number of shares traded

    Returns:
        TradeAlpha with all metrics, or None if prices are invalid

    Example:
        >>> alpha = calculate_trade_alpha(
        ...     "long", buy_price=100, sell_price=110,
        ...     close_at_buy=101, close_at_sell=108, shares=1000
        ... )
        >>> alpha.alpha  # (10% trade return) - (6.93% benchmark) = +3.07%
        0.0307...
    """
    # Validate inputs
    if buy_price <= 0 or close_at_buy <= 0 or close_at_sell <= 0:
        return None
    if shares <= 0:
        return None

    # Normalize trade_type to lowercase string
    trade_type_str = str(trade_type).lower()

    # Calculate returns based on trade type
    if trade_type_str == "long":
        trade_return = (sell_price - buy_price) / buy_price
        benchmark_return = (close_at_sell - close_at_buy) / close_at_buy
    else:  # SHORT
        trade_return = (buy_price - sell_price) / buy_price
        benchmark_return = (close_at_buy - close_at_sell) / close_at_buy

    alpha = trade_return - benchmark_return
    trade_value = shares * buy_price
    alpha_dollars = alpha * trade_value

    return TradeAlpha(
        trade_return=trade_return,
        benchmark_return=benchmark_return,
        alpha=alpha,
        trade_value=trade_value,
        alpha_dollars=alpha_dollars,
    )


# =============================================================================
# DataFrame-based Calculation
# =============================================================================

def add_alpha_columns(
    closed_trades: pl.DataFrame,
    price_dict: dict[str, float],
) -> pl.DataFrame:
    """Add execution alpha columns to closed trades DataFrame.

    Adds columns:
    - close_at_buy: Close price on buy date
    - close_at_sell: Close price on sell date
    - trade_return: Return from execution prices
    - benchmark_return: Return from close prices
    - trade_value: Notional value
    - alpha: trade_return - benchmark_return
    - alpha_dollars: alpha × trade_value

    Args:
        closed_trades: DataFrame with columns: trade_type, buy_price,
                      sell_price, buy_date, sell_date, shares
        price_dict: Dict mapping date string to close price

    Returns:
        DataFrame with additional alpha columns
    """
    # Add close prices
    result = closed_trades.with_columns([
        pl.col("buy_date").map_elements(
            lambda d: price_dict.get(d, 0.0), return_dtype=pl.Float64
        ).alias("close_at_buy"),
        pl.col("sell_date").map_elements(
            lambda d: price_dict.get(d, 0.0), return_dtype=pl.Float64
        ).alias("close_at_sell"),
    ])

    # Calculate returns and alpha
    result = result.with_columns([
        # Trade return (based on execution prices)
        pl.when(pl.col("trade_type") == "long")
        .then((pl.col("sell_price") - pl.col("buy_price")) / pl.col("buy_price"))
        .otherwise((pl.col("buy_price") - pl.col("sell_price")) / pl.col("buy_price"))
        .alias("trade_return"),

        # Benchmark return (based on close prices)
        pl.when(pl.col("trade_type") == "long")
        .then((pl.col("close_at_sell") - pl.col("close_at_buy")) / pl.col("close_at_buy"))
        .otherwise((pl.col("close_at_buy") - pl.col("close_at_sell")) / pl.col("close_at_buy"))
        .alias("benchmark_return"),

        # Trade value
        (pl.col("shares") * pl.col("buy_price")).alias("trade_value"),
    ]).with_columns([
        # Alpha
        (pl.col("trade_return") - pl.col("benchmark_return")).alias("alpha"),
    ]).with_columns([
        # Alpha dollars
        (pl.col("alpha") * pl.col("trade_value")).alias("alpha_dollars"),
    ])

    return result


def calculate_broker_alpha(
    closed_trades_with_alpha: pl.DataFrame,
    broker: str,
) -> BrokerExecutionAlpha | None:
    """Calculate aggregated execution alpha for a broker.

    Args:
        closed_trades_with_alpha: DataFrame with alpha columns
                                  (from add_alpha_columns)
        broker: Broker code to analyze

    Returns:
        BrokerExecutionAlpha or None if no valid trades
    """
    # Filter for this broker and valid data
    broker_trades = closed_trades_with_alpha.filter(
        (pl.col("broker") == broker) &
        (pl.col("close_at_buy") > 0) &
        (pl.col("close_at_sell") > 0)
    )

    if len(broker_trades) == 0:
        return None

    # Aggregate
    total_trade_value = broker_trades["trade_value"].sum()
    total_alpha_dollars = broker_trades["alpha_dollars"].sum()
    trade_count = len(broker_trades)
    long_count = broker_trades.filter(pl.col("trade_type") == "long").height
    short_count = trade_count - long_count

    if total_trade_value <= 0:
        return None

    weighted_alpha = total_alpha_dollars / total_trade_value

    return BrokerExecutionAlpha(
        broker=broker,
        weighted_alpha=weighted_alpha,
        total_alpha_dollars=total_alpha_dollars,
        total_trade_value=total_trade_value,
        trade_count=trade_count,
        long_count=long_count,
        short_count=short_count,
    )


def calculate_all_broker_alphas(
    closed_trades: pl.DataFrame,
    price_dict: dict[str, float],
) -> pl.DataFrame:
    """Calculate execution alpha for all brokers.

    Args:
        closed_trades: DataFrame with closed trade records
        price_dict: Dict mapping date to close price

    Returns:
        DataFrame with broker-level alpha metrics
    """
    # Add alpha columns
    with_alpha = add_alpha_columns(closed_trades, price_dict)

    # Filter valid trades
    valid = with_alpha.filter(
        (pl.col("close_at_buy") > 0) & (pl.col("close_at_sell") > 0)
    )

    # Aggregate by broker
    result = (
        valid
        .group_by("broker")
        .agg([
            pl.col("trade_value").sum().alias("total_trade_value"),
            pl.col("alpha_dollars").sum().alias("total_alpha_dollars"),
            pl.len().alias("trade_count"),
            (pl.col("trade_type") == "long").sum().alias("long_count"),
            (pl.col("trade_type") == "short").sum().alias("short_count"),
        ])
        .with_columns([
            (pl.col("total_alpha_dollars") / pl.col("total_trade_value"))
            .alias("weighted_alpha"),
        ])
        .sort("weighted_alpha", descending=True)
    )

    return result
