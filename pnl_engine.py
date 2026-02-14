"""PNL Engine: FIFO-based PNL calculation with parallel processing.

Processes daily_summary/{symbol}.parquet files and outputs:
1. pnl/{symbol}.parquet - Per-symbol PNL results
2. derived/broker_ranking.parquet - Pre-aggregated broker rankings

FIFO Logic:
- Each (symbol, broker) pair is an independent account
- Sells close long positions first, then open short
- Buys cover short positions first, then open long
- Tracks realized PNL (closed trades) and unrealized PNL (open positions)

Backtest Window:
- FIFO state accumulates from 2021-01-01
- Performance (PNL) only counts from 2023-01-01
"""

import sys
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import NamedTuple

import polars as pl

from pnl_analytics.infrastructure.config import DataPaths, AnalysisConfig, DEFAULT_PATHS, DEFAULT_CONFIG


# =============================================================================
# FIFO Data Structures
# =============================================================================

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
            # Close long positions (FIFO)
            while sell_shares > 0 and self.long_lots:
                lot = self.long_lots[0]
                take = min(sell_shares, lot.shares)
                realized_today += take * (avg_sell - lot.cost_per_share)
                lot.shares -= take
                sell_shares -= take
                if lot.shares == 0:
                    self.long_lots.pop(0)

            # Remaining sells open short
            if sell_shares > 0:
                self.short_lots.append(Lot(sell_shares, avg_sell, current_date))

        # Process buys: cover shorts first, then open longs
        if buy_shares > 0:
            # Cover short positions (FIFO)
            while buy_shares > 0 and self.short_lots:
                lot = self.short_lots[0]
                take = min(buy_shares, lot.shares)
                realized_today += take * (lot.cost_per_share - avg_buy)
                lot.shares -= take
                buy_shares -= take
                if lot.shares == 0:
                    self.short_lots.pop(0)

            # Remaining buys open long
            if buy_shares > 0:
                self.long_lots.append(Lot(buy_shares, avg_buy, current_date))

        # Calculate unrealized PNL
        unrealized = 0.0
        for lot in self.long_lots:
            unrealized += lot.shares * (close_price - lot.cost_per_share)
        for lot in self.short_lots:
            unrealized += lot.shares * (lot.cost_per_share - close_price)

        return realized_today, unrealized


# =============================================================================
# Per-Symbol Processing
# =============================================================================

class BrokerResult(NamedTuple):
    """Result for a single broker in a symbol."""
    broker: str
    total_pnl: float        # Realized (after backtest_start) + final unrealized
    realized_pnl: float     # Sum of realized after backtest_start
    unrealized_pnl: float   # Final unrealized
    total_buy: float
    total_sell: float
    win_count: int
    loss_count: int


def process_symbol(
    symbol: str,
    paths: DataPaths,
    price_lookup: dict[tuple[str, date], float],
    backtest_start: date,
) -> list[BrokerResult]:
    """Process a single symbol and return broker results.

    Args:
        symbol: Stock symbol
        paths: Data paths configuration
        price_lookup: {(symbol, date): close_price}
        backtest_start: Start date for performance calculation

    Returns:
        List of BrokerResult for each broker
    """
    trade_path = paths.symbol_trade_path(symbol)
    if not trade_path.exists():
        return []

    df = pl.read_parquet(trade_path)
    if len(df) == 0:
        return []

    # Get all dates and brokers
    dates = sorted(df["date"].unique().to_list())
    brokers = df["broker"].unique().to_list()

    results = []

    for broker in brokers:
        broker_df = df.filter(pl.col("broker") == broker).sort("date")
        trade_dict = {row["date"]: row for row in broker_df.iter_rows(named=True)}

        account = FIFOAccount()
        realized_after_start = 0.0
        total_buy = 0.0
        total_sell = 0.0
        win_count = 0
        loss_count = 0
        last_unrealized = 0.0
        last_price = 0.0

        for d in dates:
            # Get close price
            price = price_lookup.get((symbol, d), last_price)
            if price > 0:
                last_price = price

            if d in trade_dict:
                row = trade_dict[d]
                buy_shares = row["buy_shares"] or 0
                sell_shares = row["sell_shares"] or 0
                buy_amount = row["buy_amount"] or 0.0
                sell_amount = row["sell_amount"] or 0.0

                realized, unrealized = account.process_day(
                    buy_shares, sell_shares,
                    buy_amount, sell_amount,
                    price, d,
                )

                # Only count performance after backtest_start
                if d >= backtest_start:
                    realized_after_start += realized
                    total_buy += buy_amount
                    total_sell += sell_amount
                    if realized > 0:
                        win_count += 1
                    elif realized < 0:
                        loss_count += 1

                last_unrealized = unrealized
            else:
                # No trade, recalculate unrealized
                unrealized = 0.0
                for lot in account.long_lots:
                    unrealized += lot.shares * (price - lot.cost_per_share)
                for lot in account.short_lots:
                    unrealized += lot.shares * (lot.cost_per_share - price)
                last_unrealized = unrealized

        # Total PNL = realized (after start) + final unrealized
        total_pnl = realized_after_start + last_unrealized

        results.append(BrokerResult(
            broker=broker,
            total_pnl=total_pnl,
            realized_pnl=realized_after_start,
            unrealized_pnl=last_unrealized,
            total_buy=total_buy,
            total_sell=total_sell,
            win_count=win_count,
            loss_count=loss_count,
        ))

    return results


# =============================================================================
# Parallel Processing & Aggregation
# =============================================================================

def load_price_lookup(paths: DataPaths) -> dict[tuple[str, date], float]:
    """Load price data into lookup dict."""
    if not paths.close_prices.exists():
        print(f"Warning: Price file not found: {paths.close_prices}")
        return {}

    df = pl.read_parquet(paths.close_prices)
    lookup = {}
    for row in df.iter_rows(named=True):
        # Handle both string and date types
        d = row["date"]
        if isinstance(d, str):
            d = date.fromisoformat(d)
        lookup[(row["symbol_id"], d)] = row["close_price"]
    return lookup


def calculate_all_pnl(
    paths: DataPaths = DEFAULT_PATHS,
    config: AnalysisConfig = DEFAULT_CONFIG,
    workers: int | None = None,
) -> pl.DataFrame:
    """Calculate PNL for all symbols and generate broker ranking.

    Args:
        paths: Data paths configuration
        config: Analysis configuration
        workers: Number of parallel workers (default: config.parallel_workers)

    Returns:
        Broker ranking DataFrame
    """
    workers = workers or config.parallel_workers
    backtest_start = date.fromisoformat(config.backtest_start)

    # Ensure output directories exist
    paths.ensure_dirs()

    # Load prices
    print("Loading prices...")
    price_lookup = load_price_lookup(paths)
    print(f"  Loaded {len(price_lookup):,} price records")

    # Get symbols
    symbols = paths.list_symbols()
    if not symbols:
        print("Error: No symbols found in daily_summary/")
        return pl.DataFrame()

    print(f"Processing {len(symbols)} symbols with {workers} workers...")

    # Aggregate results by broker
    broker_totals: dict[str, dict] = defaultdict(lambda: {
        "total_pnl": 0.0,
        "realized_pnl": 0.0,
        "unrealized_pnl": 0.0,
        "total_buy": 0.0,
        "total_sell": 0.0,
        "win_count": 0,
        "loss_count": 0,
    })

    # Process symbols (can be parallelized, but keeping simple for now)
    # TODO: Add ProcessPoolExecutor for true parallelism
    for i, symbol in enumerate(symbols):
        if (i + 1) % 100 == 0 or i == 0:
            print(f"  {i + 1}/{len(symbols)}: {symbol}")

        results = process_symbol(symbol, paths, price_lookup, backtest_start)

        for r in results:
            b = broker_totals[r.broker]
            b["total_pnl"] += r.total_pnl
            b["realized_pnl"] += r.realized_pnl
            b["unrealized_pnl"] += r.unrealized_pnl
            b["total_buy"] += r.total_buy
            b["total_sell"] += r.total_sell
            b["win_count"] += r.win_count
            b["loss_count"] += r.loss_count

    # Build ranking DataFrame
    rows = []
    for broker, stats in broker_totals.items():
        win = stats["win_count"]
        loss = stats["loss_count"]
        total_trades = win + loss
        win_rate = win / total_trades if total_trades > 0 else 0.0

        rows.append({
            "broker": broker,
            "total_pnl": stats["total_pnl"],
            "realized_pnl": stats["realized_pnl"],
            "unrealized_pnl": stats["unrealized_pnl"],
            "total_buy_amount": stats["total_buy"],
            "total_sell_amount": stats["total_sell"],
            "total_amount": stats["total_buy"] + stats["total_sell"],
            "win_count": win,
            "loss_count": loss,
            "trade_count": total_trades,
            "win_rate": win_rate,
        })

    df = pl.DataFrame(rows)
    df = df.sort("total_pnl", descending=True)
    df = df.with_row_index("rank", offset=1)

    # Save ranking
    df.write_parquet(paths.broker_ranking)
    print(f"\nSaved: {paths.broker_ranking}")
    print(f"  {len(df)} brokers")
    print(f"  Total PNL range: {df['total_pnl'].min():,.0f} ~ {df['total_pnl'].max():,.0f}")

    return df


def main() -> None:
    """CLI entry point."""
    paths = DEFAULT_PATHS
    config = DEFAULT_CONFIG

    # Check for custom paths
    if len(sys.argv) > 1:
        paths = DataPaths(root=Path(sys.argv[1]))

    # Validate
    missing = paths.validate()
    if missing:
        print("Missing required files:")
        for p in missing:
            print(f"  - {p}")
        print("\nRun ETL and sync_prices first.")
        sys.exit(1)

    df = calculate_all_pnl(paths, config)

    if len(df) > 0:
        print("\nTop 10 brokers by PNL:")
        print(df.head(10).select(["rank", "broker", "total_pnl", "win_rate"]))


if __name__ == "__main__":
    main()
