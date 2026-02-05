"""Fusion & PNL Engine: Build 3D PNL Tensor with Realized/Unrealized separation.

Module C & D of High-Speed PNL Analytics System (dev.md).
- Module C: Dimension Mapping (string → int16 index)
- Module D: PNL calculation with FIFO cost basis tracking

Cost Basis Logic (FIFO - First In First Out):
- Each (symbol, broker) pair is treated as an independent account
- Tracks individual lots (purchase records) with their cost
- When selling, sells from oldest lots first
- Separates realized PNL (closed positions) from unrealized PNL (open positions)
"""

import json
import sys
from pathlib import Path
from dataclasses import dataclass, field
from collections import deque

import numpy as np
import polars as pl


@dataclass
class Lot:
    """A single purchase lot."""
    shares: int
    cost_per_share: float


@dataclass
class FIFOAccount:
    """Tracks position and lots for a (symbol, broker) pair using FIFO."""
    long_lots: deque = field(default_factory=deque)   # Lots for long position
    short_lots: deque = field(default_factory=deque)  # Lots for short position
    realized_pnl: float = 0.0

    @property
    def position(self) -> int:
        """Net position: positive=long, negative=short."""
        long_shares = sum(lot.shares for lot in self.long_lots)
        short_shares = sum(lot.shares for lot in self.short_lots)
        return long_shares - short_shares

    def _close_long(self, shares_to_sell: int, sell_price: float) -> float:
        """Close long position using FIFO. Returns realized PNL."""
        realized = 0.0
        remaining = shares_to_sell

        while remaining > 0 and self.long_lots:
            lot = self.long_lots[0]
            take = min(remaining, lot.shares)

            # Realized PNL = (sell price - cost) × shares
            realized += take * (sell_price - lot.cost_per_share)

            lot.shares -= take
            remaining -= take

            if lot.shares == 0:
                self.long_lots.popleft()

        return realized

    def _close_short(self, shares_to_cover: int, buy_price: float) -> float:
        """Close short position using FIFO. Returns realized PNL."""
        realized = 0.0
        remaining = shares_to_cover

        while remaining > 0 and self.short_lots:
            lot = self.short_lots[0]
            take = min(remaining, lot.shares)

            # Realized PNL = (short price - buy price) × shares
            realized += take * (lot.cost_per_share - buy_price)

            lot.shares -= take
            remaining -= take

            if lot.shares == 0:
                self.short_lots.popleft()

        return realized

    def process_day(
        self,
        buy_shares: int,
        sell_shares: int,
        buy_amount: float,
        sell_amount: float,
        close_price: float,
    ) -> tuple[float, float]:
        """Process a day's transactions using FIFO.

        Returns:
            (realized_pnl_today, unrealized_pnl): Today's realized and current unrealized PNL
        """
        realized_today = 0.0
        avg_buy_price = buy_amount / buy_shares if buy_shares > 0 else 0.0
        avg_sell_price = sell_amount / sell_shares if sell_shares > 0 else 0.0

        # Current position before today's trades
        current_long = sum(lot.shares for lot in self.long_lots)
        current_short = sum(lot.shares for lot in self.short_lots)

        # Process sells
        if sell_shares > 0:
            if current_long > 0:
                # Close long positions first (FIFO)
                shares_to_close = min(sell_shares, current_long)
                realized_today += self._close_long(shares_to_close, avg_sell_price)
                sell_shares -= shares_to_close

            # Remaining sells open/add to short position
            if sell_shares > 0:
                self.short_lots.append(Lot(shares=sell_shares, cost_per_share=avg_sell_price))

        # Process buys
        if buy_shares > 0:
            # Recalculate current short after sells
            current_short = sum(lot.shares for lot in self.short_lots)

            if current_short > 0:
                # Cover short positions first (FIFO)
                shares_to_cover = min(buy_shares, current_short)
                realized_today += self._close_short(shares_to_cover, avg_buy_price)
                buy_shares -= shares_to_cover

            # Remaining buys open/add to long position
            if buy_shares > 0:
                self.long_lots.append(Lot(shares=buy_shares, cost_per_share=avg_buy_price))

        # Update cumulative realized PNL
        self.realized_pnl += realized_today

        # Calculate unrealized PNL
        unrealized = 0.0

        # Long positions: profit if price > cost
        for lot in self.long_lots:
            unrealized += lot.shares * (close_price - lot.cost_per_share)

        # Short positions: profit if price < cost (short price)
        for lot in self.short_lots:
            unrealized += lot.shares * (lot.cost_per_share - close_price)

        return realized_today, unrealized


def build_dimension_maps(trade_df: pl.DataFrame) -> dict[str, dict[str, int]]:
    """Build dimension mapping tables (Module C)."""
    dates = sorted(trade_df["date"].unique().to_list())
    symbols = sorted(trade_df["symbol_id"].unique().to_list())
    brokers = sorted(trade_df["broker"].unique().to_list())

    maps = {
        "dates": {d: i for i, d in enumerate(dates)},
        "symbols": {s: i for i, s in enumerate(symbols)},
        "brokers": {b: i for i, b in enumerate(brokers)},
    }

    print(f"Dimension maps: {len(maps['symbols'])} symbols, {len(maps['dates'])} dates, {len(maps['brokers'])} brokers")
    return maps


def calculate_pnl_tensor(
    trade_path: Path,
    price_path: Path,
    output_dir: Path | None = None,
) -> tuple[np.ndarray, np.ndarray, dict]:
    """Calculate PNL tensors with realized/unrealized separation using FIFO (Module D).

    Args:
        trade_path: Path to daily_trade_summary.parquet
        price_path: Path to price_master.parquet
        output_dir: Output directory. Defaults to current directory.

    Returns:
        (realized_tensor, unrealized_tensor, dimension_maps)
    """
    if output_dir is None:
        output_dir = Path(".")

    # Load data
    trade_df = pl.read_parquet(trade_path)
    price_df = pl.read_parquet(price_path).rename({"coid": "symbol_id"})

    # Build dimension maps
    maps = build_dimension_maps(trade_df)
    n_symbols = len(maps["symbols"])
    n_dates = len(maps["dates"])
    n_brokers = len(maps["brokers"])

    print(f"Tensor shape: ({n_symbols}, {n_dates}, {n_brokers})")

    # Build price lookup: {(symbol, date): price}
    price_lookup = {
        (row["symbol_id"], row["date"]): row["close_price"]
        for row in price_df.iter_rows(named=True)
    }

    # Initialize tensors
    realized_tensor = np.zeros((n_symbols, n_dates, n_brokers), dtype=np.float32)
    unrealized_tensor = np.zeros((n_symbols, n_dates, n_brokers), dtype=np.float32)

    # Group trades by (symbol, broker) for efficient processing
    trade_df = trade_df.sort(["symbol_id", "broker", "date"])

    # Process each (symbol, broker) pair
    print("Calculating PNL with FIFO...")
    dates_list = sorted(maps["dates"].keys())

    for sym in maps["symbols"]:
        sym_idx = maps["symbols"][sym]
        sym_trades = trade_df.filter(pl.col("symbol_id") == sym)

        # Pre-fetch prices for this symbol
        sym_prices = {d: price_lookup.get((sym, d), 0.0) for d in dates_list}

        for broker in maps["brokers"]:
            broker_idx = maps["brokers"][broker]
            account = FIFOAccount()

            broker_trades = sym_trades.filter(pl.col("broker") == broker)
            trade_dict = {row["date"]: row for row in broker_trades.iter_rows(named=True)}

            # Track last known price for days without price data
            last_price = 0.0

            for date in dates_list:
                date_idx = maps["dates"][date]
                close_price = sym_prices.get(date, last_price)
                if close_price > 0:
                    last_price = close_price

                if date in trade_dict:
                    row = trade_dict[date]
                    realized, unrealized = account.process_day(
                        buy_shares=row["buy_shares"] or 0,
                        sell_shares=row["sell_shares"] or 0,
                        buy_amount=row["buy_amount"] or 0.0,
                        sell_amount=row["sell_amount"] or 0.0,
                        close_price=close_price,
                    )
                else:
                    # No trade today, just recalculate unrealized with current price
                    realized = 0.0
                    unrealized = 0.0

                    for lot in account.long_lots:
                        unrealized += lot.shares * (close_price - lot.cost_per_share)
                    for lot in account.short_lots:
                        unrealized += lot.shares * (lot.cost_per_share - close_price)

                realized_tensor[sym_idx, date_idx, broker_idx] = realized
                unrealized_tensor[sym_idx, date_idx, broker_idx] = unrealized

    # Save outputs
    np.save(output_dir / "realized_pnl.npy", realized_tensor)
    np.save(output_dir / "unrealized_pnl.npy", unrealized_tensor)
    with open(output_dir / "index_maps.json", "w") as f:
        json.dump(maps, f, indent=2, ensure_ascii=False)

    # Stats
    print(f"\nOutputs saved to {output_dir}/")
    print(f"  realized_pnl.npy:   {realized_tensor.nbytes / 1e6:.2f} MB")
    print(f"  unrealized_pnl.npy: {unrealized_tensor.nbytes / 1e6:.2f} MB")
    print(f"\nStats:")
    print(f"  Realized PNL range:   {realized_tensor.min():,.0f} ~ {realized_tensor.max():,.0f}")
    print(f"  Unrealized PNL range: {unrealized_tensor.min():,.0f} ~ {unrealized_tensor.max():,.0f}")

    return realized_tensor, unrealized_tensor, maps


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python pnl_engine.py <daily_trade_summary.parquet> <price_master.parquet> [output_dir]")
        sys.exit(1)

    output_dir = Path(sys.argv[3]) if len(sys.argv) > 3 else Path(".")
    calculate_pnl_tensor(Path(sys.argv[1]), Path(sys.argv[2]), output_dir)
