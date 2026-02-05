"""Fusion & PNL Engine: Build 3D PNL Tensor with Realized/Unrealized separation.

Module C & D of High-Speed PNL Analytics System (dev.md).
- Module C: Dimension Mapping (string → int16 index)
- Module D: PNL calculation with proper cost basis tracking

Cost Basis Logic:
- Each (symbol, broker) pair is treated as an independent account
- Tracks position (net shares) and average cost
- Separates realized PNL (closed positions) from unrealized PNL (open positions)
"""

import json
import sys
from pathlib import Path
from dataclasses import dataclass

import numpy as np
import polars as pl


@dataclass
class Account:
    """Tracks position and cost basis for a (symbol, broker) pair."""
    position: int = 0           # positive=long, negative=short
    cost_basis: float = 0.0     # total cost for long, total proceeds for short
    realized_pnl: float = 0.0   # cumulative realized PNL

    @property
    def avg_cost(self) -> float:
        """Average cost per share (for long) or avg short price (for short)."""
        if self.position == 0:
            return 0.0
        return abs(self.cost_basis / self.position)

    def process_day(
        self,
        buy_shares: int,
        sell_shares: int,
        buy_amount: float,
        sell_amount: float,
        close_price: float,
    ) -> tuple[float, float]:
        """Process a day's transactions and return (realized_pnl_today, unrealized_pnl).

        Returns:
            (realized_pnl_today, unrealized_pnl): Today's realized and current unrealized PNL
        """
        realized_today = 0.0

        # Process sells first (reduces long or increases short)
        if sell_shares > 0:
            if self.position > 0:
                # Selling from long position
                shares_to_close = min(sell_shares, self.position)
                avg_cost = self.avg_cost
                realized_today += shares_to_close * (sell_amount / sell_shares - avg_cost)

                # Reduce position and cost basis proportionally
                self.cost_basis -= shares_to_close * avg_cost
                self.position -= shares_to_close

                # Remaining sells open short position
                remaining_sells = sell_shares - shares_to_close
                if remaining_sells > 0:
                    short_price = sell_amount / sell_shares
                    self.position -= remaining_sells
                    self.cost_basis -= remaining_sells * short_price  # negative cost = proceeds
            else:
                # Adding to short position (or opening new short)
                avg_sell_price = sell_amount / sell_shares
                self.position -= sell_shares
                self.cost_basis -= sell_shares * avg_sell_price

        # Process buys (reduces short or increases long)
        if buy_shares > 0:
            if self.position < 0:
                # Buying to cover short position
                shares_to_cover = min(buy_shares, abs(self.position))
                avg_short_price = self.avg_cost  # avg price we shorted at
                avg_buy_price = buy_amount / buy_shares
                realized_today += shares_to_cover * (avg_short_price - avg_buy_price)

                # Reduce short position
                self.cost_basis += shares_to_cover * avg_short_price
                self.position += shares_to_cover

                # Remaining buys open long position
                remaining_buys = buy_shares - shares_to_cover
                if remaining_buys > 0:
                    self.position += remaining_buys
                    self.cost_basis += remaining_buys * (buy_amount / buy_shares)
            else:
                # Adding to long position (or opening new long)
                avg_buy_price = buy_amount / buy_shares
                self.position += buy_shares
                self.cost_basis += buy_shares * avg_buy_price

        # Update cumulative realized PNL
        self.realized_pnl += realized_today

        # Calculate unrealized PNL
        if self.position > 0:
            # Long: profit if price > avg_cost
            unrealized = self.position * (close_price - self.avg_cost)
        elif self.position < 0:
            # Short: profit if price < avg_short_price
            unrealized = abs(self.position) * (self.avg_cost - close_price)
        else:
            unrealized = 0.0

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
    """Calculate PNL tensors with realized/unrealized separation (Module D).

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

    # Join with prices
    trade_df = trade_df.join(price_df, on=["symbol_id", "date"], how="left")
    trade_df = trade_df.sort(["symbol_id", "date"])
    trade_df = trade_df.with_columns(
        pl.col("close_price").forward_fill().over("symbol_id").fill_null(0.0)
    )

    # Initialize tensors
    realized_tensor = np.zeros((n_symbols, n_dates, n_brokers), dtype=np.float32)
    unrealized_tensor = np.zeros((n_symbols, n_dates, n_brokers), dtype=np.float32)

    # Process each (symbol, broker) pair
    print("Calculating PNL...")
    accounts: dict[tuple[str, str], Account] = {}
    dates_list = sorted(maps["dates"].keys())

    for sym in maps["symbols"]:
        sym_idx = maps["symbols"][sym]
        sym_data = trade_df.filter(pl.col("symbol_id") == sym)

        for broker in maps["brokers"]:
            broker_idx = maps["brokers"][broker]
            key = (sym, broker)
            accounts[key] = Account()

            broker_data = sym_data.filter(pl.col("broker") == broker)
            broker_dict = {row["date"]: row for row in broker_data.iter_rows(named=True)}

            for date in dates_list:
                date_idx = maps["dates"][date]

                if date in broker_dict:
                    row = broker_dict[date]
                    realized, unrealized = accounts[key].process_day(
                        buy_shares=row["buy_shares"] or 0,
                        sell_shares=row["sell_shares"] or 0,
                        buy_amount=row["buy_amount"] or 0.0,
                        sell_amount=row["sell_amount"] or 0.0,
                        close_price=row["close_price"] or 0.0,
                    )
                else:
                    # No trade today, but unrealized PNL may change with price
                    # Get close price for this date
                    price_row = price_df.filter(
                        (pl.col("symbol_id") == sym) & (pl.col("date") == date)
                    )
                    if len(price_row) > 0:
                        close_price = price_row["close_price"][0]
                    else:
                        close_price = 0.0

                    realized = 0.0
                    acc = accounts[key]
                    if acc.position > 0:
                        unrealized = acc.position * (close_price - acc.avg_cost)
                    elif acc.position < 0:
                        unrealized = abs(acc.position) * (acc.avg_cost - close_price)
                    else:
                        unrealized = 0.0

                realized_tensor[sym_idx, date_idx, broker_idx] = realized
                unrealized_tensor[sym_idx, date_idx, broker_idx] = unrealized

    # Save outputs
    np.save(output_dir / "realized_pnl.npy", realized_tensor)
    np.save(output_dir / "unrealized_pnl.npy", unrealized_tensor)
    with open(output_dir / "index_maps.json", "w") as f:
        json.dump(maps, f, indent=2, ensure_ascii=False)

    # Stats
    total_tensor = realized_tensor + unrealized_tensor
    print(f"\nOutputs saved to {output_dir}/")
    print(f"  realized_pnl.npy:   {realized_tensor.nbytes / 1e6:.2f} MB")
    print(f"  unrealized_pnl.npy: {unrealized_tensor.nbytes / 1e6:.2f} MB")
    print(f"\nStats:")
    print(f"  Realized PNL range:   {realized_tensor.min():,.0f} ~ {realized_tensor.max():,.0f}")
    print(f"  Unrealized PNL range: {unrealized_tensor.min():,.0f} ~ {unrealized_tensor.max():,.0f}")
    print(f"  Total PNL range:      {total_tensor.min():,.0f} ~ {total_tensor.max():,.0f}")

    return realized_tensor, unrealized_tensor, maps


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python pnl_engine.py <daily_trade_summary.parquet> <price_master.parquet> [output_dir]")
        sys.exit(1)

    output_dir = Path(sys.argv[3]) if len(sys.argv) > 3 else Path(".")
    calculate_pnl_tensor(Path(sys.argv[1]), Path(sys.argv[2]), output_dir)
