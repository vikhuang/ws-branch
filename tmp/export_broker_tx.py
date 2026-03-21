"""Export raw broker_tx (分價量) for selected stocks as CSV.

Usage:
    uv run python tmp/export_broker_tx.py 20260316 20260317
    uv run python tmp/export_broker_tx.py 20260316 --symbols 2308,2313

Output: tmp/price_volume_{date}.csv per date
"""

import argparse
from pathlib import Path

import polars as pl

BROKER_TX_DIR = Path.home() / "r20/data/fugle/broker_tx"
OUTPUT_DIR = Path("tmp")

DEFAULT_STOCKS = [
    "2308", "2313", "2345", "2360", "2383",
    "3017", "6285", "8046", "8358",
]


def export_date(date_str: str, symbols: list[str]) -> Path | None:
    """Export one date's broker_tx for given symbols."""
    src = BROKER_TX_DIR / f"broker_tx_{date_str}.parquet"
    if not src.exists():
        print(f"  {date_str}: {src.name} not found, skipping")
        return None

    df = pl.read_parquet(src).filter(pl.col("symbol_id").is_in(symbols))
    if len(df) == 0:
        print(f"  {date_str}: no data for {symbols}")
        return None

    out = df.select(["symbol_id", "broker", "broker_name", "price", "buy", "sell"])
    out_path = OUTPUT_DIR / f"price_volume_{date_str}.csv"
    out.write_csv(out_path)
    print(f"  {date_str}: {len(out):,} rows → {out_path}")
    return out_path


def main():
    parser = argparse.ArgumentParser(description="Export broker_tx (分價量) to CSV")
    parser.add_argument("dates", nargs="+", help="Date(s) in YYYYMMDD format")
    parser.add_argument(
        "--symbols", default=",".join(DEFAULT_STOCKS),
        help=f"Comma-separated symbols (default: {','.join(DEFAULT_STOCKS)})",
    )
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",")]
    print(f"Symbols: {symbols}")
    print()

    for date_str in args.dates:
        export_date(date_str, symbols)


if __name__ == "__main__":
    main()
