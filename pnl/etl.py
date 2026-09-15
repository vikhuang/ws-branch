"""ETL: broker_tx → daily_summary/{symbol}.parquet

Transforms broker transaction data into per-symbol daily summaries.

Input:
    ~/r20/data/fugle/broker_tx/ (per-day parquet files, managed by ws-admin)
    - symbol_id, date, broker, broker_name, price, buy, sell

Output:
    daily_summary/{symbol}.parquet
    - broker (Categorical), date (Date), buy_shares, sell_shares, buy_amount, sell_amount
    - Sorted by (broker, date) for FIFO optimization

Modes:
    Full:        python etl.py           (rebuild all)
    Incremental: python etl.py --incr    (only new dates)
"""

import argparse
import sys
from datetime import date
from pathlib import Path

import polars as pl

# Number of symbols per batch scan.
# Higher = fewer scans but more memory. 500 symbols ≈ ~1 GB per batch.
BATCH_SIZE = 500

DEFAULT_INPUT = Path.home() / "r20" / "data" / "fugle" / "broker_tx"
DEFAULT_OUTPUT = Path("data/daily_summary")


def _build_lazy_query(
    input_path: Path,
    skip_proprietary: bool = True,
) -> pl.LazyFrame:
    """Build the common lazy query for broker_tx processing."""
    if input_path.is_dir():
        lf = pl.scan_parquet(input_path / "*.parquet")
    else:
        lf = pl.scan_parquet(input_path)

    if skip_proprietary:
        lf = lf.filter(pl.col("price") != "-")

    lf = lf.with_columns([
        pl.col("price").str.replace_all(",", "").cast(pl.Float32).alias("price"),
        pl.col("date").dt.convert_time_zone("Asia/Taipei").dt.date().alias("date"),
    ])

    lf = lf.with_columns([
        (pl.col("buy") * pl.col("price")).alias("buy_amount"),
        (pl.col("sell") * pl.col("price")).alias("sell_amount"),
    ])

    return lf


def _process_and_write(
    lf: pl.LazyFrame,
    symbols: list[str],
    output_dir: Path,
    existing: dict[str, pl.DataFrame] | None = None,
) -> dict[str, int]:
    """Process symbols in batches, optionally appending to existing data."""
    output_dir.mkdir(parents=True, exist_ok=True)

    total_batches = (len(symbols) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"Processing in {total_batches} batches ({BATCH_SIZE} symbols/batch)...")

    results = {}
    for batch_idx in range(0, len(symbols), BATCH_SIZE):
        batch = symbols[batch_idx : batch_idx + BATCH_SIZE]
        batch_num = batch_idx // BATCH_SIZE + 1
        print(f"  Batch {batch_num}/{total_batches}: {len(batch)} symbols")

        batch_df = (
            lf.filter(pl.col("symbol_id").is_in(batch))
            .group_by(["symbol_id", "broker", "date"])
            .agg([
                pl.col("buy").sum().cast(pl.Int32).alias("buy_shares"),
                pl.col("sell").sum().cast(pl.Int32).alias("sell_shares"),
                pl.col("buy_amount").sum().cast(pl.Float32).alias("buy_amount"),
                pl.col("sell_amount").sum().cast(pl.Float32).alias("sell_amount"),
            ])
            .collect(engine="streaming")
        )

        for symbol_df in batch_df.partition_by("symbol_id", maintain_order=False):
            symbol = symbol_df["symbol_id"][0]
            symbol_df = symbol_df.drop("symbol_id")

            # Append to existing data if incremental
            if existing and symbol in existing:
                symbol_df = pl.concat([existing[symbol], symbol_df])

            symbol_df = (
                symbol_df
                .sort(["broker", "date"])
                .with_columns(pl.col("broker").cast(pl.Categorical))
            )

            output_path = output_dir / f"{symbol}.parquet"
            symbol_df.write_parquet(output_path)
            results[symbol] = len(symbol_df)

    return results


def transform_full(input_path: Path, output_dir: Path) -> dict[str, int]:
    """Full rebuild: process all broker_tx files."""
    lf = _build_lazy_query(input_path)

    print("Scanning symbols...")
    symbols = (
        lf.select("symbol_id").unique().collect(engine="streaming")
    )["symbol_id"].to_list()
    print(f"  Found {len(symbols)} symbols")

    return _process_and_write(lf, symbols, output_dir)


def transform_incremental(input_path: Path, output_dir: Path) -> dict[str, int]:
    """Incremental: only process new dates not yet in daily_summary."""
    if not input_path.is_dir():
        print("Error: Incremental mode requires a directory input.")
        sys.exit(1)

    # Find max date in existing daily_summary (sample a few files)
    existing_files = sorted(output_dir.glob("*.parquet"))
    if not existing_files:
        print("No existing daily_summary found, running full ETL.")
        return transform_full(input_path, output_dir)

    # Sample 3 files to find max date
    sample_files = [existing_files[0], existing_files[len(existing_files) // 2], existing_files[-1]]
    max_date = date(2000, 1, 1)
    for f in sample_files:
        d = pl.read_parquet(f, columns=["date"])["date"].max()
        if d > max_date:
            max_date = d

    print(f"  Existing data up to: {max_date}")

    # Find new broker_tx files
    all_tx_files = sorted(input_path.glob("broker_tx_*.parquet"))
    new_files = []
    for f in all_tx_files:
        # Parse date from filename: broker_tx_YYYYMMDD.parquet
        file_date_str = f.stem.replace("broker_tx_", "")
        file_date = date(int(file_date_str[:4]), int(file_date_str[4:6]), int(file_date_str[6:8]))
        # File date is in Taiwan time, but actual content date may differ by timezone
        # Include files from max_date onwards to be safe (timezone edge cases)
        if file_date > max_date:
            new_files.append(f)

    if not new_files:
        print("  No new broker_tx files found. Data is up to date.")
        return {}

    print(f"  New files: {len(new_files)} ({new_files[0].name} ~ {new_files[-1].name})")

    # Build lazy query from only new files
    lf = _build_lazy_query(input_path)
    # Filter to only dates after max_date
    lf = lf.filter(pl.col("date") > max_date)

    # Get symbols in new data
    print("Scanning new symbols...")
    symbols = (
        lf.select("symbol_id").unique().collect(engine="streaming")
    )["symbol_id"].to_list()
    print(f"  Found {len(symbols)} symbols with new data")

    if not symbols:
        print("  No new data after date filter.")
        return {}

    # Load existing data for these symbols
    print("Loading existing data for merge...")
    existing = {}
    for sym in symbols:
        path = output_dir / f"{sym}.parquet"
        if path.exists():
            existing[sym] = pl.read_parquet(path).cast({"broker": pl.Utf8})

    return _process_and_write(lf, symbols, output_dir, existing=existing)


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="ETL: broker_tx → daily_summary")
    parser.add_argument("input", nargs="?", help="broker_tx path (default: ~/r20/data/fugle/broker_tx)")
    parser.add_argument("output", nargs="?", help="Output directory (default: data/daily_summary)")
    parser.add_argument("--incr", action="store_true", help="Incremental mode: only process new dates")
    args = parser.parse_args()

    input_path = Path(args.input) if args.input else DEFAULT_INPUT
    output_dir = Path(args.output) if args.output else DEFAULT_OUTPUT

    if not input_path.exists():
        print(f"Error: Input not found: {input_path}")
        print(f"Usage: python etl.py [input] [output] [--incr]")
        sys.exit(1)

    mode = "incremental" if args.incr else "full"
    print(f"Input:  {input_path}")
    print(f"Output: {output_dir}/")
    print(f"Mode:   {mode}")
    print()

    if args.incr:
        results = transform_incremental(input_path, output_dir)
    else:
        results = transform_full(input_path, output_dir)

    # Summary
    if results:
        total_rows = sum(results.values())
        total_files = len(results)
        print()
        print(f"Done! {'Updated' if args.incr else 'Created'} {total_files} files with {total_rows:,} total rows")
        print(f"Output: {output_dir}/")
    elif args.incr:
        print("\nNothing to update.")


if __name__ == "__main__":
    main()
