"""ETL: broker_tx.parquet → daily_summary/{symbol}.parquet

Transforms supplier broker transaction data into per-symbol daily summaries.

Input:
    broker_tx.parquet (10GB, 2.08B rows)
    - symbol_id, date, broker, broker_name, price, buy, sell

Output:
    daily_summary/{symbol}.parquet
    - broker (Categorical), date (Date), buy_shares, sell_shares, buy_amount, sell_amount
    - Sorted by (broker, date) for FIFO optimization

Notes:
    - Skips proprietary traders (price="-") as they lack price data
    - Batched scan: groups symbols into batches to reduce scan count
      (6 scans instead of 2,839, while staying within memory limits)
"""

import sys
from pathlib import Path

import polars as pl

# Number of symbols per batch scan.
# Higher = fewer scans but more memory. 500 symbols ≈ ~1 GB per batch.
BATCH_SIZE = 500


def transform_broker_tx(
    input_path: Path,
    output_dir: Path,
    skip_proprietary: bool = True,
) -> dict[str, int]:
    """Transform broker_tx.parquet to per-symbol daily summaries.

    Batched approach: scan once to get symbols, then process in batches
    of BATCH_SIZE symbols per scan. Reduces 2,839 scans to ~6 scans.

    Args:
        input_path: Path to broker_tx.parquet
        output_dir: Output directory for daily_summary/*.parquet
        skip_proprietary: Skip rows with price="-" (default True)

    Returns:
        Dict of {symbol: row_count} for each output file
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Build lazy query: filter → parse → calculate amounts
    lf = pl.scan_parquet(input_path)

    if skip_proprietary:
        lf = lf.filter(pl.col("price") != "-")

    lf = lf.with_columns([
        pl.col("price").str.replace_all(",", "").cast(pl.Float32).alias("price"),
        pl.col("date").dt.date().alias("date"),
    ])

    lf = lf.with_columns([
        (pl.col("buy") * pl.col("price")).alias("buy_amount"),
        (pl.col("sell") * pl.col("price")).alias("sell_amount"),
    ])

    # Scan 1: get unique symbols (lightweight streaming scan)
    print("Scanning symbols...")
    symbols = (
        lf.select("symbol_id")
        .unique()
        .collect(engine="streaming")
    )["symbol_id"].to_list()
    print(f"  Found {len(symbols)} symbols")

    # Process in batches
    total_batches = (len(symbols) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"Processing in {total_batches} batches ({BATCH_SIZE} symbols/batch)...")

    results = {}
    for batch_idx in range(0, len(symbols), BATCH_SIZE):
        batch = symbols[batch_idx : batch_idx + BATCH_SIZE]
        batch_num = batch_idx // BATCH_SIZE + 1
        print(f"  Batch {batch_num}/{total_batches}: {len(batch)} symbols")

        # One scan per batch: filter to batch symbols, aggregate
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

        # Partition batch result by symbol and write
        for symbol_df in batch_df.partition_by("symbol_id", maintain_order=False):
            symbol = symbol_df["symbol_id"][0]
            symbol_df = (
                symbol_df
                .drop("symbol_id")
                .sort(["broker", "date"])
                .with_columns(pl.col("broker").cast(pl.Categorical))
            )

            output_path = output_dir / f"{symbol}.parquet"
            symbol_df.write_parquet(output_path)
            results[symbol] = len(symbol_df)

    return results


def main() -> None:
    """CLI entry point."""
    if len(sys.argv) < 2:
        print("Usage: python etl.py <broker_tx.parquet> [output_dir]")
        print("Example: python etl.py data/broker_tx.parquet data/daily_summary")
        sys.exit(1)

    input_path = Path(sys.argv[1])
    output_dir = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("data/daily_summary")

    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}")
        sys.exit(1)

    print(f"Input:  {input_path}")
    print(f"Output: {output_dir}/")
    print()

    results = transform_broker_tx(input_path, output_dir)

    # Summary
    total_rows = sum(results.values())
    total_files = len(results)
    print()
    print(f"Done! Created {total_files} files with {total_rows:,} total rows")
    print(f"Output: {output_dir}/")


if __name__ == "__main__":
    main()
