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
    - Uses streaming to handle 10GB file with ~2GB memory
"""

import sys
from pathlib import Path

import polars as pl


def transform_broker_tx(
    input_path: Path,
    output_dir: Path,
    skip_proprietary: bool = True,
) -> dict[str, int]:
    """Transform broker_tx.parquet to per-symbol daily summaries.

    Args:
        input_path: Path to broker_tx.parquet
        output_dir: Output directory for daily_summary/*.parquet
        skip_proprietary: Skip rows with price="-" (default True)

    Returns:
        Dict of {symbol: row_count} for each output file
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Scan parquet (lazy, no memory load)
    lf = pl.scan_parquet(input_path)

    # Filter out proprietary traders if requested
    if skip_proprietary:
        lf = lf.filter(pl.col("price") != "-")

    # Parse price and convert types
    lf = lf.with_columns([
        # Parse price: "1,170.00" → 1170.0
        pl.col("price").str.replace_all(",", "").cast(pl.Float32).alias("price"),
        # Convert date to Date type (from Datetime)
        pl.col("date").dt.date().alias("date"),
    ])

    # Calculate amounts
    lf = lf.with_columns([
        (pl.col("buy") * pl.col("price")).alias("buy_amount"),
        (pl.col("sell") * pl.col("price")).alias("sell_amount"),
    ])

    # Get unique symbols first (streaming collect)
    print("Scanning symbols...")
    symbols = (
        lf.select("symbol_id")
        .unique()
        .collect(engine="streaming")
    )["symbol_id"].to_list()

    print(f"Found {len(symbols)} symbols")

    # Process each symbol
    results = {}
    total = len(symbols)

    for i, symbol in enumerate(symbols):
        if (i + 1) % 100 == 0 or i == 0:
            print(f"Processing {i + 1}/{total}: {symbol}")

        # Filter and aggregate for this symbol
        symbol_df = (
            lf.filter(pl.col("symbol_id") == symbol)
            .group_by(["broker", "date"])
            .agg([
                pl.col("buy").sum().cast(pl.Int32).alias("buy_shares"),
                pl.col("sell").sum().cast(pl.Int32).alias("sell_shares"),
                pl.col("buy_amount").sum().cast(pl.Float32).alias("buy_amount"),
                pl.col("sell_amount").sum().cast(pl.Float32).alias("sell_amount"),
            ])
            .sort(["broker", "date"])
            .with_columns([
                pl.col("broker").cast(pl.Categorical),
            ])
            .collect(engine="streaming")
        )

        # Write output
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
