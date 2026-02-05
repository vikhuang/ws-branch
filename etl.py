"""ETL: JSON → Parquet with daily trade summary aggregation.

Module A of High-Speed PNL Analytics System (dev.md).
Transforms nested JSON broker data into tidy Parquet format.
"""

import sys
from pathlib import Path

import polars as pl


def json_to_parquet(input_path: Path, output_path: Path | None = None) -> Path:
    """Transform nested JSON to tidy Parquet with daily aggregates.

    Performs Intraday Aggregation per dev.md:
    - GroupBy (Date, Symbol, Broker)
    - Outputs: total_buy_amount, total_sell_amount, net_shares

    Args:
        input_path: Path to input JSON file
        output_path: Optional output path. Defaults to daily_trade_summary.parquet

    Returns:
        Path to output parquet file
    """
    if output_path is None:
        output_path = input_path.parent / "daily_trade_summary.parquet"

    df = pl.read_json(input_path)

    tidy = (
        df.explode("data")
        .with_columns(
            # Extract date (YYYY-MM-DD) from ISO string
            pl.col("date").str.slice(0, 10).alias("date"),
            # Parse "1,170.00" → 1170.0
            pl.col("data").struct.field("price")
              .str.replace_all(",", "")
              .cast(pl.Float32)
              .alias("price"),
            pl.col("data").struct.field("buy").cast(pl.Int32).alias("buy"),
            pl.col("data").struct.field("sell").cast(pl.Int32).alias("sell"),
        )
        .with_columns(
            # Calculate amounts per transaction
            (pl.col("buy") * pl.col("price")).alias("buy_amount"),
            (pl.col("sell") * pl.col("price")).alias("sell_amount"),
            (pl.col("buy") - pl.col("sell")).alias("net_shares_txn"),
        )
        .group_by(["date", "symbol_id", "broker"])
        .agg(
            pl.col("buy_amount").sum().cast(pl.Float32).alias("total_buy_amount"),
            pl.col("sell_amount").sum().cast(pl.Float32).alias("total_sell_amount"),
            pl.col("net_shares_txn").sum().cast(pl.Int32).alias("net_shares"),
        )
        .sort(["date", "broker"])
    )

    tidy.write_parquet(output_path)
    print(f"Written {len(tidy):,} rows to {output_path}")
    print(f"Schema: {tidy.schema}")
    return output_path


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python etl.py <input.json> [output.parquet]")
        sys.exit(1)

    input_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2]) if len(sys.argv) > 2 else None
    json_to_parquet(input_path, output_path)
