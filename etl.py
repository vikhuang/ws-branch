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
    - Outputs: buy_shares, sell_shares, buy_amount, sell_amount

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
            # Convert UTC timestamp to Taiwan date (UTC+8)
            # "2026-02-02T16:00:00.000Z" → 2026-02-03 in Taiwan
            pl.col("date")
              .str.to_datetime("%Y-%m-%dT%H:%M:%S%.fZ")
              .dt.convert_time_zone("Asia/Taipei")
              .dt.date()
              .cast(pl.String)
              .alias("date"),
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
            pl.col("buy").sum().cast(pl.Int32).alias("buy_shares"),
            pl.col("sell").sum().cast(pl.Int32).alias("sell_shares"),
            pl.col("buy_amount").sum().cast(pl.Float32).alias("buy_amount"),
            pl.col("sell_amount").sum().cast(pl.Float32).alias("sell_amount"),
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
