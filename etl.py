"""ETL: JSON → Parquet with PNL aggregation."""

import sys
from pathlib import Path

import polars as pl


def etl(input_path: Path) -> Path:
    """Transform nested JSON to tidy Parquet with PNL."""
    output_path = input_path.with_suffix(".parquet")

    df = pl.read_json(input_path)

    tidy = (
        df.explode("data")
        .with_columns(
            pl.col("date").str.slice(0, 10).alias("date"),
            # Parse "1,170.00" → 1170.0 using native Polars ops
            pl.col("data").struct.field("price")
              .str.replace_all(",", "")
              .cast(pl.Float32)
              .alias("price"),
            pl.col("data").struct.field("buy").cast(pl.Int32).alias("buy"),
            pl.col("data").struct.field("sell").cast(pl.Int32).alias("sell"),
        )
        .with_columns(
            ((pl.col("sell") - pl.col("buy")) * pl.col("price")).alias("pnl_component")
        )
        .group_by(["date", "symbol_id", "broker"])
        .agg(pl.col("pnl_component").sum().alias("pnl"))
    )

    tidy.write_parquet(output_path)
    print(f"Written {len(tidy)} rows to {output_path}")
    return output_path


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python etl.py <input.json>")
        sys.exit(1)
    etl(Path(sys.argv[1]))
