"""Cloud Sync Module: Download close prices from BigQuery.

Module B of High-Speed PNL Analytics System (dev.md).
Fetches close prices and caches locally as Parquet.
"""

import sys
from pathlib import Path

import polars as pl
from google.cloud import bigquery


PROJECT_ID = "gen-lang-client-0998197473"
DATASET = "wsai"
TABLE = "tej_prices"


def sync_prices(
    symbols: list[str],
    start_date: str,
    end_date: str,
    output_path: Path | None = None,
) -> Path:
    """Download close prices from BigQuery for given symbols and date range.

    Uses partition pruning (mdate by year) and cluster filtering (coid)
    to minimize query cost.

    Args:
        symbols: List of stock symbols (e.g., ["2345"])
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        output_path: Optional output path. Defaults to price_master.parquet

    Returns:
        Path to output parquet file
    """
    if output_path is None:
        output_path = Path("price_master.parquet")

    client = bigquery.Client(project=PROJECT_ID)

    # Build query with partition/cluster optimization
    symbols_str = ", ".join(f"'{s}'" for s in symbols)
    query = f"""
    SELECT DISTINCT
        coid,
        mdate,
        close_d
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE coid IN ({symbols_str})
      AND mdate BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY coid, mdate
    """

    print(f"Querying BigQuery for {len(symbols)} symbols...")
    print(f"Date range: {start_date} ~ {end_date}")

    # Estimate cost before running (dry run)
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    dry_run_job = client.query(query, job_config=job_config)
    bytes_processed = dry_run_job.total_bytes_processed
    estimated_cost = (bytes_processed / 1e12) * 5  # $5 per TB
    print(f"Estimated bytes: {bytes_processed:,} ({bytes_processed/1e6:.2f} MB)")
    print(f"Estimated cost: ${estimated_cost:.4f}")

    # Execute query
    job_config = bigquery.QueryJobConfig(use_query_cache=True)
    result = client.query(query, job_config=job_config).result()

    # Convert to Polars DataFrame
    rows = [{"coid": row.coid, "mdate": str(row.mdate), "close_d": row.close_d}
            for row in result]

    df = pl.DataFrame(rows).with_columns(
        pl.col("mdate").alias("date"),
        pl.col("close_d").cast(pl.Float32).alias("close_price"),
    ).select(["coid", "date", "close_price"])

    df.write_parquet(output_path)
    print(f"\nWritten {len(df):,} rows to {output_path}")
    print(f"Schema: {df.schema}")

    return output_path


def sync_from_trade_summary(
    trade_summary_path: Path,
    output_path: Path | None = None,
) -> Path:
    """Sync prices based on symbols and date range in trade summary.

    Reads the daily_trade_summary.parquet to determine which symbols
    and dates need price data.

    Args:
        trade_summary_path: Path to daily_trade_summary.parquet
        output_path: Optional output path

    Returns:
        Path to output parquet file
    """
    df = pl.read_parquet(trade_summary_path)

    symbols = df["symbol_id"].unique().to_list()
    start_date = df["date"].min()
    end_date = df["date"].max()

    print(f"Detected {len(symbols)} symbols from trade summary")
    print(f"Date range: {start_date} ~ {end_date}")

    return sync_prices(symbols, start_date, end_date, output_path)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python sync_prices.py <daily_trade_summary.parquet>")
        print("  python sync_prices.py --symbols 2345 --start 2025-02-03 --end 2026-02-02")
        sys.exit(1)

    if sys.argv[1] == "--symbols":
        # Manual mode: specify symbols and dates
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument("--symbols", nargs="+", required=True)
        parser.add_argument("--start", required=True)
        parser.add_argument("--end", required=True)
        parser.add_argument("--output", default="price_master.parquet")
        args = parser.parse_args()
        sync_prices(args.symbols, args.start, args.end, Path(args.output))
    else:
        # Auto mode: read from trade summary
        sync_from_trade_summary(Path(sys.argv[1]))
