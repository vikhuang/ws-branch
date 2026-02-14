"""Sync Prices: Download close prices from BigQuery.

Fetches close prices for all symbols in daily_summary/ and outputs
to data/price/close_prices.parquet.

BigQuery Table:
    Project: gen-lang-client-0998197473
    Dataset: wsai
    Table: tej_prices (partitioned by year, clustered by coid)
"""

import sys
from datetime import date
from pathlib import Path

import polars as pl
from google.cloud import bigquery

from pnl_analytics.infrastructure.config import DataPaths, DEFAULT_PATHS


# BigQuery configuration
PROJECT_ID = "gen-lang-client-0998197473"
DATASET = "wsai"
TABLE = "tej_prices"

# Batch size for BigQuery queries (avoid query length limits)
BATCH_SIZE = 500


def fetch_prices_batch(
    client: bigquery.Client,
    symbols: list[str],
    start_date: str,
    end_date: str,
) -> list[dict]:
    """Fetch prices for a batch of symbols from BigQuery.

    Args:
        client: BigQuery client
        symbols: List of stock symbols
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        List of row dicts with symbol_id, date, close_price
    """
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

    result = client.query(query).result()

    return [
        {
            "symbol_id": row.coid,
            "date": row.mdate,
            "close_price": float(row.close_d) if row.close_d else None,
        }
        for row in result
    ]


def sync_prices(
    paths: DataPaths = DEFAULT_PATHS,
    start_date: str = "2021-01-01",
    end_date: str | None = None,
) -> pl.DataFrame:
    """Sync close prices for all symbols in daily_summary/.

    Args:
        paths: Data paths configuration
        start_date: Start date for price data
        end_date: End date (defaults to today)

    Returns:
        DataFrame with columns: symbol_id, date, close_price
    """
    paths.ensure_dirs()

    # Get symbols from daily_summary/
    symbols = paths.list_symbols()
    if not symbols:
        print("Error: No symbols found in daily_summary/")
        return pl.DataFrame()

    # Default end date to today
    if end_date is None:
        end_date = date.today().isoformat()

    print(f"Syncing prices for {len(symbols)} symbols...")
    print(f"Date range: {start_date} ~ {end_date}")

    # Estimate cost (dry run with first batch)
    client = bigquery.Client(project=PROJECT_ID)
    sample_query = f"""
    SELECT DISTINCT coid, mdate, close_d
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE coid IN ('{symbols[0]}')
      AND mdate BETWEEN '{start_date}' AND '{end_date}'
    """
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    dry_run = client.query(sample_query, job_config=job_config)
    bytes_per_symbol = dry_run.total_bytes_processed
    total_bytes = bytes_per_symbol * len(symbols)
    estimated_cost = (total_bytes / 1e12) * 5  # $5 per TB
    print(f"Estimated: {total_bytes / 1e9:.2f} GB, ${estimated_cost:.4f}")

    # Fetch in batches
    all_rows = []
    total_batches = (len(symbols) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(symbols), BATCH_SIZE):
        batch = symbols[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        print(f"  Batch {batch_num}/{total_batches}: {len(batch)} symbols")

        rows = fetch_prices_batch(client, batch, start_date, end_date)
        all_rows.extend(rows)

    # Build DataFrame
    df = pl.DataFrame(all_rows)

    if len(df) == 0:
        print("Warning: No price data returned")
        return df

    # Convert types
    df = df.with_columns([
        pl.col("date").cast(pl.Date),
        pl.col("close_price").cast(pl.Float32),
    ])

    # Write output
    df.write_parquet(paths.close_prices)
    print(f"\nSaved: {paths.close_prices}")
    print(f"  {len(df):,} rows, {df['symbol_id'].n_unique()} symbols")
    print(f"  Date range: {df['date'].min()} ~ {df['date'].max()}")

    return df


def main() -> None:
    """CLI entry point."""
    paths = DEFAULT_PATHS

    # Check for custom paths
    if len(sys.argv) > 1 and sys.argv[1] != "--help":
        paths = DataPaths(root=Path(sys.argv[1]))

    # Validate prerequisites
    if not paths.daily_summary_dir.exists():
        print("Error: daily_summary/ not found. Run ETL first.")
        sys.exit(1)

    sync_prices(paths)


if __name__ == "__main__":
    main()
