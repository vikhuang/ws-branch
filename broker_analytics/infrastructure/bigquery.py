"""BigQuery client for TEJ price data.

Centralizes all BigQuery access: close prices and OHLC data.
Each function caches results to parquet files to avoid repeated queries.

Used by: sync_prices, signal_report, market_scan
"""

from pathlib import Path

import polars as pl

PROJECT_ID = "gen-lang-client-0998197473"
DATASET = "wsai"
TABLE = "tej_prices"
_TABLE_REF = f"{PROJECT_ID}.{DATASET}.{TABLE}"


def _get_client():
    """Lazy-import and create BigQuery client."""
    from google.cloud import bigquery
    return bigquery.Client(project=PROJECT_ID)


def fetch_close_prices_batch(
    symbols: list[str],
    start_date: str = "2021-01-01",
    end_date: str | None = None,
    batch_size: int = 500,
) -> pl.DataFrame:
    """Fetch close prices for multiple symbols from BigQuery.

    Args:
        symbols: List of stock symbols.
        start_date: Start date (inclusive).
        end_date: End date (inclusive). None = today.
        batch_size: Symbols per query batch.

    Returns:
        DataFrame[symbol_id (Utf8), date (Date), close_price (Float64)]
    """
    client = _get_client()
    all_rows: list[dict] = []

    end_clause = f"AND mdate <= '{end_date}'" if end_date else ""

    for i in range(0, len(symbols), batch_size):
        batch = symbols[i : i + batch_size]
        symbols_str = ", ".join(f"'{s}'" for s in batch)
        query = f"""
        SELECT DISTINCT coid, mdate, close_d
        FROM `{_TABLE_REF}`
        WHERE coid IN ({symbols_str})
          AND mdate >= '{start_date}'
          {end_clause}
        ORDER BY coid, mdate
        """
        for row in client.query(query).result():
            all_rows.append({
                "symbol_id": row["coid"],
                "date": row["mdate"],
                "close_price": float(row["close_d"]),
            })

    if not all_rows:
        return pl.DataFrame(schema={
            "symbol_id": pl.Utf8,
            "date": pl.Date,
            "close_price": pl.Float64,
        })

    return pl.DataFrame(all_rows).with_columns(
        pl.col("date").cast(pl.Date),
    )


def fetch_ohlc(symbol: str, cache_dir: Path | None = None) -> pl.DataFrame:
    """Fetch OHLC for a single symbol, with parquet cache.

    Args:
        symbol: Stock symbol.
        cache_dir: Directory for caching. None = no cache.

    Returns:
        DataFrame[date (Date), open (Float64), close (Float64)]
    """
    if cache_dir:
        cache_path = cache_dir / f"{symbol}_ohlc.parquet"
        if cache_path.exists():
            return pl.read_parquet(cache_path)

    client = _get_client()
    query = f"""
    SELECT mdate AS date, open_d AS open, close_d AS close
    FROM `{_TABLE_REF}`
    WHERE coid = '{symbol}' AND mdate >= '2021-01-01'
    ORDER BY mdate
    """
    rows = [dict(row) for row in client.query(query).result()]

    if not rows:
        return pl.DataFrame(schema={"date": pl.Date, "open": pl.Float64, "close": pl.Float64})

    df = pl.DataFrame(rows).with_columns(pl.col("date").cast(pl.Date))

    if cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        df.write_parquet(cache_dir / f"{symbol}_ohlc.parquet")

    return df


def fetch_ohlc_batch(
    symbols: list[str],
    cache_dir: Path | None = None,
) -> dict[str, pl.DataFrame]:
    """Fetch OHLC for multiple symbols, with per-symbol parquet cache.

    Symbols already cached are loaded from disk.
    Remaining symbols are fetched in a single BigQuery query.

    Args:
        symbols: List of stock symbols.
        cache_dir: Directory for caching. None = no cache.

    Returns:
        {symbol: DataFrame[date, open, close]}
    """
    result: dict[str, pl.DataFrame] = {}
    to_fetch: list[str] = []

    # Check cache
    if cache_dir:
        for s in symbols:
            cache_path = cache_dir / f"{s}_ohlc.parquet"
            if cache_path.exists():
                result[s] = pl.read_parquet(cache_path)
            else:
                to_fetch.append(s)
    else:
        to_fetch = list(symbols)

    if not to_fetch:
        return result

    # Batch fetch
    client = _get_client()
    symbols_str = ",".join(f"'{s}'" for s in to_fetch)
    query = f"""
    SELECT coid AS symbol, mdate AS date, open_d AS open, close_d AS close
    FROM `{_TABLE_REF}`
    WHERE coid IN ({symbols_str}) AND mdate >= '2021-01-01'
    ORDER BY coid, mdate
    """
    rows = [dict(row) for row in client.query(query).result()]

    if rows:
        all_df = pl.DataFrame(rows).with_columns(pl.col("date").cast(pl.Date))
        for symbol in to_fetch:
            sym_df = all_df.filter(pl.col("symbol") == symbol).drop("symbol")
            result[symbol] = sym_df
            if cache_dir:
                cache_dir.mkdir(parents=True, exist_ok=True)
                sym_df.write_parquet(cache_dir / f"{symbol}_ohlc.parquet")

    return result
