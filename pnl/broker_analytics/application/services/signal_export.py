"""Export daily signals to Signal Contract v1 CSV for ws-quant backtest-external.

Recomputes TA-weighted signals for FDR-passing stocks (from market_scan.json)
and outputs (symbol, trade_date, direction, entry_time, signal_value) where
trade_date = T+1 (next trading day after signal date T).
"""

import json
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date
from pathlib import Path

import polars as pl

from broker_analytics.application.services.market_scan import analyze_symbol, ScanConfig
from broker_analytics.infrastructure.config import DataPaths, DEFAULT_PATHS
from broker_analytics.infrastructure.repositories.price_repo import PriceRepository


def _load_fdr_symbols(paths: DataPaths = DEFAULT_PATHS) -> list[str]:
    """Load FDR-passing symbols from market_scan.json."""
    path = paths.market_scan_path
    if not path.exists():
        print(f"Error: {path} not found. Run market_scan first.")
        sys.exit(1)

    with open(path) as f:
        data = json.load(f)
    return [r["symbol"] for r in data["results"]]


def _extract_signals(
    symbol: str,
    prices: dict[date, float],
    summary_dir: str,
    config: dict,
    trade_start: date,
    trade_end: date,
) -> list[dict]:
    """Compute signal for one symbol and return Signal Contract v1 rows."""
    try:
        result = analyze_symbol(symbol, prices, summary_dir, config)
    except Exception as e:
        print(f"  Warning: {symbol} failed: {e}", file=sys.stderr)
        return []

    if not result.get("passed"):
        return []

    signal = result["signal"]
    test_dates = sorted(result["test_dates"])

    rows = []
    for i in range(len(test_dates) - 1):
        signal_date = test_dates[i]
        trade_date = test_dates[i + 1]

        if trade_date < trade_start or trade_date > trade_end:
            continue

        sig = signal.get(signal_date, 0.0)
        if sig == 0.0:
            continue

        rows.append({
            "symbol": symbol,
            "date": trade_date.isoformat(),
            "direction": "long" if sig > 0 else "short",
            "entry_time": "09:00:00",
            "signal_value": round(sig, 6),
        })

    return rows


def run_export(
    symbols: list[str] | None = None,
    trade_start: str = "2025-01-02",
    trade_end: str = "2025-12-31",
    output: str | None = None,
    workers: int = 12,
    train_start: str = "2023-01-01",
    train_end: str = "2024-06-30",
    test_start: str = "2024-07-01",
    test_end: str = "2025-12-31",
    paths: DataPaths = DEFAULT_PATHS,
) -> None:
    """Execute signal export pipeline."""
    trade_start_d = date.fromisoformat(trade_start)
    trade_end_d = date.fromisoformat(trade_end)

    config = ScanConfig(
        train_start=train_start,
        train_end=train_end,
        test_start=test_start,
        test_end=test_end,
    ).to_dict()

    # Determine symbol list
    if symbols:
        print(f"Exporting {len(symbols)} specified symbols")
    else:
        symbols = _load_fdr_symbols(paths)
        print(f"Exporting {len(symbols)} FDR-passing symbols from market_scan.json")

    # Load prices
    print("Loading close prices...")
    repo = PriceRepository(paths)
    all_prices = repo.get_all_close_prices()

    # Filter to symbols with price data
    valid_symbols = [s for s in symbols if s in all_prices]
    skipped = len(symbols) - len(valid_symbols)
    if skipped > 0:
        print(f"  Skipped {skipped} symbols without price data")

    summary_dir = str(paths.daily_summary_dir)

    # Parallel signal computation
    print(f"Computing signals for {len(valid_symbols)} symbols ({workers} workers)...")
    all_rows: list[dict] = []
    completed = 0
    n_symbols_with_signal = 0

    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _extract_signals,
                symbol, all_prices[symbol], summary_dir, config,
                trade_start_d, trade_end_d,
            ): symbol
            for symbol in valid_symbols
        }

        for future in as_completed(futures):
            completed += 1
            rows = future.result()
            if rows:
                all_rows.extend(rows)
                n_symbols_with_signal += 1
            if completed % 50 == 0 or completed == len(valid_symbols):
                print(f"  {completed}/{len(valid_symbols)} done")

    if not all_rows:
        print("No signals to export.")
        return

    # Sort by date, then symbol
    all_rows.sort(key=lambda r: (r["date"], r["symbol"]))

    # Write CSV
    if output:
        output_path = Path(output)
    else:
        output_path = (
            paths.derived_dir
            / f"signals_{trade_start}_{trade_end}.csv"
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pl.DataFrame(all_rows)
    df.write_csv(output_path)

    # Summary
    n_long = sum(1 for r in all_rows if r["direction"] == "long")
    n_short = len(all_rows) - n_long
    unique_dates = len(set(r["date"] for r in all_rows))
    unique_symbols = len(set(r["symbol"] for r in all_rows))

    print(f"\nExport complete: {output_path}")
    print(f"  Rows:    {len(all_rows):,}")
    print(f"  Symbols: {unique_symbols} (of {len(valid_symbols)} processed)")
    print(f"  Dates:   {unique_dates} trading days")
    print(f"  Long:    {n_long:,}")
    print(f"  Short:   {n_short:,}")
    print(f"\nSignal Contract v1 format — ready for:")
    print(f"  ws-quant backtest-external --triggers {output_path} --date {trade_start}:{trade_end}")
