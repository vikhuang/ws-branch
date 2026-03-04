"""Export daily signals to Signal Contract v1 CSV for ws-quant backtest-external.

Recomputes TA-weighted signals for FDR-passing stocks (from market_scan.json)
and outputs (symbol, trade_date, direction, entry_time, signal_value) where
trade_date = T+1 (next trading day after signal date T).

Usage:
    uv run python export_signals.py
    uv run python export_signals.py --symbols 3665,2345,3017
    uv run python export_signals.py --trade-start 2025-01-02 --trade-end 2025-12-31
    uv run python export_signals.py --output signals_2025.csv
"""

import argparse
import json
import sys
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date
from pathlib import Path

import polars as pl

from market_scan import analyze_symbol, ScanConfig

DATA_DIR = Path("data")


def _load_prices() -> dict[str, dict[date, float]]:
    """Load close_prices.parquet partitioned by symbol."""
    path = DATA_DIR / "price" / "close_prices.parquet"
    if not path.exists():
        print(f"Error: {path} not found. Run sync_prices.py first.")
        sys.exit(1)

    df = pl.read_parquet(path)
    prices: dict[str, dict[date, float]] = defaultdict(dict)
    for row in df.iter_rows(named=True):
        d = row["date"]
        if isinstance(d, str):
            d = date.fromisoformat(d)
        prices[row["symbol_id"]][d] = float(row["close_price"])
    return prices


def _load_fdr_symbols() -> list[str]:
    """Load FDR-passing symbols from market_scan.json."""
    path = DATA_DIR / "derived" / "market_scan.json"
    if not path.exists():
        print(f"Error: {path} not found. Run market_scan.py first.")
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
    """Compute signal for one symbol and return Signal Contract v1 rows.

    Maps signal_date T → trade_date T+1 using the trading calendar.
    Only exports rows where trade_date is within [trade_start, trade_end].
    """
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
        trade_date = test_dates[i + 1]  # next trading day

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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export signals to Signal Contract v1 CSV"
    )
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated symbols (default: all FDR-passing from market_scan.json)",
    )
    parser.add_argument(
        "--trade-start", default="2025-01-02",
        help="First trade date to export (default: 2025-01-02)",
    )
    parser.add_argument(
        "--trade-end", default="2025-12-31",
        help="Last trade date to export (default: 2025-12-31)",
    )
    parser.add_argument(
        "--output", "-o", default=None,
        help="Output CSV path (default: data/derived/signals_{start}_{end}.csv)",
    )
    parser.add_argument(
        "--workers", type=int, default=12,
        help="Parallel workers (default: 12)",
    )
    parser.add_argument("--train-start", default="2023-01-01")
    parser.add_argument("--train-end", default="2024-06-30")
    parser.add_argument("--test-start", default="2024-07-01")
    parser.add_argument("--test-end", default="2025-12-31")
    args = parser.parse_args()

    trade_start = date.fromisoformat(args.trade_start)
    trade_end = date.fromisoformat(args.trade_end)

    config = ScanConfig(
        train_start=args.train_start,
        train_end=args.train_end,
        test_start=args.test_start,
        test_end=args.test_end,
    ).to_dict()

    # Determine symbol list
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",")]
        print(f"Exporting {len(symbols)} specified symbols")
    else:
        symbols = _load_fdr_symbols()
        print(f"Exporting {len(symbols)} FDR-passing symbols from market_scan.json")

    # Load prices
    print("Loading close prices...")
    all_prices = _load_prices()

    # Filter to symbols with price data
    valid_symbols = [s for s in symbols if s in all_prices]
    skipped = len(symbols) - len(valid_symbols)
    if skipped > 0:
        print(f"  Skipped {skipped} symbols without price data")

    summary_dir = str(DATA_DIR / "daily_summary")

    # Parallel signal computation
    print(f"Computing signals for {len(valid_symbols)} symbols ({args.workers} workers)...")
    all_rows: list[dict] = []
    completed = 0
    n_symbols_with_signal = 0

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(
                _extract_signals,
                symbol, all_prices[symbol], summary_dir, config,
                trade_start, trade_end,
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
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = (
            DATA_DIR / "derived"
            / f"signals_{args.trade_start}_{args.trade_end}.csv"
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
    print(f"  ws-quant backtest-external --triggers {output_path} --date {args.trade_start}:{args.trade_end}")


if __name__ == "__main__":
    main()
