"""Export Signals: Thin wrapper → broker_analytics.application.services.signal_export.

Usage:
    uv run python export_signals.py
    uv run python export_signals.py --symbols 3665,2345,3017
    uv run python export_signals.py --trade-start 2025-01-02 --trade-end 2025-12-31
"""

import argparse

from broker_analytics.application.services.signal_export import run_export


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

    symbols = None
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",")]

    run_export(
        symbols=symbols,
        trade_start=args.trade_start,
        trade_end=args.trade_end,
        output=args.output,
        workers=args.workers,
        train_start=args.train_start,
        train_end=args.train_end,
        test_start=args.test_start,
        test_end=args.test_end,
    )


if __name__ == "__main__":
    main()
