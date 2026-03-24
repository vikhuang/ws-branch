"""Signal Report: Thin wrapper → broker_analytics.application.services.signal_report.

Usage:
    uv run python signal_report.py 2345
    uv run python signal_report.py 2330 --train-start 2023-01-01 --train-end 2024-06-30
"""

import argparse

from broker_analytics.application.services.signal_report import (
    run_pipeline,
    DEFAULT_TRAIN_START,
    DEFAULT_TRAIN_END,
    DEFAULT_TEST_START,
    DEFAULT_TEST_END,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Signal Report: Large trade detection → TA-weighted backtest"
    )
    parser.add_argument("symbol", help="Stock symbol (e.g., 2345)")
    parser.add_argument("--train-start", default=DEFAULT_TRAIN_START)
    parser.add_argument("--train-end", default=DEFAULT_TRAIN_END)
    parser.add_argument("--test-start", default=DEFAULT_TEST_START)
    parser.add_argument("--test-end", default=DEFAULT_TEST_END)
    args = parser.parse_args()

    run_pipeline(
        args.symbol,
        train_start=args.train_start,
        train_end=args.train_end,
        test_start=args.test_start,
        test_end=args.test_end,
    )


if __name__ == "__main__":
    main()
