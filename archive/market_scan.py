"""Market Scan: Thin wrapper → broker_analytics.application.services.market_scan.

Usage:
    uv run python market_scan.py
    uv run python market_scan.py --min-turnover 200000000 --cost 0.005 --fdr 0.01
"""

import argparse

from broker_analytics.application.services.market_scan import (
    ScanConfig,
    run_scan,
    DEFAULT_MIN_TURNOVER,
    DEFAULT_COST,
    DEFAULT_FDR,
    DEFAULT_WORKERS,
    DEFAULT_TRAIN_START,
    DEFAULT_TRAIN_END,
    DEFAULT_TEST_START,
    DEFAULT_TEST_END,
    MIN_TEST_DAYS,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Market Scan: Full-market signal screening with FDR correction"
    )
    parser.add_argument(
        "--min-turnover",
        type=float,
        default=DEFAULT_MIN_TURNOVER,
        help=f"Min avg daily turnover in NTD (default: {DEFAULT_MIN_TURNOVER:.0f})",
    )
    parser.add_argument(
        "--cost",
        type=float,
        default=DEFAULT_COST,
        help=f"Cost per trade (default: {DEFAULT_COST})",
    )
    parser.add_argument(
        "--fdr",
        type=float,
        default=DEFAULT_FDR,
        help=f"FDR threshold (default: {DEFAULT_FDR})",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Number of parallel workers (default: {DEFAULT_WORKERS})",
    )
    parser.add_argument("--train-start", default=DEFAULT_TRAIN_START)
    parser.add_argument("--train-end", default=DEFAULT_TRAIN_END)
    parser.add_argument("--test-start", default=DEFAULT_TEST_START)
    parser.add_argument("--test-end", default=DEFAULT_TEST_END)
    args = parser.parse_args()

    config = ScanConfig(
        min_turnover=args.min_turnover,
        cost=args.cost,
        fdr_threshold=args.fdr,
        min_test_days=MIN_TEST_DAYS,
        train_start=args.train_start,
        train_end=args.train_end,
        test_start=args.test_start,
        test_end=args.test_end,
        workers=args.workers,
    )

    run_scan(config)


if __name__ == "__main__":
    main()
