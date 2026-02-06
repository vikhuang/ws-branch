"""Entry point for running pnl_analytics as a module.

Usage:
    python -m pnl_analytics [command] [options]

Commands:
    ranking     Generate broker ranking report
    query       Query specific broker metrics
    scorecard   Generate broker scorecard
    verify      Verify data integrity

Examples:
    python -m pnl_analytics ranking --formats csv,xlsx
    python -m pnl_analytics query 1440
    python -m pnl_analytics scorecard 1440
    python -m pnl_analytics verify
"""

import sys

from pnl_analytics.interfaces.cli import main

if __name__ == "__main__":
    sys.exit(main())
