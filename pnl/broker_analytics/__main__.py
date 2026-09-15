"""Entry point for running broker_analytics as a module.

Usage:
    python -m broker_analytics [command] [options]

Commands:
    ranking     Generate broker ranking report
    query       Query specific broker metrics
    scorecard   Generate broker scorecard
    verify      Verify data integrity

Examples:
    python -m broker_analytics ranking --formats csv,xlsx
    python -m broker_analytics query 1440
    python -m broker_analytics scorecard 1440
    python -m broker_analytics verify
"""

import sys

from broker_analytics.interfaces.cli import main

if __name__ == "__main__":
    sys.exit(main())
