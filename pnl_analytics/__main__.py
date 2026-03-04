"""Backward-compatible shim — delegates to broker_analytics."""

import sys
from broker_analytics.interfaces.cli import main

if __name__ == "__main__":
    sys.exit(main())
