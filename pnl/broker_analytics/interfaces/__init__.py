"""Interfaces Layer: CLI and API endpoints.

This layer contains:
- cli.py: Command-line interface
"""

from broker_analytics.interfaces.cli import main as cli_main

__all__ = ["cli_main"]
