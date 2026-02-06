"""Data repositories for PNL Analytics.

Provides abstracted data access through the Repository pattern:
- TradeRepository: Daily trade summary data
- ClosedTradeRepository: Completed trades for alpha analysis
- PriceRepository: Daily closing prices
- BrokerRepository: Broker name mappings
- IndexMapRepository: Tensor dimension mappings
- PnlRepository: Realized and unrealized PNL tensors
"""

from pnl_analytics.infrastructure.repositories.base import Repository, RepositoryError
from pnl_analytics.infrastructure.repositories.trade_repo import (
    TradeRepository,
    ClosedTradeRepository,
)
from pnl_analytics.infrastructure.repositories.price_repo import PriceRepository
from pnl_analytics.infrastructure.repositories.broker_repo import (
    BrokerRepository,
    IndexMapRepository,
)
from pnl_analytics.infrastructure.repositories.pnl_repo import PnlRepository

__all__ = [
    "Repository",
    "RepositoryError",
    "TradeRepository",
    "ClosedTradeRepository",
    "PriceRepository",
    "BrokerRepository",
    "IndexMapRepository",
    "PnlRepository",
]
