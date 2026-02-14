"""Data repositories for PNL Analytics.

Provides abstracted data access through the Repository pattern:
- TradeRepository: Per-symbol daily trade summaries
- PriceRepository: Daily close prices
- BrokerRepository: Broker name mappings
- RankingRepository: Pre-aggregated broker rankings
"""

from pnl_analytics.infrastructure.repositories.base import Repository, RepositoryError
from pnl_analytics.infrastructure.repositories.trade_repo import TradeRepository
from pnl_analytics.infrastructure.repositories.price_repo import PriceRepository
from pnl_analytics.infrastructure.repositories.broker_repo import BrokerRepository
from pnl_analytics.infrastructure.repositories.pnl_repo import RankingRepository

__all__ = [
    "Repository",
    "RepositoryError",
    "TradeRepository",
    "PriceRepository",
    "BrokerRepository",
    "RankingRepository",
]
