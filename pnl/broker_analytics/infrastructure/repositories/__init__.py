"""Data repositories for PNL Analytics.

Provides abstracted data access through the Repository pattern:
- TradeRepository: Per-symbol daily trade summaries
- BrokerRepository: Broker name mappings
- RankingRepository: Pre-aggregated broker rankings
"""

from broker_analytics.infrastructure.repositories.base import Repository, RepositoryError
from broker_analytics.infrastructure.repositories.trade_repo import TradeRepository
from broker_analytics.infrastructure.repositories.broker_repo import BrokerRepository
from broker_analytics.infrastructure.repositories.pnl_repo import RankingRepository
from broker_analytics.infrastructure.repositories.price_repo import PriceRepository

__all__ = [
    "Repository",
    "RepositoryError",
    "TradeRepository",
    "BrokerRepository",
    "RankingRepository",
    "PriceRepository",
]
