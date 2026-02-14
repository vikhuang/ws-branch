"""Data repositories for PNL Analytics.

Provides abstracted data access through the Repository pattern:
- TradeRepository: Per-symbol daily trade summaries
- BrokerRepository: Broker name mappings
- RankingRepository: Pre-aggregated broker rankings
"""

from pnl_analytics.infrastructure.repositories.base import Repository, RepositoryError
from pnl_analytics.infrastructure.repositories.trade_repo import TradeRepository
from pnl_analytics.infrastructure.repositories.broker_repo import BrokerRepository
from pnl_analytics.infrastructure.repositories.pnl_repo import RankingRepository

__all__ = [
    "Repository",
    "RepositoryError",
    "TradeRepository",
    "BrokerRepository",
    "RankingRepository",
]
