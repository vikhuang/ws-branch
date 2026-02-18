"""Application Services for PNL Analytics.

Services orchestrate repository access to implement use cases.

Available services:
- RankingService: Broker ranking report generation
- BrokerAnalyzer: Single broker analysis
- SymbolAnalyzer: Smart money signal for individual stocks
"""

from pnl_analytics.application.services.ranking import (
    RankingService,
    RankingReportConfig,
)
from pnl_analytics.application.services.broker_analysis import (
    BrokerAnalyzer,
    BrokerAnalysisResult,
)
from pnl_analytics.application.services.symbol_analysis import (
    SymbolAnalyzer,
    SymbolAnalysisResult,
    SmartMoneySignal,
)

__all__ = [
    "RankingService",
    "RankingReportConfig",
    "BrokerAnalyzer",
    "BrokerAnalysisResult",
    "SymbolAnalyzer",
    "SymbolAnalysisResult",
    "SmartMoneySignal",
]
