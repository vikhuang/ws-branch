"""Application Services for Broker Analytics.

Services orchestrate repository access to implement use cases.

Available services:
- RankingService: Broker ranking report generation
- BrokerAnalyzer: Single broker analysis
- SymbolAnalyzer: Smart money signal for individual stocks
- signal_report: Per-stock signal analysis pipeline
- market_scan: Full-market signal screening with FDR
- signal_export: Signal CSV export for ws-quant
"""

from broker_analytics.application.services.ranking import (
    RankingService,
    RankingReportConfig,
)
from broker_analytics.application.services.broker_analysis import (
    BrokerAnalyzer,
    BrokerAnalysisResult,
)
from broker_analytics.application.services.symbol_analysis import (
    SymbolAnalyzer,
    SymbolAnalysisResult,
    SmartMoneySignal,
)
from broker_analytics.application.services.rolling_ranking import (
    RollingRankingService,
)
from broker_analytics.application.services.event_study import (
    EventStudyService,
    EventStudyReport,
)

__all__ = [
    "RankingService",
    "RankingReportConfig",
    "BrokerAnalyzer",
    "BrokerAnalysisResult",
    "SymbolAnalyzer",
    "SymbolAnalysisResult",
    "SmartMoneySignal",
    "RollingRankingService",
    "EventStudyService",
    "EventStudyReport",
]
