"""Broker Analysis Service: Comprehensive broker metrics calculation.

Orchestrates the calculation of all broker metrics:
- Basic statistics (trading days, volume, amount)
- PNL (realized, unrealized, total)
- Execution Alpha
- Timing Alpha with statistical significance
- Lead/Lag correlations
- Trading style classification

This service uses repositories for data access and domain logic for calculations.
"""

from dataclasses import dataclass
from typing import Sequence

import numpy as np
import polars as pl

from pnl_analytics.domain.returns import (
    calculate_daily_returns,
    correlation_coefficient,
    lead_lag_series,
)
from pnl_analytics.domain.metrics import (
    add_alpha_columns,
    calculate_broker_alpha,
    calculate_timing_alpha,
    permutation_test,
)


# =============================================================================
# Result Data Class
# =============================================================================

@dataclass(frozen=True, slots=True)
class BrokerAnalysisResult:
    """Complete analysis result for a single broker.

    Contains all metrics needed for the ranking report.
    """
    # Identity
    broker: str
    name: str

    # Basic Stats
    trading_days: int
    total_buy_shares: int
    total_sell_shares: int
    total_volume: int
    buy_amount: float
    sell_amount: float
    total_amount: float
    cumulative_net: int

    # Direction
    direction: str  # "做多", "做空", "中性"

    # PNL
    realized_pnl: float
    unrealized_pnl: float
    total_pnl: float

    # Execution Alpha
    exec_alpha: float | None
    trade_count: int

    # Timing Alpha
    timing_alpha: float | None
    p_value: float | None
    timing_significance: str | None  # "顯著正向", "顯著負向", "不顯著"

    # Correlations
    lead_corr: float | None
    lag_corr: float | None
    style: str | None  # "順勢", "逆勢", "中性"

    def to_dict(self) -> dict:
        """Convert to dictionary for DataFrame creation."""
        return {
            "broker": self.broker,
            "name": self.name,
            "trading_days": self.trading_days,
            "total_buy_shares": self.total_buy_shares,
            "total_sell_shares": self.total_sell_shares,
            "total_volume": self.total_volume,
            "buy_amount": self.buy_amount,
            "sell_amount": self.sell_amount,
            "total_amount": self.total_amount,
            "cumulative_net": self.cumulative_net,
            "direction": self.direction,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "total_pnl": self.total_pnl,
            "exec_alpha": self.exec_alpha,
            "trade_count": self.trade_count,
            "timing_alpha": self.timing_alpha,
            "p_value": self.p_value,
            "timing_significance": self.timing_significance,
            "lead_corr": self.lead_corr,
            "lag_corr": self.lag_corr,
            "style": self.style,
        }


# =============================================================================
# Broker Analyzer
# =============================================================================

class BrokerAnalyzer:
    """Analyzes a single broker's trading performance.

    This class encapsulates the logic for calculating all broker metrics.
    It takes pre-loaded data and calculates metrics on demand.

    Example:
        >>> analyzer = BrokerAnalyzer(
        ...     trade_df=trade_df,
        ...     closed_trades_with_alpha=closed_with_alpha,
        ...     realized_pnl=realized,
        ...     unrealized_pnl=unrealized,
        ...     broker_index_map=maps["brokers"],
        ...     daily_returns=returns,
        ...     all_dates=dates,
        ... )
        >>> result = analyzer.analyze("1440", name="美林")
    """

    def __init__(
        self,
        trade_df: pl.DataFrame,
        closed_trades_with_alpha: pl.DataFrame,
        realized_pnl: np.ndarray,
        unrealized_pnl: np.ndarray,
        broker_index_map: dict[str, int],
        daily_returns: dict[str, float],
        all_dates: list[str],
        min_trading_days: int = 20,
        permutation_count: int = 200,
    ):
        """Initialize the analyzer with required data.

        Args:
            trade_df: Daily trade summary DataFrame
            closed_trades_with_alpha: Closed trades with alpha columns added
            realized_pnl: 3D numpy array of realized PNL
            unrealized_pnl: 3D numpy array of unrealized PNL
            broker_index_map: Dict mapping broker code to tensor index
            daily_returns: Dict mapping date to daily return
            all_dates: Sorted list of all trading dates
            min_trading_days: Minimum days for timing analysis
            permutation_count: Number of permutations for significance test
        """
        self._trade_df = trade_df
        self._closed_trades = closed_trades_with_alpha
        self._realized = realized_pnl
        self._unrealized = unrealized_pnl
        self._broker_idx_map = broker_index_map
        self._returns = daily_returns
        self._all_dates = all_dates
        self._min_trading_days = min_trading_days
        self._permutation_count = permutation_count

        # Precompute valid dates for timing analysis
        self._valid_dates = [d for d in all_dates if d in daily_returns]

    def analyze(self, broker: str, name: str = "") -> BrokerAnalysisResult | None:
        """Analyze a single broker.

        Args:
            broker: Broker code (e.g., "1440")
            name: Broker name (optional)

        Returns:
            BrokerAnalysisResult with all metrics, or None if broker not found
        """
        # Check broker exists in index
        broker_idx = self._broker_idx_map.get(broker)
        if broker_idx is None:
            return None

        # Filter trades for this broker
        broker_trades = self._trade_df.filter(pl.col("broker") == broker)

        # === Basic Stats ===
        trading_days = len(broker_trades)
        total_buy = int(broker_trades["buy_shares"].sum() or 0)
        total_sell = int(broker_trades["sell_shares"].sum() or 0)
        total_volume = total_buy + total_sell
        buy_amount = float(broker_trades["buy_amount"].sum() or 0)
        sell_amount = float(broker_trades["sell_amount"].sum() or 0)
        total_amount = buy_amount + sell_amount

        # === Direction ===
        cumulative_net = total_buy - total_sell
        if cumulative_net > 0:
            direction = "做多"
        elif cumulative_net < 0:
            direction = "做空"
        else:
            direction = "中性"

        # === PNL ===
        sym_idx = 0  # Single stock analysis
        total_realized = float(self._realized[sym_idx, :, broker_idx].sum())
        final_unrealized = float(self._unrealized[sym_idx, -1, broker_idx])
        total_pnl = total_realized + final_unrealized

        # === Execution Alpha ===
        alpha_result = calculate_broker_alpha(self._closed_trades, broker)
        if alpha_result:
            exec_alpha = alpha_result.weighted_alpha
            trade_count = alpha_result.trade_count
        else:
            exec_alpha = None
            trade_count = 0

        # === Timing Alpha & Correlations ===
        timing_alpha = None
        lead_corr = None
        lag_corr = None
        p_value = None
        style = None
        timing_sig = None

        if trading_days >= self._min_trading_days:
            # Build net buys series
            net_buys_raw = {}
            for row in broker_trades.iter_rows(named=True):
                net = (row["buy_shares"] or 0) - (row["sell_shares"] or 0)
                net_buys_raw[row["date"]] = net

            # Align with returns
            net_buys = [net_buys_raw.get(d, 0) for d in self._valid_dates]
            daily_returns_list = [self._returns[d] for d in self._valid_dates]

            # Timing alpha
            timing_alpha = calculate_timing_alpha(net_buys, daily_returns_list)

            # Lead correlation: net_buy[t-1] vs return[t]
            if len(net_buys) > 10:
                lead_x = net_buys[:-1]
                lead_y = daily_returns_list[1:]
                lead_corr = correlation_coefficient(lead_x, lead_y)

                # Lag correlation: return[t-1] vs net_buy[t]
                lag_x = daily_returns_list[:-1]
                lag_y = net_buys[1:]
                lag_corr = correlation_coefficient(lag_x, lag_y)

            # Permutation test
            p_value = permutation_test(
                net_buys, daily_returns_list,
                n_permutations=self._permutation_count
            )

            # Style classification
            if lag_corr is not None:
                if lag_corr > 0.05:
                    style = "順勢"
                elif lag_corr < -0.05:
                    style = "逆勢"
                else:
                    style = "中性"

            # Timing significance
            if p_value is not None and timing_alpha is not None:
                if p_value < 0.05 and timing_alpha > 0:
                    timing_sig = "顯著正向"
                elif p_value < 0.05 and timing_alpha < 0:
                    timing_sig = "顯著負向"
                else:
                    timing_sig = "不顯著"

        return BrokerAnalysisResult(
            broker=broker,
            name=name,
            trading_days=trading_days,
            total_buy_shares=total_buy,
            total_sell_shares=total_sell,
            total_volume=total_volume,
            buy_amount=buy_amount,
            sell_amount=sell_amount,
            total_amount=total_amount,
            cumulative_net=cumulative_net,
            direction=direction,
            realized_pnl=total_realized,
            unrealized_pnl=final_unrealized,
            total_pnl=total_pnl,
            exec_alpha=exec_alpha,
            trade_count=trade_count,
            timing_alpha=timing_alpha,
            p_value=p_value,
            timing_significance=timing_sig,
            lead_corr=lead_corr,
            lag_corr=lag_corr,
            style=style,
        )
