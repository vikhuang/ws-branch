"""Strategy registry: Maps strategy names to HypothesisConfig instances.

Each of the 9 strategies is defined here as a concrete composition
of selector + filter + outcome + baseline + stat_test.
"""

from broker_analytics.domain.hypothesis.types import HypothesisConfig
from broker_analytics.domain.hypothesis.selectors import (
    select_contrarian_brokers,
    select_dual_window_intersection,
    select_top_k_by_pnl,
    select_top_and_bottom_k,
    select_ta_regime_change,
    select_all_active_brokers,
)
from broker_analytics.domain.hypothesis.filters import (
    filter_large_trades,
    filter_conviction_signals,
    filter_collective_exodus,
    filter_contrarian_on_panic,
    filter_hhi_breakout,
    filter_herding_agreement,
)
from broker_analytics.domain.hypothesis.outcomes import (
    outcome_forward_returns,
    outcome_cross_stock_returns,
)
from broker_analytics.domain.hypothesis.baselines import (
    baseline_unconditional,
    baseline_cross_stock_unconditional,
    baseline_disagreement_returns,
)
from broker_analytics.domain.hypothesis.stat_tests import (
    stat_test_permutation,
)


STRATEGIES: dict[str, HypothesisConfig] = {

    "contrarian_broker": HypothesisConfig(
        name="contrarian_broker",
        display_name="反差券商",
        description="Global PNL bottom 20% but per-stock PNL top 20%",
        selector=select_contrarian_brokers,
        filter=filter_large_trades,
        outcome=outcome_forward_returns,
        baseline=baseline_unconditional,
        stat_test=stat_test_permutation,
        params={"global_pct": 0.2, "local_pct": 0.2, "sigma": 2.0},
    ),

    "dual_window": HypothesisConfig(
        name="dual_window",
        display_name="雙窗口交集",
        description="Brokers in top-K for both 1yr and 3yr rolling windows",
        selector=select_dual_window_intersection,
        filter=filter_large_trades,
        outcome=outcome_forward_returns,
        baseline=baseline_unconditional,
        stat_test=stat_test_permutation,
        params={"top_k": 20, "short_years": 1, "long_years": 3, "sigma": 2.0},
    ),

    "conviction": HypothesisConfig(
        name="conviction",
        display_name="加碼信號",
        description="Top brokers adding to winning positions",
        selector=select_top_k_by_pnl,
        filter=filter_conviction_signals,
        outcome=outcome_forward_returns,
        baseline=baseline_unconditional,
        stat_test=stat_test_permutation,
        params={"top_k": 20, "min_brokers": 3},
    ),

    "exodus": HypothesisConfig(
        name="exodus",
        display_name="集體撤退",
        description="Multiple top brokers simultaneously reducing positions",
        selector=select_top_k_by_pnl,
        filter=filter_collective_exodus,
        outcome=outcome_forward_returns,
        baseline=baseline_unconditional,
        stat_test=stat_test_permutation,
        params={"top_k": 20, "min_brokers": 5},
    ),

    "cross_stock": HypothesisConfig(
        name="cross_stock",
        display_name="跨股資訊流",
        description="Activity in stock A predicts returns in stock B",
        selector=select_top_k_by_pnl,
        filter=filter_large_trades,
        outcome=outcome_cross_stock_returns,
        baseline=baseline_cross_stock_unconditional,
        stat_test=stat_test_permutation,
        params={"top_k": 20, "sigma": 2.0},
        # target_symbol must be set via params_override at runtime
    ),

    "ta_regime": HypothesisConfig(
        name="ta_regime",
        display_name="TA突變",
        description="Rolling timing alpha z-score breakout",
        selector=select_ta_regime_change,
        filter=filter_large_trades,
        outcome=outcome_forward_returns,
        baseline=baseline_unconditional,
        stat_test=stat_test_permutation,
        params={"window_days": 120, "z_threshold": 2.0, "sigma": 2.0},
    ),

    "contrarian_smart": HypothesisConfig(
        name="contrarian_smart",
        display_name="逆勢操作",
        description="Top brokers buying on panic days (drop > 2%)",
        selector=select_top_k_by_pnl,
        filter=filter_contrarian_on_panic,
        outcome=outcome_forward_returns,
        baseline=baseline_unconditional,
        stat_test=stat_test_permutation,
        params={"top_k": 20, "drop_pct": -0.02, "min_brokers": 3},
    ),

    "concentration": HypothesisConfig(
        name="concentration",
        display_name="持倉集中度",
        description="HHI of broker position weights breakout",
        selector=select_all_active_brokers,
        filter=filter_hhi_breakout,
        outcome=outcome_forward_returns,
        baseline=baseline_unconditional,
        stat_test=stat_test_permutation,
        params={"z_threshold": 2.0},
    ),

    "herding": HypothesisConfig(
        name="herding",
        display_name="券商群聚",
        description="Returns when top/bottom brokers agree vs disagree",
        selector=select_top_and_bottom_k,
        filter=filter_herding_agreement,
        outcome=outcome_forward_returns,
        baseline=baseline_disagreement_returns,
        stat_test=stat_test_permutation,
        params={"top_k": 20},
    ),
}


def get_strategy(name: str) -> HypothesisConfig:
    """Get a strategy config by name. Raises KeyError if not found."""
    return STRATEGIES[name]


def list_strategies() -> list[str]:
    """List all registered strategy names."""
    return list(STRATEGIES.keys())
