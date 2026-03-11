"""Strategy registry: Maps strategy names to HypothesisConfig instances.

Each of the 10 strategies is defined here as a concrete composition
of selector + filter + outcome + baseline + stat_test.
"""

from broker_analytics.domain.hypothesis.types import HypothesisConfig
from broker_analytics.domain.hypothesis.selectors import (
    select_by_large_trade_scar,
    select_contrarian_brokers,
    select_dual_window_intersection,
    select_top_k_by_pnl,
    select_ta_regime_change,
    select_concentrated_brokers,
)
from broker_analytics.domain.hypothesis.filters import (
    filter_large_trades,
    filter_large_trades_test_window,
    filter_conviction_signals,
    filter_collective_exodus,
    filter_contrarian_on_panic,
    filter_cluster_accumulation,
    filter_concentration_increase,
    filter_herding_divergence,
)
from broker_analytics.domain.hypothesis.outcomes import (
    outcome_forward_returns,
)
from broker_analytics.domain.hypothesis.baselines import (
    baseline_unconditional,
)
from broker_analytics.domain.hypothesis.stat_tests import (
    stat_test_permutation,
)


STRATEGIES: dict[str, HypothesisConfig] = {

    "large_trade_scar": HypothesisConfig(
        name="large_trade_scar",
        display_name="大單預測力",
        description="Training-window SCAR selects skilled brokers; test-window validates",
        selector=select_by_large_trade_scar,
        filter=filter_large_trades_test_window,
        outcome=outcome_forward_returns,
        baseline=baseline_unconditional,
        stat_test=stat_test_permutation,
        params={
            "top_k": 20,
            "sigma": 2.0,
            "min_events": 5,
            "min_amount": 10_000_000,
            "train_end_date": "2023-12-31",
            "test_start_date": "2024-01-01",
        },
        horizons=(5, 10, 20, 60),
    ),

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
        params={"top_k": 20, "min_brokers": 3, "min_profit_ratio": 0.2},
    ),

    "exodus": HypothesisConfig(
        name="exodus",
        display_name="集體撤退",
        description="Multiple top brokers exiting or significantly reducing positions (rolling window)",
        selector=select_top_k_by_pnl,
        filter=filter_collective_exodus,
        outcome=outcome_forward_returns,
        baseline=baseline_unconditional,
        stat_test=stat_test_permutation,
        params={"top_k": 20, "min_brokers": 5, "window_days": 20, "reduction_pct": 0.5},
    ),

    "cross_stock": HypothesisConfig(
        name="cross_stock",
        display_name="跨股資訊流",
        description="Same broker with large trades across multiple cluster stocks simultaneously",
        selector=select_top_k_by_pnl,
        filter=filter_cluster_accumulation,
        outcome=outcome_forward_returns,
        baseline=baseline_unconditional,
        stat_test=stat_test_permutation,
        params={"top_k": 20, "sigma": 2.0, "min_cluster_stocks": 2},
        # cluster must be set via params_override at runtime (e.g. --params cluster=2330,3711)
    ),

    "ta_regime": HypothesisConfig(
        name="ta_regime",
        display_name="TA突變",
        description="Broker's recent timing alpha breaks out vs own historical TA (temporal z-score)",
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
        description="Top brokers buying on panic days (single-day >2% or 3-day >5% drop)",
        selector=select_top_k_by_pnl,
        filter=filter_contrarian_on_panic,
        outcome=outcome_forward_returns,
        baseline=baseline_unconditional,
        stat_test=stat_test_permutation,
        params={"top_k": 20, "drop_pct": -0.02, "cum_drop_pct": -0.05, "min_brokers": 3},
    ),

    "concentration": HypothesisConfig(
        name="concentration",
        display_name="持倉集中度",
        description="Brokers with high cross-stock portfolio concentration adding to position",
        selector=select_concentrated_brokers,
        filter=filter_concentration_increase,
        outcome=outcome_forward_returns,
        baseline=baseline_unconditional,
        stat_test=stat_test_permutation,
        params={"min_concentration": 0.3, "min_brokers": 2},
    ),

    "herding": HypothesisConfig(
        name="herding",
        display_name="券商群聚",
        description="Crowd vs smart money divergence: herding index signals",
        selector=select_top_k_by_pnl,
        filter=filter_herding_divergence,
        outcome=outcome_forward_returns,
        baseline=baseline_unconditional,
        stat_test=stat_test_permutation,
        params={"top_k": 20, "herding_threshold": 0.3},
    ),
}


def get_strategy(name: str) -> HypothesisConfig:
    """Get a strategy config by name. Raises KeyError if not found."""
    return STRATEGIES[name]


def list_strategies() -> list[str]:
    """List all registered strategy names."""
    return list(STRATEGIES.keys())
