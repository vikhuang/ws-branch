"""Tests for the composable hypothesis testing framework.

All tests use synthetic data -- no file I/O.
"""

import numpy as np
import polars as pl
import pytest
from datetime import date, timedelta


from broker_analytics.domain.hypothesis.types import (
    SymbolData,
    GlobalContext,
    HypothesisConfig,
    HypothesisResult,
    HorizonDetail,
)
from broker_analytics.domain.hypothesis.position import derive_positions
from broker_analytics.domain.hypothesis.selectors import (
    select_top_k_by_pnl,
    select_niche_top_brokers,
    select_concentrated_brokers,
)
from broker_analytics.domain.hypothesis.filters import (
    filter_large_trades,
    filter_collective_exodus,
    filter_herding_divergence,
)
from broker_analytics.domain.hypothesis.outcomes import outcome_forward_returns
from broker_analytics.domain.hypothesis.baselines import baseline_unconditional
from broker_analytics.domain.hypothesis.stat_tests import (
    stat_test_parametric,
    stat_test_permutation,
)
from broker_analytics.domain.hypothesis.registry import (
    STRATEGIES,
    get_strategy,
    list_strategies,
)


# =============================================================================
# Test Fixtures
# =============================================================================

def _make_dates(n: int = 60) -> list[date]:
    """Generate n trading dates starting from 2024-01-02."""
    start = date(2024, 1, 2)
    dates = []
    d = start
    while len(dates) < n:
        if d.weekday() < 5:  # skip weekends
            dates.append(d)
        d += timedelta(days=1)
    return dates


def _make_prices(dates: list[date], symbol: str = "TEST") -> pl.DataFrame:
    """Generate synthetic prices with a slight uptrend."""
    n = len(dates)
    np.random.seed(42)
    base = 100.0
    returns = np.random.normal(0.001, 0.02, n)
    prices = [base]
    for r in returns[1:]:
        prices.append(prices[-1] * (1 + r))
    return pl.DataFrame({
        "symbol_id": [symbol] * n,
        "date": dates,
        "close_price": prices[:n],
    })


def _make_trade_df(dates: list[date], brokers: list[str]) -> pl.DataFrame:
    """Generate synthetic daily_summary trades."""
    np.random.seed(123)
    rows = []
    for d in dates:
        for b in brokers:
            buy = int(np.random.randint(0, 500))
            sell = int(np.random.randint(0, 500))
            rows.append({
                "broker": b,
                "date": d,
                "buy_shares": buy,
                "sell_shares": sell,
                "buy_amount": float(buy * 100),
                "sell_amount": float(sell * 100),
            })
    return pl.DataFrame(rows).with_columns(pl.col("broker").cast(pl.Categorical))


def _make_pnl_daily(dates: list[date], brokers: list[str]) -> pl.DataFrame:
    """Generate synthetic pnl_daily data."""
    np.random.seed(456)
    rows = []
    for b in brokers:
        cum_rpnl = 0.0
        for d in dates:
            rpnl = float(np.random.normal(0, 10000))
            cum_rpnl += rpnl
            rows.append({
                "broker": b,
                "date": d,
                "realized_pnl": rpnl,
                "unrealized_pnl": float(np.random.normal(0, 50000)),
            })
    return pl.DataFrame(rows)


def _make_pnl_ranking(brokers: list[str]) -> pl.DataFrame:
    """Generate synthetic per-stock PNL ranking."""
    np.random.seed(789)
    pnls = sorted(np.random.normal(0, 100000, len(brokers)), reverse=True)
    return pl.DataFrame({
        "rank": list(range(1, len(brokers) + 1)),
        "broker": brokers,
        "total_pnl": pnls,
        "realized_pnl": [p * 0.8 for p in pnls],
        "unrealized_pnl": [p * 0.2 for p in pnls],
        "timing_alpha": [float(np.random.normal(0, 0.5)) for _ in brokers],
    }).cast({"rank": pl.UInt32})


def _make_global_ranking(n: int = 50) -> pl.DataFrame:
    """Generate synthetic global broker ranking."""
    np.random.seed(321)
    brokers = [f"B{i:03d}" for i in range(n)]
    pnls = sorted(np.random.normal(0, 1e9, n), reverse=True)
    amounts = [abs(p) * 10 for p in pnls]  # total_amount for contrast selector
    return pl.DataFrame({
        "rank": list(range(1, n + 1)),
        "broker": brokers,
        "total_pnl": pnls,
        "total_amount": amounts,
    }).cast({"rank": pl.UInt32})


def _make_symbol_data(
    n_dates: int = 60, brokers: list[str] | None = None, symbol: str = "TEST",
) -> SymbolData:
    """Create a complete SymbolData for testing."""
    if brokers is None:
        brokers = [f"B{i:03d}" for i in range(10)]
    dates = _make_dates(n_dates)
    return SymbolData(
        symbol=symbol,
        trade_df=_make_trade_df(dates, brokers),
        pnl_daily_df=_make_pnl_daily(dates, brokers),
        pnl_df=_make_pnl_ranking(brokers),
        prices=_make_prices(dates, symbol),
    )


def _make_global_context() -> GlobalContext:
    """Create a GlobalContext for testing."""
    return GlobalContext(
        global_ranking=_make_global_ranking(50),
        all_symbols=["TEST"],
        prices=_make_prices(_make_dates(60), "TEST"),
    )


# =============================================================================
# Position Derivation Tests
# =============================================================================

class TestPositionDerivation:
    """Tests for derive_positions (Plan A)."""

    def test_basic_long_position(self):
        """Buy shares → positive net_shares."""
        dates = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]
        trade_df = pl.DataFrame({
            "broker": ["B001"] * 3,
            "date": dates,
            "buy_shares": [100, 50, 0],
            "sell_shares": [0, 0, 30],
            "buy_amount": [10000.0, 5000.0, 0.0],
            "sell_amount": [0.0, 0.0, 3000.0],
        }).with_columns(pl.col("broker").cast(pl.Categorical))

        pnl_daily = pl.DataFrame({
            "broker": ["B001"] * 3,
            "date": dates,
            "realized_pnl": [0.0, 0.0, 100.0],
            "unrealized_pnl": [500.0, 1200.0, 900.0],
        })

        prices = pl.DataFrame({
            "symbol_id": ["TEST"] * 3,
            "date": dates,
            "close_price": [100.0, 112.0, 107.5],
        })

        pos = derive_positions(trade_df, pnl_daily, prices, "TEST")

        assert len(pos) == 3
        assert pos["net_shares"].to_list() == [100, 150, 120]
        # avg_cost = price - unrealized_pnl / net_shares
        expected_costs = [
            100.0 - 500.0 / 100,    # 95.0
            112.0 - 1200.0 / 150,   # 104.0
            107.5 - 900.0 / 120,    # 100.0
        ]
        actual_costs = pos["avg_cost"].to_list()
        for e, a in zip(expected_costs, actual_costs):
            assert abs(e - a) < 1e-6

    def test_zero_shares_excluded(self):
        """Days with net_shares=0 are excluded from output."""
        dates = [date(2024, 1, 2), date(2024, 1, 3)]
        trade_df = pl.DataFrame({
            "broker": ["B001"] * 2,
            "date": dates,
            "buy_shares": [100, 0],
            "sell_shares": [0, 100],
            "buy_amount": [10000.0, 0.0],
            "sell_amount": [0.0, 10000.0],
        }).with_columns(pl.col("broker").cast(pl.Categorical))

        pnl_daily = pl.DataFrame({
            "broker": ["B001"] * 2,
            "date": dates,
            "realized_pnl": [0.0, 500.0],
            "unrealized_pnl": [200.0, 0.0],
        })

        prices = pl.DataFrame({
            "symbol_id": ["TEST"] * 2,
            "date": dates,
            "close_price": [100.0, 102.0],
        })

        pos = derive_positions(trade_df, pnl_daily, prices, "TEST")
        assert len(pos) == 1  # day 2 excluded (net_shares=0)
        assert pos["net_shares"][0] == 100


# =============================================================================
# Selector Tests
# =============================================================================

class TestSelectors:
    """Tests for Step 1 selector functions."""

    def test_top_k_returns_correct_count(self):
        data = _make_symbol_data()
        ctx = _make_global_context()
        brokers = select_top_k_by_pnl(data, ctx, {"top_k": 3})
        assert len(brokers) == 3
        assert all(isinstance(b, str) for b in brokers)

    def test_top_k_sorted_by_pnl(self):
        data = _make_symbol_data()
        ctx = _make_global_context()
        brokers = select_top_k_by_pnl(data, ctx, {"top_k": 5})
        # Should be in PNL descending order
        pnls = data.pnl_df.sort("total_pnl", descending=True).head(5)
        expected = pnls["broker"].cast(pl.Utf8).to_list()
        assert brokers == expected

    def test_niche_top_brokers(self):
        """Contrast score: high local PNL + low global PNL = information edge."""
        brokers = [f"B{i:03d}" for i in range(20)]
        data = _make_symbol_data(brokers=brokers)
        ctx = _make_global_context()

        result = select_niche_top_brokers(data, ctx, {
            "top_k": 5,
            "min_contrast": 0.1,
            "min_global_amount": 0,  # no amount filter for test
            "years": 3,
            "train_end_date": "2025-12-31",
        })
        assert isinstance(result, list)
        assert all(isinstance(b, str) for b in result)
        assert len(result) <= 5

    def test_concentrated_brokers_empty_without_data(self):
        """Without _broker_concentrations, returns empty list."""
        data = _make_symbol_data()
        ctx = _make_global_context()
        result = select_concentrated_brokers(data, ctx, {})
        assert result == []

    def test_concentrated_brokers_with_data(self):
        """Brokers with high concentration are selected."""
        data = _make_symbol_data()
        ctx = _make_global_context()
        concentrations = pl.DataFrame({
            "broker": ["B000", "B001", "B002"],
            "hhi": [0.5, 0.3, 0.1],
            "concentration_ratio": [0.6, 0.1, 0.05],
        })
        result = select_concentrated_brokers(
            data, ctx, {"_broker_concentrations": concentrations, "min_concentration": 0.3}
        )
        assert "B000" in result
        assert "B001" not in result


# =============================================================================
# Filter Tests
# =============================================================================

class TestFilters:
    """Tests for Step 2 filter functions."""

    def test_large_trade_schema(self):
        """Output must have [date, direction] columns."""
        data = _make_symbol_data()
        brokers = select_top_k_by_pnl(data, _make_global_context(), {"top_k": 5})
        events = filter_large_trades(data, brokers, {"sigma": 2.0})
        assert "date" in events.columns
        assert "direction" in events.columns
        assert events.schema["direction"] == pl.Int8

    def test_large_trade_directions(self):
        """Directions should only be +1 or -1."""
        data = _make_symbol_data()
        brokers = select_top_k_by_pnl(data, _make_global_context(), {"top_k": 5})
        events = filter_large_trades(data, brokers, {"sigma": 1.5})
        if len(events) > 0:
            dirs = set(events["direction"].to_list())
            assert dirs.issubset({1, -1})

    def test_exodus_returns_valid_direction(self):
        """Exodus events should have direction +1 or -1 (price-context dependent)."""
        data = _make_symbol_data(n_dates=200)
        brokers = select_top_k_by_pnl(data, _make_global_context(), {"top_k": 5})
        events = filter_collective_exodus(data, brokers, {"min_brokers": 2})
        if len(events) > 0:
            dirs = set(events["direction"].to_list())
            assert dirs.issubset({1, -1})

    def test_empty_brokers_returns_empty(self):
        """Empty broker list → empty events."""
        data = _make_symbol_data()
        events = filter_large_trades(data, [], {"sigma": 2.0})
        assert len(events) == 0


# =============================================================================
# Outcome Tests
# =============================================================================

class TestOutcomes:
    """Tests for Step 3 outcome functions."""

    def test_forward_returns_shape(self):
        """Returns dict with expected horizons."""
        data = _make_symbol_data()
        events = pl.DataFrame({
            "date": [date(2024, 1, 5), date(2024, 1, 12)],
            "direction": [1, -1],
        }).cast({"direction": pl.Int8})

        result = outcome_forward_returns(data, events, {"horizons": (1, 5)})
        assert 1 in result
        assert 5 in result
        assert isinstance(result[1], np.ndarray)

    def test_empty_events_returns_empty_arrays(self):
        data = _make_symbol_data()
        events = pl.DataFrame(schema={"date": pl.Date, "direction": pl.Int8})
        result = outcome_forward_returns(data, events, {"horizons": (1, 5, 10, 20)})
        for h in (1, 5, 10, 20):
            assert len(result[h]) == 0


# =============================================================================
# Stat Test Tests
# =============================================================================

class TestStatTests:
    """Tests for Step 5 stat test functions."""

    def test_parametric_returns_per_horizon(self):
        event_ret = {1: np.random.normal(50, 100, 50), 5: np.random.normal(80, 150, 50)}
        baseline_ret = {1: np.random.normal(0, 100, 1000), 5: np.random.normal(0, 150, 1000)}
        result = stat_test_parametric(event_ret, baseline_ret, {})
        assert 1 in result
        assert 5 in result
        assert hasattr(result[1], "t_stat")
        assert hasattr(result[1], "significant")

    def test_insufficient_data(self):
        """< 3 samples → empty result."""
        event_ret = {1: np.array([1.0, 2.0])}
        baseline_ret = {1: np.array([0.0, 0.0])}
        result = stat_test_parametric(event_ret, baseline_ret, {})
        assert result[1].significant is False
        assert result[1].p_value == 1.0

    def test_permutation_basic(self):
        """Permutation test runs without error."""
        np.random.seed(42)
        event_ret = {1: np.random.normal(100, 50, 30)}
        baseline_ret = {1: np.random.normal(0, 50, 500)}
        result = stat_test_permutation(event_ret, baseline_ret, {"n_perms": 100})
        assert 1 in result
        assert hasattr(result[1], "significant")


# =============================================================================
# Registry Tests
# =============================================================================

class TestRegistry:
    """Tests for strategy registry."""

    def test_ten_strategies(self):
        assert len(STRATEGIES) == 10

    def test_list_strategies(self):
        names = list_strategies()
        assert len(names) == 10
        assert "contrarian_broker" in names
        assert "herding" in names

    def test_get_strategy(self):
        cfg = get_strategy("contrarian_broker")
        assert cfg.name == "contrarian_broker"
        assert cfg.display_name == "反差券商"
        assert callable(cfg.selector)
        assert callable(cfg.filter)

    def test_get_strategy_invalid(self):
        with pytest.raises(KeyError):
            get_strategy("nonexistent")

    def test_all_strategies_have_required_fields(self):
        for name, cfg in STRATEGIES.items():
            assert cfg.name == name
            assert len(cfg.display_name) > 0
            assert len(cfg.description) > 0
            assert callable(cfg.selector)
            assert callable(cfg.filter)
            assert callable(cfg.outcome)
            assert callable(cfg.baseline)
            assert callable(cfg.stat_test)


# =============================================================================
# Integration: Full Pipeline (Synthetic Data)
# =============================================================================

class TestPipelineIntegration:
    """End-to-end pipeline test with synthetic data."""

    def test_full_pipeline_contrarian(self):
        """Run a complete 5-step pipeline with synthetic data."""
        data = _make_symbol_data(n_dates=100)
        ctx = _make_global_context()
        config = get_strategy("contrarian_broker")
        params = {**config.params, "horizons": config.horizons}

        # Step 1
        brokers = config.selector(data, ctx, params)
        # May be empty with synthetic data, that's ok
        if not brokers:
            return

        # Step 2
        events = config.filter(data, brokers, params)
        if len(events) == 0:
            return

        # Step 3
        outcome = config.outcome(data, events, params)
        assert isinstance(outcome, dict)

        # Step 4
        baseline = config.baseline(data, events, params)
        assert isinstance(baseline, dict)

        # Step 5
        results = config.stat_test(outcome, baseline, params)
        assert isinstance(results, dict)
        for h, r in results.items():
            assert hasattr(r, "significant")

    def test_full_pipeline_top_k(self):
        """Run pipeline with top_k selector (strategies 3, 4, 7)."""
        data = _make_symbol_data(n_dates=100)
        ctx = _make_global_context()

        brokers = select_top_k_by_pnl(data, ctx, {"top_k": 3})
        assert len(brokers) == 3

        events = filter_large_trades(data, brokers, {"sigma": 1.5})
        # Just verify schema
        assert "date" in events.columns
        assert "direction" in events.columns

    def test_herding_pipeline(self):
        """Run herding pipeline with smart money vs crowd."""
        data = _make_symbol_data(n_dates=100)
        ctx = _make_global_context()
        config = get_strategy("herding")
        params = {**config.params, "horizons": config.horizons}

        brokers = config.selector(data, ctx, params)
        assert len(brokers) > 0  # top-K smart money

        events = config.filter(data, brokers, params)
        # Events may be empty, that's ok for synthetic data
        assert "date" in events.columns
        assert "direction" in events.columns


# =============================================================================
# Permutation Acceleration Tests
# =============================================================================

class TestPermutationAcceleration:
    """Tests for the A+B+C acceleration in stat_test_permutation."""

    def test_cohens_d_prefilter_skips_permutation(self):
        """A: When |d| < 0.2, permutation is skipped and result is not significant."""
        rng = np.random.default_rng(42)
        # Two nearly identical distributions → |d| ≈ 0
        a = rng.normal(100, 10, 200)
        b = rng.normal(100.5, 10, 10000)  # tiny difference
        event_returns = {1: a, 5: a}
        baseline_returns = {1: b, 5: b}

        results = stat_test_permutation(event_returns, baseline_returns, {})
        for h, r in results.items():
            assert abs(r.cohens_d) < 0.2
            assert r.p_value == 1.0  # sentinel for d-prefilter
            assert not r.significant

    def test_welch_prefilter_skips_permutation(self):
        """B: When |d| >= 0.2 but Welch p > 0.2, permutation is skipped."""
        rng = np.random.default_rng(99)
        # Small sample with moderate effect → |d| > 0.2 but Welch p >> 0.05
        a = rng.normal(105, 15, 5)   # very few samples → large p
        b = rng.normal(100, 10, 100)
        from broker_analytics.domain.statistics import cohens_d, welch_t_test
        d = cohens_d(a, b)
        _, t_p = welch_t_test(a, b)

        if abs(d) >= 0.2 and t_p > 0.2:
            event_returns = {1: a}
            baseline_returns = {1: b}
            results = stat_test_permutation(event_returns, baseline_returns, {})
            r = results[1]
            assert r.p_value == t_p  # uses Welch p as proxy
            assert not r.significant

    def test_adaptive_early_stop_not_significant(self):
        """C: Adaptive permutation stops early for clearly non-significant data."""
        from broker_analytics.domain.statistics import permutation_test_adaptive
        rng = np.random.default_rng(42)
        a = rng.normal(100, 10, 50)
        b = rng.normal(100, 10, 10000)

        # With alpha=0.0125 (Bonferroni for 4 horizons), clearly not significant
        p = permutation_test_adaptive(a, b, n_perms=10000, alpha=0.0125)
        assert p > 0.05  # should be clearly not significant

    def test_adaptive_early_stop_significant(self):
        """C: Adaptive permutation stops early for clearly significant data."""
        from broker_analytics.domain.statistics import permutation_test_adaptive
        rng = np.random.default_rng(42)
        a = rng.normal(110, 5, 100)   # clearly shifted
        b = rng.normal(100, 10, 10000)

        p = permutation_test_adaptive(a, b, n_perms=10000, alpha=0.0125)
        assert p < 0.01  # should be clearly significant

    def test_adaptive_matches_full_for_borderline(self):
        """Adaptive and full permutation agree on borderline cases."""
        from broker_analytics.domain.statistics import (
            permutation_test, permutation_test_adaptive,
        )
        rng = np.random.default_rng(77)
        a = rng.normal(101, 10, 50)
        b = rng.normal(100, 10, 5000)

        p_full = permutation_test(a, b, n_perms=10000, seed=42)
        p_adaptive = permutation_test_adaptive(
            a, b, n_perms=10000, seed=42, alpha=0.0125,
        )
        # Both should reach the same conclusion (sig or not)
        # p values may differ slightly due to early stop, but direction matches
        assert (p_full < 0.05) == (p_adaptive < 0.05)

    def test_acceleration_preserves_conclusion(self):
        """Full pipeline: accelerated stat_test_permutation gives same conclusion."""
        data = _make_symbol_data(n_dates=100)
        ctx = _make_global_context()

        config = get_strategy("contrarian_broker")
        params = {**config.params, "horizons": config.horizons}

        brokers = config.selector(data, ctx, params)
        if not brokers:
            pytest.skip("No brokers selected from synthetic data")

        events = config.filter(data, brokers, params)
        if len(events) == 0:
            pytest.skip("No events from synthetic data")

        event_returns = config.outcome(data, events, params)
        baseline_returns = config.baseline(data, events, params)

        # Run accelerated version
        results = stat_test_permutation(event_returns, baseline_returns, params)
        for h, r in results.items():
            assert hasattr(r, "significant")
            assert hasattr(r, "cohens_d")
            assert hasattr(r, "p_value")


# =============================================================================
# Strategy 0: Large Trade SCAR Tests
# =============================================================================

class TestLargeTradeScar:
    """Tests for Strategy 0 (large_trade_scar)."""

    def test_selector_basic(self):
        """Selector returns brokers sorted by SCAR in training window."""
        from broker_analytics.domain.hypothesis.selectors import select_by_large_trade_scar

        # Create data spanning train + test windows
        # Train: 2023-01 to 2023-12, Test: 2024-01+
        train_dates = [date(2023, 1, 1) + timedelta(days=i) for i in range(300)]
        test_dates = [date(2024, 1, 1) + timedelta(days=i) for i in range(100)]
        all_dates = train_dates + test_dates
        brokers = [f"B{i:03d}" for i in range(5)]

        data = _make_symbol_data(n_dates=len(all_dates), brokers=brokers)
        # Override with our date range
        np.random.seed(42)
        rows = []
        for d in all_dates:
            for b in brokers:
                buy = int(np.random.randint(0, 500))
                sell = int(np.random.randint(0, 500))
                rows.append({
                    "broker": b, "date": d,
                    "buy_shares": buy, "sell_shares": sell,
                    "buy_amount": float(buy * 100000),  # large amounts
                    "sell_amount": float(sell * 100000),
                })
        trade_df = pl.DataFrame(rows).with_columns(
            pl.col("broker").cast(pl.Categorical)
        )

        prices = _make_prices(all_dates, "TEST")
        data = SymbolData(
            symbol="TEST",
            trade_df=trade_df,
            pnl_daily_df=_make_pnl_daily(all_dates, brokers),
            pnl_df=_make_pnl_ranking(brokers),
            prices=prices,
        )
        ctx = GlobalContext(
            global_ranking=_make_global_ranking(50),
            all_symbols=["TEST"],
            prices=prices,
        )

        params = {
            "train_end_date": "2023-12-31",
            "test_start_date": "2024-01-01",
            "sigma": 1.5,  # lower threshold to get more events
            "top_k": 3,
            "min_events": 2,
            "min_amount": 1000,  # low threshold for synthetic data
            "horizons": (5, 10, 20),
        }

        result = select_by_large_trade_scar(data, ctx, params)
        assert isinstance(result, list)
        assert len(result) <= 3
        # All should be strings
        for b in result:
            assert isinstance(b, str)

    def test_selector_etf_returns_empty(self):
        """ETF symbols (starting with '00') return empty list."""
        from broker_analytics.domain.hypothesis.selectors import select_by_large_trade_scar

        data = _make_symbol_data(symbol="0050")
        ctx = _make_global_context()
        params = {"horizons": (5, 10, 20, 60)}

        result = select_by_large_trade_scar(data, ctx, params)
        assert result == []

    def test_filter_test_window_only(self):
        """Filter only returns events from test window."""
        from broker_analytics.domain.hypothesis.filters import filter_large_trades_test_window

        train_dates = [date(2023, 6, 1) + timedelta(days=i) for i in range(200)]
        test_dates = [date(2024, 1, 1) + timedelta(days=i) for i in range(100)]
        all_dates = train_dates + test_dates
        brokers = ["B001", "B002", "B003"]
        test_start = date(2024, 1, 1)

        np.random.seed(55)
        rows = []
        for d in all_dates:
            for b in brokers:
                buy = int(np.random.randint(0, 500))
                sell = int(np.random.randint(0, 500))
                rows.append({
                    "broker": b, "date": d,
                    "buy_shares": buy, "sell_shares": sell,
                    "buy_amount": float(buy * 100000),
                    "sell_amount": float(sell * 100000),
                })
        trade_df = pl.DataFrame(rows).with_columns(
            pl.col("broker").cast(pl.Categorical)
        )

        data = SymbolData(
            symbol="TEST",
            trade_df=trade_df,
            pnl_daily_df=_make_pnl_daily(all_dates, brokers),
            pnl_df=_make_pnl_ranking(brokers),
            prices=_make_prices(all_dates, "TEST"),
        )

        params = {
            "test_start_date": "2024-01-01",
            "sigma": 1.5,
            "min_amount": 1000,
        }

        events = filter_large_trades_test_window(data, brokers, params)
        assert "date" in events.columns
        assert "direction" in events.columns
        # All event dates must be >= test_start
        if len(events) > 0:
            min_date = events["date"].min()
            assert min_date >= test_start

    def test_registry_entry_exists(self):
        """Strategy 0 is registered with correct config."""
        cfg = get_strategy("large_trade_scar")
        assert cfg.name == "large_trade_scar"
        assert cfg.display_name == "大單預測力"
        assert cfg.horizons == (5, 10, 20, 60)
        assert cfg.params["min_amount"] == 10_000_000
        assert callable(cfg.selector)
        assert callable(cfg.filter)
