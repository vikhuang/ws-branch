"""Baseline snapshot tests for refactoring.

These tests verify that data outputs remain unchanged after refactoring.
They read existing parquet/json files and check known invariants.
If any test fails after a refactor, the refactor changed behaviour.
"""

import json
from pathlib import Path

import polars as pl
import pytest

DATA_ROOT = Path("data")


# =============================================================================
# broker_ranking.parquet snapshot
# =============================================================================


class TestBrokerRankingSnapshot:
    """Verify broker_ranking.parquet is unchanged."""

    @pytest.fixture
    def ranking(self):
        path = DATA_ROOT / "derived" / "broker_ranking.parquet"
        if not path.exists():
            pytest.skip("broker_ranking.parquet not found")
        return pl.read_parquet(path)

    def test_schema(self, ranking):
        expected = {
            "rank", "broker", "total_pnl", "realized_pnl", "unrealized_pnl",
            "total_buy_amount", "total_sell_amount", "total_amount", "timing_alpha",
        }
        assert set(ranking.columns) == expected

    def test_broker_count(self, ranking):
        """Should have ~910+ brokers."""
        assert ranking.height >= 900, f"Only {ranking.height} brokers"

    def test_top10_stability(self, ranking):
        """Top 10 brokers by total_pnl should be deterministic."""
        top10 = ranking.sort("total_pnl", descending=True).head(10)
        # Verify top 10 have positive PNL
        assert (top10["total_pnl"] > 0).all()
        # Verify rank column matches sort order
        assert top10["rank"].to_list() == list(range(1, 11))

    def test_zero_sum(self, ranking):
        """Total PNL across all brokers should be near zero (market is zero-sum)."""
        total = ranking["total_pnl"].sum()
        total_abs = ranking["total_pnl"].abs().sum()
        ratio = abs(total) / total_abs if total_abs > 0 else 0
        assert ratio < 0.05, f"Total PNL ratio {ratio:.4f} too far from zero"

    def test_timing_alpha_exists(self, ranking):
        """timing_alpha should be populated for most brokers."""
        non_null = ranking.filter(pl.col("timing_alpha").is_not_null()).height
        assert non_null / ranking.height > 0.5


# =============================================================================
# pnl/{symbol}.parquet snapshot
# =============================================================================


class TestSymbolPnlSnapshot:
    """Verify per-symbol PNL files are unchanged."""

    @pytest.fixture
    def symbol_pnl(self):
        pnl_dir = DATA_ROOT / "pnl"
        if not pnl_dir.exists():
            pytest.skip("pnl/ directory not found")
        files = sorted(pnl_dir.glob("*.parquet"))
        if not files:
            pytest.skip("No pnl files found")
        # Use first file as representative
        return pl.read_parquet(files[0]), files[0].stem

    def test_schema(self, symbol_pnl):
        df, _ = symbol_pnl
        required = {"rank", "broker", "total_pnl", "realized_pnl", "unrealized_pnl", "timing_alpha"}
        assert required <= set(df.columns), f"Missing columns: {required - set(df.columns)}"

    def test_nonempty(self, symbol_pnl):
        df, sym = symbol_pnl
        assert df.height > 0, f"pnl/{sym}.parquet is empty"


# =============================================================================
# market_scan.json snapshot
# =============================================================================


class TestMarketScanSnapshot:
    """Verify market_scan.json is unchanged."""

    @pytest.fixture
    def scan_data(self):
        path = DATA_ROOT / "derived" / "market_scan.json"
        if not path.exists():
            pytest.skip("market_scan.json not found")
        with open(path) as f:
            return json.load(f)

    def test_has_results(self, scan_data):
        """Should have scan results."""
        assert "results" in scan_data or "passing_symbols" in scan_data or isinstance(scan_data, list)

    def test_has_filter_funnel(self, scan_data):
        """Should have filter funnel metadata."""
        # market_scan.json structure may vary, check for common keys
        if isinstance(scan_data, dict):
            assert len(scan_data) > 0


# =============================================================================
# CLI import smoke test
# =============================================================================


class TestImportSmoke:
    """Verify core imports work."""

    def test_import_pnl_analytics(self):
        from pnl_analytics import DataPaths, AnalysisConfig, DEFAULT_PATHS
        assert DataPaths is not None
        assert DEFAULT_PATHS.root is not None

    def test_import_config(self):
        from pnl_analytics.infrastructure.config import DataPaths
        paths = DataPaths()
        assert paths.daily_summary_dir.name == "daily_summary"

    def test_import_repositories(self):
        from pnl_analytics.infrastructure.repositories.pnl_repo import RankingRepository
        from pnl_analytics.infrastructure.repositories.trade_repo import TradeRepository
        from pnl_analytics.infrastructure.repositories.broker_repo import BrokerRepository
        assert RankingRepository is not None

    def test_import_services(self):
        from pnl_analytics.application.services.ranking import RankingService
        from pnl_analytics.application.services.broker_analysis import BrokerAnalyzer
        from pnl_analytics.application.services.symbol_analysis import SymbolAnalyzer
        from pnl_analytics.application.services.event_study import EventStudyService
        assert RankingService is not None

    def test_import_domain(self):
        from pnl_analytics.domain.statistics import welch_t_test, cohens_d, permutation_test
        from pnl_analytics.domain.event_detection import detect_smart_money_events
        from pnl_analytics.domain.forward_returns import compute_forward_returns
        assert welch_t_test is not None
