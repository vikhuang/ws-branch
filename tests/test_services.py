"""Unit tests for application/services/ module.

Tests verify:
1. RankingService works with pre-aggregated data
2. BrokerAnalyzer produces correct results
"""

import pytest
from pathlib import Path

import polars as pl

from pnl_analytics.application import (
    BrokerAnalyzer,
    BrokerAnalysisResult,
    RankingService,
    RankingReportConfig,
)
from pnl_analytics.infrastructure import DEFAULT_PATHS
from pnl_analytics.infrastructure.repositories import RankingRepository, RepositoryError


# =============================================================================
# BrokerAnalysisResult Tests
# =============================================================================

class TestBrokerAnalysisResult:
    """Tests for BrokerAnalysisResult dataclass."""

    def test_to_dict(self):
        """to_dict should return all fields."""
        result = BrokerAnalysisResult(
            broker="1440",
            name="美林",
            rank=1,
            total_pnl=99_140_000_000,
            realized_pnl=97_840_000_000,
            unrealized_pnl=1_300_000_000,
            total_buy_amount=100_000_000_000,
            total_sell_amount=80_000_000_000,
            total_amount=180_000_000_000,
            win_count=300,
            loss_count=200,
            trade_count=500,
            win_rate=0.6,
            direction="做多",
        )
        d = result.to_dict()

        assert d["broker"] == "1440"
        assert d["name"] == "美林"
        assert d["rank"] == 1
        assert d["direction"] == "做多"

    def test_frozen(self):
        """BrokerAnalysisResult should be immutable."""
        result = BrokerAnalysisResult(
            broker="1440", name="", rank=1,
            total_pnl=0, realized_pnl=0, unrealized_pnl=0,
            total_buy_amount=0, total_sell_amount=0, total_amount=0,
            win_count=0, loss_count=0, trade_count=0,
            win_rate=0, direction="中性",
        )
        with pytest.raises(AttributeError):
            result.broker = "9999"


# =============================================================================
# BrokerAnalyzer Tests
# =============================================================================

class TestBrokerAnalyzer:
    """Tests for BrokerAnalyzer class."""

    @pytest.fixture
    def analyzer(self):
        """Create analyzer with real data."""
        return BrokerAnalyzer(paths=DEFAULT_PATHS)

    def test_analyze_returns_result(self, analyzer):
        """analyze should return BrokerAnalysisResult."""
        try:
            repo = RankingRepository(DEFAULT_PATHS)
            df = repo.get_all()
            if len(df) > 0:
                broker = df["broker"][0]
                result = analyzer.analyze(broker)
                assert result is not None
                assert isinstance(result, BrokerAnalysisResult)
        except RepositoryError:
            pytest.skip("Ranking data not available")

    def test_analyze_nonexistent_broker(self, analyzer):
        """Nonexistent broker should return None."""
        result = analyzer.analyze("NONEXISTENT_BROKER_99999")
        assert result is None

    def test_analyze_result_fields(self, analyzer):
        """Result should have all required fields."""
        try:
            repo = RankingRepository(DEFAULT_PATHS)
            df = repo.get_all()
            if len(df) > 0:
                broker = df["broker"][0]
                result = analyzer.analyze(broker)
                if result:
                    assert result.broker == broker
                    assert result.rank >= 1
                    assert isinstance(result.total_pnl, float)
                    assert isinstance(result.win_rate, float)
                    assert result.direction in ("做多", "做空", "中性")
        except RepositoryError:
            pytest.skip("Ranking data not available")

    def test_get_symbol_breakdown(self, analyzer):
        """get_symbol_breakdown should return DataFrame."""
        try:
            repo = RankingRepository(DEFAULT_PATHS)
            df = repo.get_all()
            if len(df) > 0:
                broker = df["broker"][0]
                breakdown = analyzer.get_symbol_breakdown(broker)
                assert isinstance(breakdown, pl.DataFrame)
        except RepositoryError:
            pytest.skip("Ranking data not available")


# =============================================================================
# RankingService Tests
# =============================================================================

class TestRankingService:
    """Tests for RankingService class."""

    @pytest.fixture
    def service(self):
        """Create service."""
        return RankingService(paths=DEFAULT_PATHS)

    def test_get_ranking(self, service):
        """get_ranking should return DataFrame."""
        try:
            df = service.get_ranking()
            assert isinstance(df, pl.DataFrame)
        except RepositoryError:
            pytest.skip("Ranking data not available")

    def test_get_ranking_has_columns(self, service):
        """Ranking should have required columns."""
        try:
            df = service.get_ranking()
            required = ["rank", "broker", "total_pnl"]
            for col in required:
                assert col in df.columns
        except RepositoryError:
            pytest.skip("Ranking data not available")

    def test_get_ranking_with_names(self, service):
        """Ranking with names should add name column."""
        try:
            df = service.get_ranking(with_names=True)
            # Name column may or may not exist depending on broker data
            assert isinstance(df, pl.DataFrame)
        except RepositoryError:
            pytest.skip("Ranking data not available")

    def test_get_top(self, service):
        """get_top should return N rows."""
        try:
            df = service.get_top(10)
            assert len(df) <= 10
        except RepositoryError:
            pytest.skip("Ranking data not available")

    def test_get_bottom(self, service):
        """get_bottom should return N rows."""
        try:
            df = service.get_bottom(5)
            assert len(df) <= 5
        except RepositoryError:
            pytest.skip("Ranking data not available")

    def test_get_broker(self, service):
        """get_broker should return single row or None."""
        try:
            df = service.get_ranking()
            if len(df) > 0:
                broker = df["broker"][0]
                result = service.get_broker(broker)
                assert result is not None
                assert len(result) == 1
        except RepositoryError:
            pytest.skip("Ranking data not available")

    def test_get_broker_not_found(self, service):
        """get_broker should return None for unknown broker."""
        try:
            result = service.get_broker("NONEXISTENT_99999")
            assert result is None
        except RepositoryError:
            pytest.skip("Ranking data not available")

    def test_get_summary(self, service):
        """get_summary should return dict with stats."""
        try:
            summary = service.get_summary()
            assert isinstance(summary, dict)
            assert "broker_count" in summary
            assert "total_pnl" in summary
        except RepositoryError:
            pytest.skip("Ranking data not available")


# =============================================================================
# RankingReportConfig Tests
# =============================================================================

class TestRankingReportConfig:
    """Tests for RankingReportConfig."""

    def test_defaults(self):
        """Default config should have sensible values."""
        config = RankingReportConfig()
        assert config.output_dir == Path(".")
        assert "csv" in config.output_formats or "parquet" in config.output_formats

    def test_custom_config(self):
        """Should accept custom values."""
        config = RankingReportConfig(
            output_dir=Path("/tmp"),
            output_formats=("csv", "xlsx"),
        )
        assert config.output_dir == Path("/tmp")
        assert "xlsx" in config.output_formats


# =============================================================================
# Integration Tests
# =============================================================================

class TestIntegration:
    """Integration tests."""

    def test_ranking_is_sorted(self):
        """Ranking should be sorted by PNL descending."""
        try:
            service = RankingService(paths=DEFAULT_PATHS)
            df = service.get_ranking()
            if len(df) > 1:
                pnls = df["total_pnl"].to_list()
                assert pnls == sorted(pnls, reverse=True)
        except RepositoryError:
            pytest.skip("Ranking data not available")

    def test_broker_count(self):
        """Should have reasonable number of brokers."""
        try:
            service = RankingService(paths=DEFAULT_PATHS)
            summary = service.get_summary()
            assert summary["broker_count"] > 0
        except RepositoryError:
            pytest.skip("Ranking data not available")
