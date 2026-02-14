"""Unit tests for infrastructure repositories.

Tests verify that repositories:
1. Load data correctly from files
2. Provide proper caching
3. Handle errors gracefully
"""

import pytest
from pathlib import Path
from datetime import date

from pnl_analytics.infrastructure import (
    DataPaths,
    DEFAULT_PATHS,
    RepositoryError,
)
from pnl_analytics.infrastructure.repositories import (
    TradeRepository,
    PriceRepository,
    BrokerRepository,
    RankingRepository,
)


# =============================================================================
# DataPaths Tests
# =============================================================================

class TestDataPaths:
    """Tests for DataPaths configuration."""

    def test_default_paths_exist(self):
        """Default paths should point to existing directory."""
        assert DEFAULT_PATHS.root.exists()

    def test_validate_returns_missing_files(self):
        """Validate should return list of missing paths."""
        fake_paths = DataPaths(root=Path("/nonexistent"))
        missing = fake_paths.validate()
        assert len(missing) > 0
        assert all(isinstance(m, str) for m in missing)

    def test_paths_are_consistent(self):
        """All paths should be relative to root directory."""
        paths = DEFAULT_PATHS
        assert paths.daily_summary_dir == paths.data_dir / "daily_summary"
        assert paths.close_prices == paths.price_dir / "close_prices.parquet"
        assert paths.broker_ranking == paths.derived_dir / "broker_ranking.parquet"

    def test_list_symbols(self):
        """list_symbols should return list of parquet files."""
        paths = DEFAULT_PATHS
        if paths.daily_summary_dir.exists():
            symbols = paths.list_symbols()
            assert isinstance(symbols, list)
            assert all(isinstance(s, str) for s in symbols)

    def test_symbol_trade_path(self):
        """symbol_trade_path should return correct path."""
        path = DEFAULT_PATHS.symbol_trade_path("2330")
        assert path == DEFAULT_PATHS.daily_summary_dir / "2330.parquet"


# =============================================================================
# TradeRepository Tests
# =============================================================================

class TestTradeRepository:
    """Tests for TradeRepository."""

    @pytest.fixture
    def repo(self):
        return TradeRepository(DEFAULT_PATHS)

    def test_list_symbols(self, repo):
        """list_symbols should return list of symbols."""
        symbols = repo.list_symbols()
        assert isinstance(symbols, list)

    def test_get_symbol_returns_dataframe(self, repo):
        """get_symbol should return a polars DataFrame."""
        import polars as pl
        symbols = repo.list_symbols()
        if symbols:
            df = repo.get_symbol(symbols[0])
            assert isinstance(df, pl.DataFrame)

    def test_get_symbol_has_required_columns(self, repo):
        """DataFrame should have required columns."""
        symbols = repo.list_symbols()
        if symbols:
            df = repo.get_symbol(symbols[0])
            required = ["broker", "date", "buy_shares", "sell_shares"]
            for col in required:
                assert col in df.columns, f"Missing column: {col}"

    def test_caching_works(self, repo):
        """Second call should return cached data."""
        symbols = repo.list_symbols()
        if symbols:
            df1 = repo.get_symbol(symbols[0])
            df2 = repo.get_symbol(symbols[0])
            assert df1 is df2  # Same object reference

    def test_clear_cache(self, repo):
        """clear_cache should invalidate cache."""
        symbols = repo.list_symbols()
        if symbols:
            df1 = repo.get_symbol(symbols[0])
            repo.clear_cache()
            df2 = repo.get_symbol(symbols[0])
            assert df1 is not df2

    def test_get_by_broker(self, repo):
        """get_by_broker should filter correctly."""
        symbols = repo.list_symbols()
        if symbols:
            df = repo.get_symbol(symbols[0])
            if len(df) > 0:
                broker = df["broker"][0]
                filtered = repo.get_by_broker(symbols[0], broker)
                assert len(filtered) > 0
                assert all(filtered["broker"] == broker)

    def test_missing_symbol_raises(self, repo):
        """Missing symbol should raise RepositoryError."""
        with pytest.raises(RepositoryError):
            repo.get_symbol("NONEXISTENT_SYMBOL")


# =============================================================================
# PriceRepository Tests
# =============================================================================

class TestPriceRepository:
    """Tests for PriceRepository."""

    @pytest.fixture
    def repo(self):
        return PriceRepository(DEFAULT_PATHS)

    def test_get_all_returns_dataframe(self, repo):
        """get_all should return a polars DataFrame."""
        import polars as pl
        try:
            df = repo.get_all()
            assert isinstance(df, pl.DataFrame)
        except RepositoryError:
            pytest.skip("Price data not available")

    def test_has_required_columns(self, repo):
        """DataFrame should have required columns."""
        try:
            df = repo.get_all()
            required = ["symbol_id", "date", "close_price"]
            for col in required:
                assert col in df.columns, f"Missing column: {col}"
        except RepositoryError:
            pytest.skip("Price data not available")

    def test_get_lookup(self, repo):
        """get_lookup should return dict mapping (symbol, date) to price."""
        try:
            lookup = repo.get_lookup()
            assert isinstance(lookup, dict)
            if lookup:
                key = next(iter(lookup.keys()))
                assert isinstance(key, tuple)
                assert len(key) == 2
                assert isinstance(key[0], str)
                assert isinstance(key[1], date)
        except RepositoryError:
            pytest.skip("Price data not available")

    def test_get_price(self, repo):
        """get_price should return float for valid symbol/date."""
        try:
            lookup = repo.get_lookup()
            if lookup:
                symbol, d = next(iter(lookup.keys()))
                price = repo.get_price(symbol, d)
                assert price is not None
                assert isinstance(price, (int, float))
        except RepositoryError:
            pytest.skip("Price data not available")

    def test_get_price_missing_returns_none(self, repo):
        """get_price should return None for invalid date."""
        try:
            price = repo.get_price("XXXXXX", date(1900, 1, 1))
            assert price is None
        except RepositoryError:
            pytest.skip("Price data not available")


# =============================================================================
# BrokerRepository Tests
# =============================================================================

class TestBrokerRepository:
    """Tests for BrokerRepository."""

    @pytest.fixture
    def repo(self):
        return BrokerRepository(DEFAULT_PATHS)

    def test_get_all_returns_dict(self, repo):
        """get_all should return dict mapping code to name."""
        try:
            names = repo.get_all()
            assert isinstance(names, dict)
            assert len(names) > 0
        except RepositoryError:
            pytest.skip("Broker names not available")

    def test_get_name(self, repo):
        """get_name should return broker name."""
        try:
            names = repo.get_all()
            if names:
                broker = next(iter(names.keys()))
                name = repo.get_name(broker)
                assert isinstance(name, str)
        except RepositoryError:
            pytest.skip("Broker names not available")

    def test_get_name_missing_returns_empty(self, repo):
        """get_name should return empty string for unknown broker."""
        try:
            name = repo.get_name("NONEXISTENT")
            assert name == ""
        except RepositoryError:
            pytest.skip("Broker names not available")

    def test_get_names(self, repo):
        """get_names should return dict for multiple brokers."""
        try:
            names = repo.get_all()
            if len(names) >= 2:
                brokers = list(names.keys())[:2]
                result = repo.get_names(brokers)
                assert isinstance(result, dict)
                assert len(result) == 2
        except RepositoryError:
            pytest.skip("Broker names not available")


# =============================================================================
# RankingRepository Tests
# =============================================================================

class TestRankingRepository:
    """Tests for RankingRepository."""

    @pytest.fixture
    def repo(self):
        return RankingRepository(DEFAULT_PATHS)

    def test_get_all_returns_dataframe(self, repo):
        """get_all should return a polars DataFrame."""
        import polars as pl
        try:
            df = repo.get_all()
            assert isinstance(df, pl.DataFrame)
        except RepositoryError:
            pytest.skip("Ranking data not available")

    def test_has_required_columns(self, repo):
        """DataFrame should have required columns."""
        try:
            df = repo.get_all()
            required = ["rank", "broker", "total_pnl", "realized_pnl", "unrealized_pnl"]
            for col in required:
                assert col in df.columns, f"Missing column: {col}"
        except RepositoryError:
            pytest.skip("Ranking data not available")

    def test_get_top(self, repo):
        """get_top should return N rows."""
        try:
            df = repo.get_top(10)
            assert len(df) <= 10
        except RepositoryError:
            pytest.skip("Ranking data not available")

    def test_get_broker(self, repo):
        """get_broker should return single row."""
        try:
            df = repo.get_all()
            if len(df) > 0:
                broker = df["broker"][0]
                result = repo.get_broker(broker)
                assert len(result) == 1
        except RepositoryError:
            pytest.skip("Ranking data not available")

    def test_get_broker_not_found_raises(self, repo):
        """get_broker should raise for unknown broker."""
        try:
            df = repo.get_all()  # Ensure data is available
            with pytest.raises(RepositoryError):
                repo.get_broker("NONEXISTENT")
        except RepositoryError:
            pytest.skip("Ranking data not available")

    def test_get_broker_pnl(self, repo):
        """get_broker_pnl should return float."""
        try:
            df = repo.get_all()
            if len(df) > 0:
                broker = df["broker"][0]
                pnl = repo.get_broker_pnl(broker)
                assert isinstance(pnl, float)
        except RepositoryError:
            pytest.skip("Ranking data not available")


# =============================================================================
# Error Handling Tests
# =============================================================================

class TestRepositoryErrors:
    """Tests for error handling."""

    def test_trade_repo_missing_symbol_raises(self):
        """TradeRepository should raise RepositoryError for missing symbol."""
        repo = TradeRepository(DEFAULT_PATHS)
        with pytest.raises(RepositoryError):
            repo.get_symbol("NONEXISTENT_SYMBOL_12345")

    def test_ranking_repo_missing_file_raises(self):
        """RankingRepository should raise RepositoryError for missing file."""
        fake_paths = DataPaths(root=Path("/nonexistent"))
        repo = RankingRepository(fake_paths)
        with pytest.raises(RepositoryError):
            repo.get_all()
