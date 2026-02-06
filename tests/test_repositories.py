"""Unit tests for infrastructure repositories.

Tests verify that repositories:
1. Load data correctly from files
2. Provide proper caching
3. Handle errors gracefully
4. Return expected types and shapes
"""

import pytest
from pathlib import Path

from pnl_analytics.infrastructure import (
    DataPaths,
    DEFAULT_PATHS,
    RepositoryError,
)
from pnl_analytics.infrastructure.repositories import (
    TradeRepository,
    ClosedTradeRepository,
    PriceRepository,
    BrokerRepository,
    IndexMapRepository,
    PnlRepository,
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
        """Validate should return list of missing files."""
        fake_paths = DataPaths(root=Path("/nonexistent"))
        missing = fake_paths.validate()
        assert len(missing) > 0
        assert all(isinstance(m, str) for m in missing)

    def test_paths_are_consistent(self):
        """All paths should be relative to root directory."""
        paths = DEFAULT_PATHS
        for attr in ["trade_summary", "price_master", "closed_trades"]:
            path = getattr(paths, attr)
            # Paths should be resolvable from root
            assert isinstance(path, Path)


# =============================================================================
# TradeRepository Tests
# =============================================================================

class TestTradeRepository:
    """Tests for TradeRepository."""

    @pytest.fixture
    def repo(self):
        return TradeRepository(DEFAULT_PATHS)

    def test_get_all_returns_dataframe(self, repo):
        """get_all should return a polars DataFrame."""
        import polars as pl
        df = repo.get_all()
        assert isinstance(df, pl.DataFrame)

    def test_has_required_columns(self, repo):
        """DataFrame should have required columns."""
        df = repo.get_all()
        required = ["date", "broker", "buy_shares", "sell_shares"]
        for col in required:
            assert col in df.columns, f"Missing column: {col}"

    def test_caching_works(self, repo):
        """Second call should return cached data."""
        df1 = repo.get_all()
        df2 = repo.get_all()
        assert df1 is df2  # Same object reference

    def test_clear_cache(self, repo):
        """clear_cache should invalidate cache."""
        df1 = repo.get_all()
        repo.clear_cache()
        df2 = repo.get_all()
        assert df1 is not df2

    def test_get_by_broker(self, repo):
        """get_by_broker should filter correctly."""
        df = repo.get_by_broker("1440")  # Merrill Lynch
        assert len(df) > 0
        assert df["broker"].unique().to_list() == ["1440"]

    def test_get_by_broker_empty_raises(self, repo):
        """Empty broker should raise ValueError."""
        with pytest.raises(ValueError, match="cannot be empty"):
            repo.get_by_broker("")

    def test_get_brokers_returns_list(self, repo):
        """get_brokers should return sorted list."""
        brokers = repo.get_brokers()
        assert isinstance(brokers, list)
        assert len(brokers) > 0
        assert brokers == sorted(brokers)


# =============================================================================
# ClosedTradeRepository Tests
# =============================================================================

class TestClosedTradeRepository:
    """Tests for ClosedTradeRepository."""

    @pytest.fixture
    def repo(self):
        return ClosedTradeRepository(DEFAULT_PATHS)

    def test_get_all_returns_dataframe(self, repo):
        """get_all should return a polars DataFrame."""
        import polars as pl
        df = repo.get_all()
        assert isinstance(df, pl.DataFrame)

    def test_has_required_columns(self, repo):
        """DataFrame should have required columns."""
        df = repo.get_all()
        required = ["symbol", "broker", "shares", "buy_date", "sell_date", "trade_type"]
        for col in required:
            assert col in df.columns, f"Missing column: {col}"

    def test_get_by_trade_type(self, repo):
        """get_by_trade_type should filter correctly."""
        long_trades = repo.get_by_trade_type("long")
        short_trades = repo.get_by_trade_type("short")
        assert len(long_trades) + len(short_trades) == len(repo.get_all())

    def test_invalid_trade_type_raises(self, repo):
        """Invalid trade type should raise ValueError."""
        with pytest.raises(ValueError, match="must be 'long' or 'short'"):
            repo.get_by_trade_type("invalid")


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
        df = repo.get_all()
        assert isinstance(df, pl.DataFrame)

    def test_has_required_columns(self, repo):
        """DataFrame should have required columns."""
        df = repo.get_all()
        required = ["date", "close_price"]
        for col in required:
            assert col in df.columns, f"Missing column: {col}"

    def test_get_price_dict(self, repo):
        """get_price_dict should return dict mapping date to price."""
        prices = repo.get_price_dict()
        assert isinstance(prices, dict)
        assert len(prices) > 0
        # All values should be floats
        for date, price in list(prices.items())[:5]:
            assert isinstance(date, str)
            assert isinstance(price, (int, float))

    def test_get_price(self, repo):
        """get_price should return float for valid date."""
        prices = repo.get_price_dict()
        some_date = next(iter(prices.keys()))
        price = repo.get_price(some_date)
        assert price is not None
        assert isinstance(price, (int, float))

    def test_get_price_missing_returns_none(self, repo):
        """get_price should return None for invalid date."""
        price = repo.get_price("1900-01-01")
        assert price is None

    def test_get_market_return(self, repo):
        """get_market_return should return a float."""
        ret = repo.get_market_return()
        assert isinstance(ret, float)


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
        names = repo.get_all()
        assert isinstance(names, dict)
        assert len(names) > 0

    def test_get_name(self, repo):
        """get_name should return broker name."""
        name = repo.get_name("1440")  # Merrill Lynch
        assert isinstance(name, str)
        # Merrill should have a name
        assert len(name) > 0

    def test_get_name_missing_returns_empty(self, repo):
        """get_name should return empty string for unknown broker."""
        name = repo.get_name("NONEXISTENT")
        assert name == ""

    def test_get_names(self, repo):
        """get_names should return dict for multiple brokers."""
        brokers = ["1440", "5920"]
        names = repo.get_names(brokers)
        assert isinstance(names, dict)
        assert len(names) == 2


# =============================================================================
# IndexMapRepository Tests
# =============================================================================

class TestIndexMapRepository:
    """Tests for IndexMapRepository."""

    @pytest.fixture
    def repo(self):
        return IndexMapRepository(DEFAULT_PATHS)

    def test_get_all_returns_dict(self, repo):
        """get_all should return dict with mappings."""
        maps = repo.get_all()
        assert isinstance(maps, dict)
        assert "brokers" in maps
        assert "dates" in maps

    def test_get_broker_index(self, repo):
        """get_broker_index should return int for valid broker."""
        idx = repo.get_broker_index("1440")
        assert idx is not None
        assert isinstance(idx, int)
        assert idx >= 0

    def test_get_broker_index_missing_returns_none(self, repo):
        """get_broker_index should return None for unknown broker."""
        idx = repo.get_broker_index("NONEXISTENT")
        assert idx is None

    def test_get_brokers_returns_sorted_list(self, repo):
        """get_brokers should return list of broker codes."""
        brokers = repo.get_brokers()
        assert isinstance(brokers, list)
        assert len(brokers) > 0

    def test_get_dates_returns_sorted_list(self, repo):
        """get_dates should return sorted list of dates."""
        dates = repo.get_dates()
        assert isinstance(dates, list)
        assert len(dates) > 0
        assert dates == sorted(dates)


# =============================================================================
# PnlRepository Tests
# =============================================================================

class TestPnlRepository:
    """Tests for PnlRepository."""

    @pytest.fixture
    def repo(self):
        return PnlRepository(DEFAULT_PATHS)

    def test_get_realized_returns_ndarray(self, repo):
        """get_realized should return numpy array."""
        import numpy as np
        arr = repo.get_realized()
        assert isinstance(arr, np.ndarray)

    def test_get_unrealized_returns_ndarray(self, repo):
        """get_unrealized should return numpy array."""
        import numpy as np
        arr = repo.get_unrealized()
        assert isinstance(arr, np.ndarray)

    def test_tensor_shape(self, repo):
        """Tensors should be 3D with shape (symbols, dates, brokers)."""
        shape = repo.get_shape()
        assert len(shape) == 3
        n_symbols, n_dates, n_brokers = shape
        assert n_symbols >= 1
        assert n_dates > 0
        assert n_brokers > 0

    def test_realized_unrealized_same_shape(self, repo):
        """Realized and unrealized should have same shape."""
        realized = repo.get_realized()
        unrealized = repo.get_unrealized()
        assert realized.shape == unrealized.shape

    def test_get_all_returns_tuple(self, repo):
        """get_all should return tuple of both arrays."""
        result = repo.get_all()
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_get_broker_realized(self, repo):
        """get_broker_realized should return 1D array."""
        import numpy as np
        arr = repo.get_broker_realized(0)
        assert isinstance(arr, np.ndarray)
        assert arr.ndim == 1

    def test_get_broker_total_realized(self, repo):
        """get_broker_total_realized should return float."""
        total = repo.get_broker_total_realized(0)
        assert isinstance(total, float)

    def test_caching_works(self, repo):
        """Caching should return same array."""
        arr1 = repo.get_realized()
        arr2 = repo.get_realized()
        assert arr1 is arr2


# =============================================================================
# Error Handling Tests
# =============================================================================

class TestRepositoryErrors:
    """Tests for error handling."""

    def test_trade_repo_missing_file_raises(self):
        """TradeRepository should raise RepositoryError for missing file."""
        fake_paths = DataPaths(root=Path("/nonexistent"))
        repo = TradeRepository(fake_paths)
        with pytest.raises(RepositoryError):
            repo.get_all()

    def test_pnl_repo_missing_file_raises(self):
        """PnlRepository should raise RepositoryError for missing file."""
        fake_paths = DataPaths(root=Path("/nonexistent"))
        repo = PnlRepository(fake_paths)
        with pytest.raises(RepositoryError):
            repo.get_realized()
