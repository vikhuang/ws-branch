"""Tests for Layer 1.5: pnl_daily and fifo_state output.

Validates schema, sort order, and consistency with Layer 2 (pnl/{symbol}).
Runs against real data; skips if data not available.
"""

import pytest
import polars as pl

from pnl_analytics.infrastructure import DEFAULT_PATHS, DEFAULT_CONFIG


BACKTEST_START = DEFAULT_CONFIG.backtest_start  # "2023-01-01"


def _get_test_symbol() -> str | None:
    """Return a symbol that has both pnl_daily and pnl files, or None."""
    symbols = DEFAULT_PATHS.list_symbols()
    for sym in symbols[:10]:  # Check first 10
        daily_path = DEFAULT_PATHS.symbol_pnl_daily_path(sym)
        pnl_path = DEFAULT_PATHS.symbol_pnl_path(sym)
        if daily_path.exists() and pnl_path.exists():
            return sym
    return None


# =============================================================================
# pnl_daily schema and invariants
# =============================================================================

class TestPnlDaily:

    @pytest.fixture
    def symbol(self):
        sym = _get_test_symbol()
        if sym is None:
            pytest.skip("pnl_daily data not available")
        return sym

    @pytest.fixture
    def daily_df(self, symbol):
        return pl.read_parquet(DEFAULT_PATHS.symbol_pnl_daily_path(symbol))

    def test_schema(self, daily_df):
        """pnl_daily should have exactly 4 columns with correct types."""
        assert set(daily_df.columns) == {"broker", "date", "realized_pnl", "unrealized_pnl"}
        assert daily_df.schema["broker"] == pl.Utf8
        assert daily_df.schema["date"] == pl.Date
        assert daily_df.schema["realized_pnl"] == pl.Float64
        assert daily_df.schema["unrealized_pnl"] == pl.Float64

    def test_no_zero_rows(self, daily_df):
        """Should not contain rows where both realized and unrealized are 0."""
        zeros = daily_df.filter(
            (pl.col("realized_pnl") == 0.0) & (pl.col("unrealized_pnl") == 0.0)
        )
        assert len(zeros) == 0, f"Found {len(zeros)} rows with (0, 0)"

    def test_sort_order(self, daily_df):
        """Should be sorted by (broker, date) ascending."""
        sorted_df = daily_df.sort(["broker", "date"])
        assert daily_df["broker"].to_list() == sorted_df["broker"].to_list()
        assert daily_df["date"].to_list() == sorted_df["date"].to_list()

    def test_no_duplicate_rows(self, daily_df):
        """Each (broker, date) pair should be unique."""
        n_unique = daily_df.select(["broker", "date"]).unique().height
        assert n_unique == daily_df.height, "Duplicate (broker, date) pairs found"


# =============================================================================
# Consistency: pnl_daily ↔ pnl/{symbol}
# =============================================================================

class TestPnlDailyConsistency:

    @pytest.fixture
    def symbol(self):
        sym = _get_test_symbol()
        if sym is None:
            pytest.skip("pnl_daily data not available")
        return sym

    @pytest.fixture
    def daily_df(self, symbol):
        return pl.read_parquet(DEFAULT_PATHS.symbol_pnl_daily_path(symbol))

    @pytest.fixture
    def pnl_df(self, symbol):
        return pl.read_parquet(DEFAULT_PATHS.symbol_pnl_path(symbol))

    def test_realized_pnl_matches(self, daily_df, pnl_df):
        """Sum of daily realized (after backtest_start) should match pnl/ realized."""
        from datetime import date
        start = date.fromisoformat(BACKTEST_START)

        daily_agg = (
            daily_df
            .filter(pl.col("date") >= start)
            .group_by("broker")
            .agg(pl.col("realized_pnl").sum().alias("realized_sum"))
        )

        merged = pnl_df.select("broker", "realized_pnl").join(
            daily_agg, on="broker", how="left"
        ).with_columns(
            pl.col("realized_sum").fill_null(0.0)
        )

        for row in merged.iter_rows(named=True):
            diff = abs(row["realized_pnl"] - row["realized_sum"])
            assert diff < 0.01, (
                f"Broker {row['broker']}: pnl/ realized={row['realized_pnl']:.2f}, "
                f"daily sum={row['realized_sum']:.2f}, diff={diff:.2f}"
            )

    def test_unrealized_pnl_matches(self, daily_df, pnl_df):
        """Last day's unrealized in daily should match pnl/ unrealized."""
        # Get last unrealized per broker from daily
        last_daily = (
            daily_df
            .sort(["broker", "date"])
            .group_by("broker")
            .last()
            .select("broker", pl.col("unrealized_pnl").alias("last_unrealized"))
        )

        merged = pnl_df.select("broker", "unrealized_pnl").join(
            last_daily, on="broker", how="left"
        )

        # Brokers with no daily rows have no position → unrealized should be 0
        merged = merged.with_columns(
            pl.col("last_unrealized").fill_null(0.0)
        )

        for row in merged.iter_rows(named=True):
            diff = abs(row["unrealized_pnl"] - row["last_unrealized"])
            assert diff < 0.01, (
                f"Broker {row['broker']}: pnl/ unrealized={row['unrealized_pnl']:.2f}, "
                f"daily last={row['last_unrealized']:.2f}, diff={diff:.2f}"
            )


# =============================================================================
# fifo_state schema
# =============================================================================

class TestFifoState:

    @pytest.fixture
    def symbol(self):
        sym = _get_test_symbol()
        if sym is None:
            pytest.skip("fifo_state data not available")
        return sym

    @pytest.fixture
    def fifo_df(self, symbol):
        path = DEFAULT_PATHS.symbol_fifo_state_path(symbol)
        if not path.exists():
            pytest.skip("fifo_state not generated for this symbol")
        return pl.read_parquet(path)

    def test_schema(self, fifo_df):
        """fifo_state should have correct columns and types."""
        assert set(fifo_df.columns) == {"broker", "side", "shares", "cost_per_share", "open_date"}
        assert fifo_df.schema["broker"] == pl.Utf8
        assert fifo_df.schema["side"] == pl.Utf8
        assert fifo_df.schema["shares"] == pl.Int64
        assert fifo_df.schema["cost_per_share"] == pl.Float64
        assert fifo_df.schema["open_date"] == pl.Date

    def test_side_values(self, fifo_df):
        """side should only be 'long' or 'short'."""
        sides = set(fifo_df["side"].unique().to_list())
        assert sides <= {"long", "short"}, f"Unexpected side values: {sides}"

    def test_shares_positive(self, fifo_df):
        """All shares should be positive."""
        assert (fifo_df["shares"] > 0).all(), "Found non-positive shares in fifo_state"

    def test_cost_positive(self, fifo_df):
        """All cost_per_share should be positive."""
        assert (fifo_df["cost_per_share"] > 0).all(), "Found non-positive cost in fifo_state"
