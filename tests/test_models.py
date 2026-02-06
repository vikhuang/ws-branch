"""Unit tests for domain models."""

import pytest
from pnl_analytics.domain.models import Lot, ClosedTrade, BrokerSummary


class TestLot:
    """Tests for Lot dataclass."""

    def test_valid_lot(self):
        """Test creating a valid lot."""
        lot = Lot(shares=1000, cost_per_share=150.5, buy_date="2024-01-15")
        assert lot.shares == 1000
        assert lot.cost_per_share == 150.5
        assert lot.buy_date == "2024-01-15"
        assert lot.total_cost == 150500.0

    def test_invalid_shares_zero(self):
        """Test that zero shares raises ValueError."""
        with pytest.raises(ValueError, match="shares must be positive"):
            Lot(shares=0, cost_per_share=100.0, buy_date="2024-01-15")

    def test_invalid_shares_negative(self):
        """Test that negative shares raises ValueError."""
        with pytest.raises(ValueError, match="shares must be positive"):
            Lot(shares=-100, cost_per_share=100.0, buy_date="2024-01-15")

    def test_invalid_cost_negative(self):
        """Test that negative cost raises ValueError."""
        with pytest.raises(ValueError, match="cost_per_share must be non-negative"):
            Lot(shares=100, cost_per_share=-10.0, buy_date="2024-01-15")

    def test_invalid_date_format(self):
        """Test that invalid date format raises ValueError."""
        with pytest.raises(ValueError, match="must be YYYY-MM-DD"):
            Lot(shares=100, cost_per_share=100.0, buy_date="2024/01/15")

    def test_invalid_date_empty(self):
        """Test that empty date raises ValueError."""
        with pytest.raises(ValueError, match="cannot be empty"):
            Lot(shares=100, cost_per_share=100.0, buy_date="")

    def test_split_partial(self):
        """Test splitting a lot partially."""
        lot = Lot(shares=1000, cost_per_share=100.0, buy_date="2024-01-15")
        taken, remaining = lot.split(300)
        
        assert taken.shares == 300
        assert taken.cost_per_share == 100.0
        assert remaining is not None
        assert remaining.shares == 700
        assert remaining.cost_per_share == 100.0

    def test_split_full(self):
        """Test splitting a lot completely."""
        lot = Lot(shares=1000, cost_per_share=100.0, buy_date="2024-01-15")
        taken, remaining = lot.split(1000)
        
        assert taken.shares == 1000
        assert remaining is None

    def test_split_invalid_exceeds(self):
        """Test that taking more than available raises ValueError."""
        lot = Lot(shares=100, cost_per_share=100.0, buy_date="2024-01-15")
        with pytest.raises(ValueError, match="Cannot take"):
            lot.split(200)

    def test_immutable(self):
        """Test that lot is immutable."""
        lot = Lot(shares=1000, cost_per_share=100.0, buy_date="2024-01-15")
        with pytest.raises(AttributeError):
            lot.shares = 2000


class TestClosedTrade:
    """Tests for ClosedTrade dataclass."""

    def test_valid_long_trade(self):
        """Test creating a valid long trade."""
        trade = ClosedTrade(
            symbol="2330", broker="1440", shares=1000,
            buy_date="2024-01-15", buy_price=100.0,
            sell_date="2024-02-20", sell_price=110.0,
            trade_type="long"
        )
        assert trade.realized_pnl == 10000.0
        assert trade.trade_return == 0.1
        assert trade.holding_days == 36
        assert trade.is_profitable is True
        assert trade.is_day_trade is False

    def test_valid_short_trade(self):
        """Test creating a valid short trade."""
        trade = ClosedTrade(
            symbol="2330", broker="1440", shares=1000,
            buy_date="2024-01-15", buy_price=110.0,
            sell_date="2024-02-20", sell_price=100.0,
            trade_type="short"
        )
        assert trade.realized_pnl == 10000.0  # profit from shorting
        assert trade.is_profitable is True

    def test_day_trade(self):
        """Test day trade detection."""
        trade = ClosedTrade(
            symbol="2330", broker="1440", shares=1000,
            buy_date="2024-01-15", buy_price=100.0,
            sell_date="2024-01-15", sell_price=101.0,
            trade_type="long"
        )
        assert trade.is_day_trade is True
        assert trade.holding_days == 0

    def test_invalid_trade_type(self):
        """Test that invalid trade type raises ValueError."""
        with pytest.raises(ValueError, match="trade_type must be"):
            ClosedTrade(
                symbol="2330", broker="1440", shares=1000,
                buy_date="2024-01-15", buy_price=100.0,
                sell_date="2024-02-20", sell_price=110.0,
                trade_type="invalid"
            )

    def test_invalid_empty_symbol(self):
        """Test that empty symbol raises ValueError."""
        with pytest.raises(ValueError, match="symbol cannot be empty"):
            ClosedTrade(
                symbol="", broker="1440", shares=1000,
                buy_date="2024-01-15", buy_price=100.0,
                sell_date="2024-02-20", sell_price=110.0,
                trade_type="long"
            )

    def test_trade_value(self):
        """Test trade value calculation."""
        trade = ClosedTrade(
            symbol="2330", broker="1440", shares=1000,
            buy_date="2024-01-15", buy_price=100.0,
            sell_date="2024-02-20", sell_price=110.0,
            trade_type="long"
        )
        assert trade.trade_value == 100000.0


class TestBrokerSummary:
    """Tests for BrokerSummary dataclass."""

    def test_timing_significance_positive(self):
        """Test timing significance for positive alpha."""
        summary = BrokerSummary(
            broker="1440", name="美林", direction="做多",
            total_pnl=1e9, realized_pnl=1e9, unrealized_pnl=0,
            trading_days=100, total_volume=1000000,
            total_amount=1e11, cumulative_net=100000, trade_count=500,
            timing_alpha=10000, p_value=0.01
        )
        assert summary.timing_significance == "顯著正向"

    def test_timing_significance_negative(self):
        """Test timing significance for negative alpha."""
        summary = BrokerSummary(
            broker="1440", name="美林", direction="做多",
            total_pnl=1e9, realized_pnl=1e9, unrealized_pnl=0,
            trading_days=100, total_volume=1000000,
            total_amount=1e11, cumulative_net=100000, trade_count=500,
            timing_alpha=-10000, p_value=0.01
        )
        assert summary.timing_significance == "顯著負向"

    def test_timing_significance_not_significant(self):
        """Test timing significance when p > 0.05."""
        summary = BrokerSummary(
            broker="1440", name="美林", direction="做多",
            total_pnl=1e9, realized_pnl=1e9, unrealized_pnl=0,
            trading_days=100, total_volume=1000000,
            total_amount=1e11, cumulative_net=100000, trade_count=500,
            timing_alpha=10000, p_value=0.5
        )
        assert summary.timing_significance == "不顯著"

    def test_style_momentum(self):
        """Test style detection for momentum trading."""
        summary = BrokerSummary(
            broker="1440", name="美林", direction="做多",
            total_pnl=1e9, realized_pnl=1e9, unrealized_pnl=0,
            trading_days=100, total_volume=1000000,
            total_amount=1e11, cumulative_net=100000, trade_count=500,
            lag_corr=0.1
        )
        assert summary.style == "順勢"

    def test_style_contrarian(self):
        """Test style detection for contrarian trading."""
        summary = BrokerSummary(
            broker="1440", name="美林", direction="做多",
            total_pnl=1e9, realized_pnl=1e9, unrealized_pnl=0,
            trading_days=100, total_volume=1000000,
            total_amount=1e11, cumulative_net=100000, trade_count=500,
            lag_corr=-0.1
        )
        assert summary.style == "逆勢"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
