"""Unit tests for interfaces/cli.py.

Tests verify:
1. CLI commands execute without errors
2. Output format is correct
3. Error handling works properly
"""

import pytest

from pnl_analytics.interfaces.cli import main


class TestCliBasic:
    """Basic CLI tests."""

    def test_version(self, capsys):
        """--version should show version."""
        with pytest.raises(SystemExit) as exc:
            main(["--version"])
        assert exc.value.code == 0

    def test_help(self, capsys):
        """No command should show help."""
        result = main([])
        assert result == 0

    def test_invalid_command(self):
        """Invalid command should fail."""
        with pytest.raises(SystemExit):
            main(["invalid_command"])


class TestVerifyCommand:
    """Tests for verify command."""

    def test_verify_passes(self):
        """Verify should pass with valid data."""
        result = main(["verify"])
        assert result == 0


class TestQueryCommand:
    """Tests for query command."""

    def test_query_valid_broker(self, capsys):
        """Query valid broker should succeed."""
        result = main(["query", "1440", "--permutations", "10"])
        assert result == 0

        captured = capsys.readouterr()
        assert "1440" in captured.out
        assert "美林" in captured.out

    def test_query_invalid_broker(self, capsys):
        """Query invalid broker should fail."""
        result = main(["query", "INVALID"])
        assert result == 1

        captured = capsys.readouterr()
        assert "找不到" in captured.out


class TestScorecardCommand:
    """Tests for scorecard command."""

    def test_scorecard_valid_broker(self, capsys):
        """Scorecard for valid broker should succeed."""
        result = main(["scorecard", "1440"])
        assert result == 0

        captured = capsys.readouterr()
        assert "評分卡" in captured.out
        assert "美林" in captured.out

    def test_scorecard_invalid_broker(self, capsys):
        """Scorecard for invalid broker should fail."""
        result = main(["scorecard", "INVALID"])
        assert result == 1


class TestRankingCommand:
    """Tests for ranking command (slower, fewer tests)."""

    def test_ranking_help(self, capsys):
        """Ranking --help should show options."""
        with pytest.raises(SystemExit) as exc:
            main(["ranking", "--help"])
        assert exc.value.code == 0
