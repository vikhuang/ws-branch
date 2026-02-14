"""Unit tests for interfaces/cli.py.

Tests verify:
1. CLI commands execute without errors
2. Output format is correct
3. Error handling works properly
"""

import pytest

from pnl_analytics.interfaces.cli import main
from pnl_analytics.infrastructure.repositories import RepositoryError


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

    def test_verify_runs(self, capsys):
        """Verify should run without crash."""
        # May pass or fail depending on data, but shouldn't crash
        result = main(["verify"])
        assert result in (0, 1)

        captured = capsys.readouterr()
        assert "數據驗證" in captured.out


class TestQueryCommand:
    """Tests for query command."""

    def test_query_invalid_broker(self, capsys):
        """Query invalid broker should fail."""
        result = main(["query", "INVALID_99999"])
        assert result == 1

        captured = capsys.readouterr()
        assert "找不到" in captured.out

    def test_query_help(self):
        """Query --help should show options."""
        with pytest.raises(SystemExit) as exc:
            main(["query", "--help"])
        assert exc.value.code == 0


class TestRankingCommand:
    """Tests for ranking command."""

    def test_ranking_help(self, capsys):
        """Ranking --help should show options."""
        with pytest.raises(SystemExit) as exc:
            main(["ranking", "--help"])
        assert exc.value.code == 0
