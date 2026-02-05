"""Tests for ws-branch."""


def test_version():
    """Test version is defined."""
    from ws_branch import __version__

    assert __version__ == "0.1.0"
