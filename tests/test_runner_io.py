"""runner / io / cli 單元測試(fake table 注入,不碰真資料)。"""

from __future__ import annotations

import datetime

import polars as pl
import pytest

from ws_branch import cli
from ws_branch.tables import io, runner
from ws_branch.tables.registry import Table


@pytest.fixture()
def fake_table(tmp_path, monkeypatch) -> dict:
    monkeypatch.setenv("WS_BRANCH_DATA_DIR", str(tmp_path))
    calls = {"built": [], "verified": []}

    def build_year(year: int) -> pl.LazyFrame:
        calls["built"].append(year)
        return pl.DataFrame(
            {"date": [datetime.date(year, 1, 5)], "x": [year]}).lazy()

    def verify(n: int, seed: int) -> None:
        calls["verified"].append((n, seed))

    t = Table(name="fake", build_year=build_year, verify=verify, first_year=2025)
    monkeypatch.setitem(runner.TABLES, "fake", t)
    return calls


def test_io_env_override(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("WS_BRANCH_DATA_DIR", str(tmp_path))
    assert io.data_dir() == tmp_path
    monkeypatch.delenv("WS_BRANCH_DATA_DIR")
    assert io.data_dir() == io.REPO / "data"


def test_runner_build_skip_and_force(fake_table, tmp_path) -> None:
    runner.build("fake", 2025)
    assert fake_table["built"] == [2025]
    assert io.existing_years("fake") == [2025]
    runner.build("fake", 2025)            # 存在即跳過(含當年語意已修)
    assert fake_table["built"] == [2025]
    runner.build("fake", 2025, force=True)
    assert fake_table["built"] == [2025, 2025]


def test_runner_unknown_table_raises(fake_table) -> None:
    with pytest.raises(KeyError, match="unknown table"):
        runner.build("nope", 2025)
    with pytest.raises(KeyError, match="unknown table"):
        runner.verify("nope")


def test_runner_verify_dispatch(fake_table) -> None:
    runner.verify("fake", n_samples=7, seed=42)
    assert fake_table["verified"] == [(7, 42)]


def test_io_scan_date_filter(fake_table) -> None:
    runner.build("fake", 2025)
    runner.build("fake", 2026)
    assert io.existing_years("fake") == [2025, 2026]
    both = io.scan("fake").collect()
    assert both.height == 2
    only26 = io.scan("fake", start="2026-01-01").collect()
    assert only26["x"].to_list() == [2026]


def test_cli_arg_mapping(fake_table, monkeypatch) -> None:
    cli.main(["build", "--table", "fake", "--year", "2025"])
    assert fake_table["built"] == [2025]
    cli.main(["verify", "--table", "fake", "--n", "9"])
    assert fake_table["verified"][-1][0] == 9

def test_frozen_table_flag_marks_t4_v2_only() -> None:
    """Step F:T4 v2 標 frozen(可重現、不更新);v3 與其他表不得被誤標。"""
    from ws_branch.tables.registry import TABLES
    assert TABLES["t4_broker_features"].frozen
    assert not TABLES["t4_broker_measure"].frozen
    assert not TABLES["t1_broker_daily"].frozen and not TABLES["t3b_accounting_bounds"].frozen
